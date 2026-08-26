# tests/test_rest_api_extractor.py 

"""
Tests for RestApiExtractor (etl_framework/extractors/rest_api.py).

Uses the `responses` library to intercept outbound HTTP calls made through
`requests` -- no real network call ever happens in this file, and no real
server is spun up.

Covers:
- every documented HTTP status -> exception mapping
- 200-with-error-body is caught as MalformedResponseError
- all three payload shapes normalize to a flat record list
- extract() is confirmed to be a real generator
- records stream correctly end-to-end via run()
- retry re-invokes _fetch_page() (not the whole generator) on a transient
  failure -- mocked to fail once then succeed
- multi-page pagination stops correctly on a short/final page, no duplicate
  records across page boundaries
- a 429 with Retry-After causes the retry decorator to wait the header's
  value, verified by mocking time.sleep (not by measuring real elapsed time)
"""

import inspect
from unittest.mock import patch

import pytest
import responses

from etl_framework.exceptions.pipeline_errors import (
    AuthenticationError,
    MalformedResponseError,
    MaxRetriesExceededError,
    NetworkError,
    RateLimitError,
    ServerError,
    SourceNotFoundError,
)
from etl_framework.extractors.rest_api import RestApiExtractor

URL = "https://api.example.com/records"

# ---------------------------------------------------------------------------
# Status code -> exception mapping (via _fetch_page(), bypassing retry)
# ---------------------------------------------------------------------------


@responses.activate
def test_extract_is_a_generator_function():
    assert inspect.isgeneratorfunction(RestApiExtractor.extract)


@pytest.mark.parametrize(
    "status, expected_exception",
    [
        (500, ServerError),
        (503, ServerError),
        (401, AuthenticationError),
        (403, AuthenticationError),
        (404, SourceNotFoundError),
    ],
)
@responses.activate
def test_status_code_maps_to_correct_exception(status, expected_exception, make_api_config):
    """
    Calls extractor._fetch_page() directly rather than extract(). extract()
    always wraps _fetch_page() in the retry decorator (see extract()'s
    docstring), so a transient status like 500 raised through extract()
    would retry 3 times by default and surface as MaxRetriesExceededError,
    not the raw ServerError. _fetch_page() is the mapping logic with no
    retry loop around it, so it's the right unit to call here.
    """
    responses.add(responses.GET, URL, json={"error": "boom"}, status=status)
    config = make_api_config(url=URL)
    extractor = RestApiExtractor(config)

    with pytest.raises(expected_exception):
        extractor._fetch_page(1)


@responses.activate
def test_429_maps_to_rate_limit_error(make_api_config):
    responses.add(responses.GET, URL, json={"error": "rate limited"}, status=429)
    config = make_api_config(url=URL)
    extractor = RestApiExtractor(config)

    with pytest.raises(RateLimitError):
        extractor._fetch_page(1)


@responses.activate
def test_unparseable_json_maps_to_malformed_response_error(make_api_config):
    responses.add(
        responses.GET, URL, body="not json at all", status=200,
        content_type="application/json",
    )
    config = make_api_config(url=URL)
    extractor = RestApiExtractor(config)

    with pytest.raises(MalformedResponseError):
        extractor._fetch_page(1)


@responses.activate
def test_200_with_error_body_is_malformed_response(make_api_config):
    """A 200 only proves transport-level success -- the body can still
    report an application-level error, which must be caught, not silently
    treated as valid data."""
    responses.add(responses.GET, URL, json={"error": "invalid query parameter"}, status=200)
    config = make_api_config(url=URL)
    extractor = RestApiExtractor(config)

    with pytest.raises(MalformedResponseError):
        extractor._fetch_page(1)


@responses.activate
def test_network_timeout_maps_to_network_error(make_api_config):
    import requests

    responses.add(responses.GET, URL, body=requests.exceptions.Timeout())
    config = make_api_config(url=URL)
    extractor = RestApiExtractor(config)

    with pytest.raises(NetworkError):
        extractor._fetch_page(1)


@responses.activate
def test_connection_error_maps_to_network_error(make_api_config):
    import requests

    responses.add(responses.GET, URL, body=requests.exceptions.ConnectionError())
    config = make_api_config(url=URL)
    extractor = RestApiExtractor(config)

    with pytest.raises(NetworkError):
        extractor._fetch_page(1)


@responses.activate
def test_unexpected_status_code_falls_through_to_malformed_response(make_api_config):
    """Covers the catch-all branch for any status not otherwise explicitly
    mapped (e.g. 418) -- treated as MalformedResponseError rather than
    silently accepted or crashing on an unhandled case."""
    responses.add(responses.GET, URL, json={}, status=418)
    config = make_api_config(url=URL)
    extractor = RestApiExtractor(config)

    with pytest.raises(MalformedResponseError):
        extractor._fetch_page(1)


@responses.activate
def test_unexpected_payload_shape_raises_malformed_response(make_api_config):
    """A 200 response whose JSON body is neither a bare list, {"results":
    [...]}, nor {"data": [...]} doesn't match any normalization rule and
    must be rejected rather than silently returned as-is."""
    responses.add(responses.GET, URL, json={"unexpected_key": "value"}, status=200)
    config = make_api_config(url=URL)
    extractor = RestApiExtractor(config)

    with pytest.raises(MalformedResponseError):
        extractor._fetch_page(1)


# ---------------------------------------------------------------------------
# Confirming what happens end-to-end when a transient status never recovers
# ---------------------------------------------------------------------------


@patch("etl_framework.decorators.retry.time.sleep")
@responses.activate
def test_persistent_server_error_surfaces_as_max_retries_exceeded_via_extract(
    mock_sleep, make_api_config
):
    """
    Complements test_status_code_maps_to_correct_exception above: that test
    checks the raw mapping via _fetch_page() directly; this test checks
    what actually reaches a caller of extract() (the retry-wrapped path)
    when a transient status never recovers -- MaxRetriesExceededError
    wrapping the original ServerError, not the ServerError itself.
    """
    responses.add(responses.GET, URL, json={"error": "down"}, status=503)
    config = make_api_config(url=URL)
    config.retry_config.max_retries = 2
    extractor = RestApiExtractor(config)

    with pytest.raises(MaxRetriesExceededError) as exc_info:
        list(extractor.extract())

    assert isinstance(exc_info.value.last_exception, ServerError)
    assert mock_sleep.call_count == 2  # one sleep per retry attempt


# ---------------------------------------------------------------------------
# Payload shape normalization
# ---------------------------------------------------------------------------


@responses.activate
def test_bare_list_payload_normalizes(make_api_config, bare_list_payload):
    responses.add(responses.GET, URL, json=bare_list_payload, status=200)
    config = make_api_config(url=URL, page_size=100)
    extractor = RestApiExtractor(config)

    records = list(extractor.extract())
    assert records == bare_list_payload


@responses.activate
def test_results_wrapped_payload_normalizes(make_api_config, results_wrapped_payload):
    responses.add(responses.GET, URL, json=results_wrapped_payload, status=200)
    config = make_api_config(url=URL, page_size=100)
    extractor = RestApiExtractor(config)

    records = list(extractor.extract())
    assert records == results_wrapped_payload["results"]


@responses.activate
def test_data_wrapped_payload_normalizes(make_api_config, data_wrapped_payload):
    responses.add(responses.GET, URL, json=data_wrapped_payload, status=200)
    config = make_api_config(url=URL, page_size=100)
    extractor = RestApiExtractor(config)

    records = list(extractor.extract())
    assert records == data_wrapped_payload["data"]


# ---------------------------------------------------------------------------
# End-to-end streaming via run()
# ---------------------------------------------------------------------------


@responses.activate
def test_run_streams_records_and_logs_success(make_api_config, bare_list_payload):
    responses.add(responses.GET, URL, json=bare_list_payload, status=200)
    config = make_api_config(url=URL, page_size=100)
    extractor = RestApiExtractor(config)

    records = list(extractor.run())
    assert records == bare_list_payload


# ---------------------------------------------------------------------------
# Retry re-invokes _fetch_page(), not the whole generator
# ---------------------------------------------------------------------------


@patch("etl_framework.decorators.retry.time.sleep")
@responses.activate
def test_retry_recovers_from_one_transient_failure(mock_sleep, make_api_config, bare_list_payload):
    """
    Registers TWO responses for the same URL: a 503 first, then a 200 with
    real data. `responses` serves registered responses in order for
    repeated calls to the same URL. The first call to _fetch_page() sees
    the 503 (raises ServerError), the retry decorator catches it and calls
    _fetch_page() again, which now sees the 200. This directly exercises
    the "retry the eager fetch, not the lazy generator" design from
    Phase 1.
    """
    responses.add(responses.GET, URL, json={"error": "temporarily down"}, status=503)
    responses.add(responses.GET, URL, json=bare_list_payload, status=200)

    config = make_api_config(url=URL, page_size=100)
    extractor = RestApiExtractor(config)

    records = list(extractor.extract())

    assert records == bare_list_payload
    assert len(responses.calls) == 2  # confirms it actually retried
    mock_sleep.assert_called_once()  # slept exactly once, for the one failure


# ---------------------------------------------------------------------------
# Pagination: stop correctly on a short/empty final page, no duplicates
# ---------------------------------------------------------------------------


@responses.activate
def test_pagination_stops_on_short_final_page_no_duplicates(make_api_config):
    page_1 = [{"id": i} for i in range(1, 4)]  # 3 records, page_size=3 -> full
    page_2 = [{"id": 4}]  # 1 record, short -> last page

    responses.add(responses.GET, URL, json=page_1, status=200)
    responses.add(responses.GET, URL, json=page_2, status=200)

    config = make_api_config(url=URL, page_size=3)
    extractor = RestApiExtractor(config)

    records = list(extractor.extract())

    assert records == page_1 + page_2
    assert len(responses.calls) == 2  # stopped after the short page


@responses.activate
def test_pagination_stops_on_empty_page(make_api_config):
    page_1 = [{"id": i} for i in range(1, 4)]  # full page of 3
    responses.add(responses.GET, URL, json=page_1, status=200)
    responses.add(responses.GET, URL, json=[], status=200)  # empty page 2

    config = make_api_config(url=URL, page_size=3)
    extractor = RestApiExtractor(config)

    records = list(extractor.extract())

    assert records == page_1
    assert len(responses.calls) == 2


# ---------------------------------------------------------------------------
# Retry-After header drives the retry decorator's actual wait
# ---------------------------------------------------------------------------


@patch("etl_framework.decorators.retry.time.sleep")
@responses.activate
def test_429_with_retry_after_header_drives_sleep_duration(mock_sleep, make_api_config, bare_list_payload):
    responses.add(
        responses.GET, URL,
        json={"error": "rate limited"}, status=429,
        headers={"Retry-After": "9"},
    )
    responses.add(responses.GET, URL, json=bare_list_payload, status=200)

    config = make_api_config(url=URL, page_size=100)
    extractor = RestApiExtractor(config)

    records = list(extractor.extract())

    assert records == bare_list_payload
    mock_sleep.assert_called_once_with(9.0)  # the header's value


