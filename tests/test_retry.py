# tests/test_retry.py 

"""
Tests for the retry() decorator factory (etl_framework/decorators/retry.py).

Covers:
- retry count respects max_retries
- PermanentExtractionError (and subclasses) never retry
- MaxRetriesExceededError is raised with correct metadata after exhaustion
- backoff timing follows backoff_factor ** attempt (exponential), not
  backoff_factor * attempt (linear) -- this is a real regression this repo's
  history had (see the commit that fixed linear -> exponential)
- a RateLimitError carrying retry_after honors that value instead of the
  computed exponential backoff

time.sleep is mocked throughout via unittest.mock.patch so these tests run
in milliseconds instead of actually waiting through real backoff delays.
"""

from unittest.mock import patch

import pytest

from etl_framework.decorators.retry import retry
from etl_framework.exceptions.pipeline_errors import (
    MaxRetriesExceededError,
    PermanentExtractionError,
    RateLimitError,
    TransientExtractionError,
)


class FlakyFunction:
    """
    A fake "extraction unit" that fails `fail_times` times (raising
    `exception_factory()` each time) and then returns `success_value`.

    Stands in for the real thing being retried -- e.g.
    RestApiExtractor._fetch_page() -- without a real network call.
    Tracking call_count on self lets a test assert exactly how many times
    the retry decorator invoked the wrapped function.
    """

    def __init__(self, fail_times, exception_factory, success_value="ok"):
        self.fail_times = fail_times
        self.exception_factory = exception_factory
        self.success_value = success_value
        self.call_count = 0

    def __call__(self, *args, **kwargs):
        self.call_count += 1
        if self.call_count <= self.fail_times:
            raise self.exception_factory()
        return self.success_value


#-------------------------------------------------------------------------
# Happy path: fails a few times, then succeeds within max_retries 
#-------------------------------------------------------------------------

@patch("etl_framework.decorators.retry.time.sleep")
def test_retry_succeeds_after_transient_failures_within_budget(
    mock_sleep, make_retry_config
):
    config = make_retry_config(max_retries=3)
    flaky = FlakyFunction(
        fail_times=2, exception_factory=lambda: TransientExtractionError("timeout")
    )
    wrapped = retry(config)(flaky)

    result = wrapped()

    assert result == "ok"
    assert flaky.call_count == 3  # 2 failures + 1 success
    assert mock_sleep.call_count == 2  # slept once per failed attempt


#-------------------------------------------------------------------------
# Retry count respects max_retries -- exhaustion raises MaxRetriesExceededError
#-------------------------------------------------------------------------

@patch("etl_framework.decorators.retry.time.sleep")
def test_retry_raises_max_retries_exceeded_when_never_succeeding(
    mock_sleep, make_retry_config
):
    config = make_retry_config(max_retries=3)
    original_error = TransientExtractionError("persistent timeout")
    flaky = FlakyFunction(fail_times=999, exception_factory=lambda: original_error)

    def fake_fetch(*args, **kwargs):
        return flaky(*args, **kwargs)

    wrapped = retry(config)(fake_fetch)

    with pytest.raises(MaxRetriesExceededError) as exc_info:
        wrapped()

    err = exc_info.value
    # max_retries=3 means: 1 initial attempt + 3 retries = 4 total calls
    assert flaky.call_count == 4
    assert err.attempts == 4
    assert err.operation == "fake_fetch"  # func.__name__, used for log/debug context
    assert err.last_exception is original_error
    assert err.__cause__ is original_error
    assert isinstance(err.duration_seconds, float)


#-------------------------------------------------------------------------
# Permanent errors never retry 
#-------------------------------------------------------------------------

@patch("etl_framework.decorators.retry.time.sleep")
def test_permanent_error_is_not_retried(mock_sleep, make_retry_config):
    config = make_retry_config(max_retries=5)
    flaky = FlakyFunction(
        fail_times=999, exception_factory=lambda: PermanentExtractionError("bad creds")
    )
    wrapped = retry(config)(flaky)

    with pytest.raises(PermanentExtractionError):
        wrapped()

    assert flaky.call_count == 1  # called exactly once, no retry attempted
    mock_sleep.assert_not_called()  # never slept -- confirms no retry loop ran

#-------------------------------------------------------------------------
# Unexpected (non-hierarchy) exceptions also propagate immediately 
#-------------------------------------------------------------------------

@patch("etl_framework.decorators.retry.time.sleep")
def test_unexpected_exception_is_not_retried(mock_sleep, make_retry_config):
    config = make_retry_config(max_retries=5)
    flaky = FlakyFunction(
        fail_times=999, exception_factory=lambda: ValueError("not part of the hierarchy")
    )
    wrapped = retry(config)(flaky)

    with pytest.raises(ValueError):
        wrapped()

    assert flaky.call_count == 1
    mock_sleep.assert_not_called()

#-------------------------------------------------------------------------
# Backoff formula: exponential (backoff_factor ** attempt) 
#-------------------------------------------------------------------------

@patch("etl_framework.decorators.retry.time.sleep")
def test_backoff_is_exponential_not_linear(mock_sleep, make_retry_config):
    """
    With backoff_factor=2.0 and 3 failing attempts before success, the
    expected wait sequence is 2**1, 2**2, 2**3 -> 2.0, 4.0, 8.0.

    A linear formula (backoff_factor * attempt) would instead produce
    2.0, 4.0, 6.0 -- identical on the first two calls, which is exactly why
    this repo's earlier linear-backoff bug went unnoticed until someone
    checked attempt 3 specifically. This test checks the full sequence, not
    just the first call, so it can't pass against a linear implementation
    by accident.
    """
    config = make_retry_config(max_retries=5, backoff_factor=2.0)
    flaky = FlakyFunction(
        fail_times=3, exception_factory=lambda: TransientExtractionError("timeout")
    )
    wrapped = retry(config)(flaky)

    wrapped()

    actual_waits = [call.args[0] for call in mock_sleep.call_args_list]
    assert actual_waits == [2.0, 4.0, 8.0]


@patch("etl_framework.decorators.retry.time.sleep")
def test_backoff_is_capped_at_max_wait_seconds(mock_sleep, make_retry_config):
    """
    The implementation caps computed backoff at 60s (min(backoff_factor **
    attempt, 60)) to avoid unbounded growth on a high backoff_factor with
    many attempts. With backoff_factor=10 and attempt 3, 10**3 = 1000,
    which must be capped down to 60.
    """
    config = make_retry_config(max_retries=5, backoff_factor=10.0)
    flaky = FlakyFunction(
        fail_times=3, exception_factory=lambda: TransientExtractionError("timeout")
    )
    wrapped = retry(config)(flaky)

    wrapped()

    actual_waits = [call.args[0] for call in mock_sleep.call_args_list]
    assert actual_waits == [10.0, 60.0, 60.0]

#-------------------------------------------------------------------------
# Retry-After: server-provided wait overrides the calculated exponential backoff 
#-------------------------------------------------------------------------

@patch("etl_framework.decorators.retry.time.sleep")
def test_retry_after_overrides_computed_backoff(mock_sleep, make_retry_config):
    config = make_retry_config(max_retries=3, backoff_factor=2.0)
    flaky = FlakyFunction(
        fail_times=2,
        exception_factory=lambda: RateLimitError("429", retry_after=17.0),
    )
    wrapped = retry(config)(flaky)

    wrapped()

    # Both failures carried retry_after=17.0, so BOTH sleep calls should use
    # 17.0 -- not the exponential sequence [2.0, 4.0] that would apply if
    # retry_after were being ignored.
    actual_waits = [call.args[0] for call in mock_sleep.call_args_list]
    assert actual_waits == [17.0, 17.0]


@patch("etl_framework.decorators.retry.time.sleep")
def test_retry_after_absent_falls_back_to_exponential(mock_sleep, make_retry_config):
    """A TransientExtractionError has no retry_after attribute at all --
    confirms the getattr(e, "retry_after", None) fallback path still
    computes exponential backoff normally when the exception doesn't carry
    the field."""
    config = make_retry_config(max_retries=3, backoff_factor=2.0)
    flaky = FlakyFunction(
        fail_times=2, exception_factory=lambda: TransientExtractionError("timeout")
    )
    wrapped = retry(config)(flaky)

    wrapped()

    actual_waits = [call.args[0] for call in mock_sleep.call_args_list]
    assert actual_waits == [2.0, 4.0]

