# tests/test_csv_extractor.py 

"""
Tests for CSVExtractor (etl_framework/extractors/csv.py).

Mirrors the REST API test coverage where it applies:
- generator behavior confirmation
- valid file parsing
- missing-file handling -> SourceNotFoundError
- malformed-row/encoding handling -> MalformedFileError
- confirms NO retry logic is invoked on failure -- this extractor
  deliberately has no retry wrapping, and this test suite locks that
  decision in so a future edit can't silently reintroduce retry logic
  without a test noticing the behavior changed.
"""

import csv
import inspect
from unittest.mock import patch

import pytest

from etl_framework.exceptions.pipeline_errors import (
    MalformedFileError,
    SourceNotFoundError,
)
from etl_framework.extractors.csv import CSVExtractor


def test_extract_is_a_generator_function():
    assert inspect.isgeneratorfunction(CSVExtractor.extract)


def test_valid_csv_parses_into_records(tmp_path, make_csv_config):
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("id,name,amount\n1,Alice,100\n2,Bob,200\n")

    config = make_csv_config(file_path=str(csv_path))
    extractor = CSVExtractor(config)

    records = list(extractor.extract())

    assert records == [
        {"id": "1", "name": "Alice", "amount": "100"},
        {"id": "2", "name": "Bob", "amount": "200"},
    ]


def test_missing_file_raises_source_not_found(tmp_path, make_csv_config):
    missing_path = tmp_path / "does_not_exist.csv"
    config = make_csv_config(file_path=str(missing_path))
    extractor = CSVExtractor(config)

    with pytest.raises(SourceNotFoundError):
        list(extractor.extract())


def test_missing_file_does_not_raise_until_iterated(tmp_path, make_csv_config):
    """
    extract() is a generator: calling it just builds the generator object,
    it does not run any code yet -- so constructing against a nonexistent
    path raises nothing. The error only fires once you start pulling
    records via next()/iteration, because that's when the `with open(...)`
    line actually executes.
    """
    missing_path = tmp_path / "does_not_exist.csv"
    config = make_csv_config(file_path=str(missing_path))
    extractor = CSVExtractor(config)

    gen = extractor.extract()  # no error here
    with pytest.raises(SourceNotFoundError):
        next(gen)  # error only surfaces here


def test_malformed_csv_structure_raises_malformed_file_error(tmp_path, make_csv_config):
    """
    Triggers a real csv.Error (not a mock) by temporarily lowering
    csv.field_size_limit() so a genuinely oversized field trips Python's
    own "field larger than field limit" check -- a real way a CSV file's
    *structure* can be unparseable, distinct from the UnicodeDecodeError
    case tested below. The limit is restored after the test so it can't
    leak into other tests.
    """
    csv_path = tmp_path / "oversized_field.csv"
    oversized_value = "x" * 200
    csv_path.write_text(f"id,name\n1,{oversized_value}\n")

    original_limit = csv.field_size_limit()
    csv.field_size_limit(50)  # smaller than the 200-char field above
    try:
        config = make_csv_config(file_path=str(csv_path))
        extractor = CSVExtractor(config)

        with pytest.raises(MalformedFileError):
            list(extractor.extract())
    finally:
        csv.field_size_limit(original_limit)  # never leave global state changed


def test_bad_encoding_raises_malformed_file_error(tmp_path, make_csv_config):
    csv_path = tmp_path / "bad_encoding.csv"
    # A lone 0xff byte is invalid utf-8 and breaks decoding
    csv_path.write_bytes(b"id,name\n1,\xffBadName\n")

    config = make_csv_config(file_path=str(csv_path), encoding="utf-8")
    extractor = CSVExtractor(config)

    with pytest.raises(MalformedFileError):
        list(extractor.extract())


def test_no_retry_logic_is_invoked_on_failure(tmp_path, make_csv_config):
    """
    Locks in the deliberate design decision that CSVExtractor never wraps
    anything with the retry() decorator.
    """
    missing_path = tmp_path / "does_not_exist.csv"
    config = make_csv_config(file_path=str(missing_path))
    extractor = CSVExtractor(config)

    with patch("etl_framework.decorators.retry.time.sleep") as mock_sleep:
        with pytest.raises(SourceNotFoundError):
            list(extractor.extract())
        mock_sleep.assert_not_called()


def test_run_streams_records_end_to_end(tmp_path, make_csv_config):
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("id,name\n1,Alice\n2,Bob\n")
    config = make_csv_config(file_path=str(csv_path))
    extractor = CSVExtractor(config)

    records = list(extractor.run())

    assert records == [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]


