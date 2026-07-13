# tests/test_base_extractor.py 

"""
Tests for BaseExtractor.run() (etl_framework/base/extractor.py) -- the
template method every concrete extractor inherits.

Covers:
- run() logs extraction_started before iteration, and extraction_succeeded
  after it completes, with the correct final record_count
- on a mid-iteration failure, run() logs extraction_failed with the
  correct records_yielded_before_failure count, then re-raises the
  original exception unchanged
- run() is itself confirmed to be a generator (same laziness contract as
  every concrete extract())

Log events are captured via structlog.testing.capture_logs(), which
patches structlog's logger factory for the duration of the `with` block
and returns a plain list of dicts (one per log call) -- no real log output
is produced or parsed as text.
"""

import inspect

import pytest
from structlog.testing import capture_logs

from etl_framework.base.extractor import BaseExtractor
from etl_framework.config.models import ExtractorConfig
from etl_framework.exceptions.pipeline_errors import NetworkError


class _FakeSucceedingExtractor(BaseExtractor):
    """Concrete extractor stub that yields a fixed set of records."""

    def extract(self):
        yield {"id": 1}
        yield {"id": 2}
        yield {"id": 3}


class _FakeFailingExtractor(BaseExtractor):
    """Concrete extractor stub that yields two records, then raises."""

    def extract(self):
        yield {"id": 1}
        yield {"id": 2}
        raise NetworkError("connection reset mid-stream")


def test_run_is_a_generator_function():
    assert inspect.isgeneratorfunction(BaseExtractor.run)


def test_run_logs_started_and_succeeded_with_correct_record_count():
    config = ExtractorConfig(source_name="fake_source", pipeline_run_id="run-1")
    extractor = _FakeSucceedingExtractor(config)

    with capture_logs() as cap_logs:
        records = list(extractor.run())

    assert records == [{"id": 1}, {"id": 2}, {"id": 3}]

    events = [entry["event"] for entry in cap_logs]
    assert events == ["extraction_started", "extraction_succeeded"]

    succeeded_entry = cap_logs[1]
    assert succeeded_entry["record_count"] == 3
    assert succeeded_entry["log_level"] == "info"
    assert isinstance(succeeded_entry["duration_seconds"], float)


def test_run_logs_failed_with_correct_partial_count_and_reraises():
    config = ExtractorConfig(source_name="fake_source", pipeline_run_id="run-2")
    extractor = _FakeFailingExtractor(config)

    with capture_logs() as cap_logs:
        with pytest.raises(NetworkError, match="connection reset mid-stream"):
            list(extractor.run())

    events = [entry["event"] for entry in cap_logs]
    assert events == ["extraction_started", "extraction_failed"]

    failed_entry = cap_logs[1]
    # 2 records were yielded before the raise on the 3rd next() call
    assert failed_entry["records_yielded_before_failure"] == 2
    assert failed_entry["log_level"] == "error"


def test_bound_context_fields_appear_on_every_log_call():
    """source_name and pipeline_run_id are bound once in __init__ and
    should appear on every subsequent log event without being passed
    again."""
    config = ExtractorConfig(source_name="crm_api", pipeline_run_id="run-xyz")
    extractor = _FakeSucceedingExtractor(config)

    with capture_logs() as cap_logs:
        list(extractor.run())

    for entry in cap_logs:
        assert entry["source_name"] == "crm_api"
        assert entry["pipeline_run_id"] == "run-xyz"


def test_partial_iteration_does_not_log_succeeded():
    """
    If a caller stops iterating early (e.g. next() once, then abandons the
    generator) without exhausting it, run() should NOT have logged
    extraction_succeeded yet -- that log only fires once the for-loop
    inside run() actually completes.
    """
    config = ExtractorConfig(source_name="fake_source", pipeline_run_id="run-3")
    extractor = _FakeSucceedingExtractor(config)

    with capture_logs() as cap_logs:
        gen = extractor.run()
        first = next(gen)

    assert first == {"id": 1}
    events = [entry["event"] for entry in cap_logs]
    assert events == ["extraction_started"]  # succeeded never logged

