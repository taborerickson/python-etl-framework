# tests/test_exceptions.py 

"""
Pytest tests for the custom exception hierarchy. 

Covers: 
- isinstance() relationships for every class in the hierarchy (this is the
  mechanism the retry decorator relies on -- see test_retry.py)
- message pass-through (does the message you pass in come back out via str())
- MaxRetriesExceededError's custom __str__ formatting
- exception chaining (__cause__) -- confirms "raise X from Y" actually wires
  up __cause__, since several parts of the framework rely on this for
  debuggability (you can walk last_exception.__cause__ in a log/debugger)
"""

import pytest 

from etl_framework.exceptions.pipeline_errors import (
    PipelineError,
    ExtractionError,
    TransientExtractionError,
    PermanentExtractionError,
    RateLimitError,
    NetworkError,
    ServerError,
    AuthenticationError,
    MalformedResponseError,
    SourceNotFoundError,
    MalformedFileError,
    MaxRetriesExceededError,
    LoadError,
    TransientLoadError,
    PermanentLoadError,
)

#-------------------------------------------------------------------------
# Hierarchy shape: every class's isinstance() relationships 
#-------------------------------------------------------------------------

@pytest.mark.parametrize(
    "exc_class, expected_ancestors",
    [
        (TransientExtractionError, (ExtractionError, PipelineError, Exception)),
        (PermanentExtractionError, (ExtractionError, PipelineError, Exception)),
        (RateLimitError, (TransientExtractionError, ExtractionError, PipelineError)),
        (NetworkError, (TransientExtractionError, ExtractionError, PipelineError)),
        (ServerError, (TransientExtractionError, ExtractionError, PipelineError)),
        (
            AuthenticationError,
            (PermanentExtractionError, ExtractionError, PipelineError),
        ),
        (
            MalformedResponseError,
            (PermanentExtractionError, ExtractionError, PipelineError),
        ),
        (
            SourceNotFoundError,
            (PermanentExtractionError, ExtractionError, PipelineError),
        ),
        (
            MalformedFileError,
            (PermanentExtractionError, ExtractionError, PipelineError),
        ),
        (TransientLoadError, (LoadError, PipelineError)),
        (PermanentLoadError, (LoadError, PipelineError)),
        # MaxRetriesExceededError deliberately sits under PipelineError directly,
        # NOT under ExtractionError -- it represents a retry-orchestration
        # failure, not a source failure. This test is a regression guard: if
        # someone "helpfully" reparents it under ExtractionError later, this
        # test catches it.
        (MaxRetriesExceededError, (PipelineError,)),
    ],
)
def test_exception_hierarchy_relationships(exc_class, expected_ancestors):
    instance = (
        exc_class(
            message="boom",
            operation="op",
            attempts=1,
            duration_seconds=0.1,
            last_exception=Exception("x"),
        )
        if exc_class is MaxRetriesExceededError
        else exc_class("boom")
    )
    for ancestor in expected_ancestors:
        assert isinstance(instance, ancestor)

def test_max_retries_exceeded_is_not_an_extraction_error():
    """Explicit negative check for the design decision called out above."""
    m = MaxRetriesExceededError(
        message="boom",
        operation="op",
        attempts=1,
        duration_seconds=0.1,
        last_exception=Exception("x"),
    )
    assert not isinstance(m, ExtractionError)

#-------------------------------------------------------------------------
# Message pass-through 
#-------------------------------------------------------------------------

@pytest.mark.parametrize(
    "exc_class",
    [
        NetworkError,
        ServerError,
        AuthenticationError,
        MalformedResponseError,
        SourceNotFoundError,
        MalformedFileError,
    ],
)
def test_message_passes_through_str(exc_class):
    e = exc_class("Connection timed out")
    assert str(e) == "Connection timed out"


def test_rate_limit_error_message_and_optional_retry_after():
    e = RateLimitError("429 from API")
    assert str(e) == "429 from API"
    assert e.retry_after is None  # optional field, default None

    e2 = RateLimitError("429 from API", retry_after=5.5)
    assert e2.retry_after == 5.5


#-------------------------------------------------------------------------
# MaxRetriesExceededError.__str__ formatting 
#-------------------------------------------------------------------------

def test_max_retries_exceeded_str_formatting():
    original = NetworkError("Connection timed out")
    m = MaxRetriesExceededError(
        message="Max retries exceeded",
        operation="extract_contacts",
        attempts=3,
        duration_seconds=12.4,
        last_exception=original,
    )
    assert str(m) == (
        "Max retries exceeded | operation=extract_contacts | attempts=3 | "
        "duration=12.40s | last_exception=NetworkError: Connection timed out"
    )
    assert m.operation == "extract_contacts"
    assert m.attempts == 3
    assert m.duration_seconds == 12.4
    assert m.last_exception is original


#-------------------------------------------------------------------------
# Exception chaining (__cause__) 
#-------------------------------------------------------------------------

def test_exception_chaining_sets_cause():
    """
    `raise X from Y` sets X.__cause__ = Y. This matters here because
    MaxRetriesExceededError is always raised `from last_exception` in the
    retry decorator (see decorators/retry.py) -- __cause__ is what lets you
    walk from "retries exhausted" back to the actual root-cause exception
    from the final failed attempt, in a debugger or in log tooling that
    understands exception chains.
    """
    root_cause = NetworkError("Connection reset")
    try:
        try:
            raise root_cause
        except NetworkError as e:
            raise MaxRetriesExceededError(
                message="Max retries exceeded",
                operation="extract",
                attempts=3,
                duration_seconds=1.0,
                last_exception=e,
            ) from e
    except MaxRetriesExceededError as m:
        assert m.__cause__ is root_cause
        assert m.last_exception is root_cause

