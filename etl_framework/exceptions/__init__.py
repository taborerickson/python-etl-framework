# etl_framework/exceptions/__init__.py
"""Public exception hierarchy for extraction and load failures."""

from etl_framework.exceptions.pipeline_errors import (
    PipelineError, ExtractionError, TransientExtractionError, 
    PermanentExtractionError, RateLimitError, NetworkError, ServerError, 
    AuthenticationError, MalformedResponseError, MalformedFileError, 
    SourceNotFoundError, MaxRetriesExceededError, LoadError, 
    TransientLoadError, PermanentLoadError,
)

__all__ = [
    "PipelineError", "ExtractionError", "TransientExtractionError", 
    "PermanentExtractionError", "RateLimitError", "NetworkError", "ServerError", 
    "AuthenticationError", "MalformedResponseError", "MalformedFileError", 
    "SourceNotFoundError", "MaxRetriesExceededError", "LoadError", 
    "TransientLoadError", "PermanentLoadError",
]
