# etl_framework/exceptions/pipeline_errors.py 
# Custom Exception Hierarchy 

"""
Custom exception hierarchy for the ETL framework. 

All framework exceptions inherit from PipelineError. 
The hierarchy is designed so the retry decorator can distinguish
transient (retryable) from permanent (non-retryable) failures 
using isinstance() checks without knowing specific exception types. 
"""

# PipelineError 
class PipelineError(Exception):
    """Base class for all pipeline errors.""" 
    def __init__(self, message: str) -> None:  
        super().__init__(message) 

#===================================================

# ------ Extraction Exceptions -------

# ExtractionError
class ExtractionError(PipelineError): 
    """Base class for all extraction failures. Catch this to handle any extraction error."""

# TransientExtractionError 
class TransientExtractionError(ExtractionError): 
    """Base class for retryable extraction failures. The retry decorator retries these."""

# RateLimitError 
class RateLimitError(TransientExtractionError):
    """
    Raised when the source API returns HTTP 429 (rate limit exceeded).
    If the response included a Retry-After header, its value (in seconds) is
    captured on `retry_after` so the retry decorator can honor the server's 
    requested wait instead of calculating its own backoff. 
    """ 
    def __init__(self, message: str, retry_after: float | None = None) -> None: 
        super().__init__(message) 
        self.retry_after = retry_after

# NetworkError 
class NetworkError(TransientExtractionError): 
    """Raised on network-level failures: timeouts, connection resets, DNS failures."""
    def __init__(self, message: str) -> None: 
        super().__init__(message) 

# ServerError  
class ServerError(TransientExtractionError):
    """Raised on HTTP 5xx responses indicating a server-side failure.""" 
    def __init__(self, message: str) -> None: 
        super().__init__(message) 

# PermanentExtractionError 
class PermanentExtractionError(ExtractionError):
    """Base class for non-retryable extraction failures. The retry decorator re-raises these immediately.""" 

# AuthenticationError 
class AuthenticationError(PermanentExtractionError): 
    """Raised on HTTP 401 or 403 responses indicating invalid or missing credentials."""
    def __init__(self, message: str) -> None: 
        super().__init__(message) 

# MalformedResponseError  
class MalformedResponseError(PermanentExtractionError):
    """Raised when the API response cannot be parsed — unexpected format or invalid JSON.""" 
    def __init__(self, message: str) -> None: 
        super().__init__(message) 

# SourceNotFoundError 
class SourceNotFoundError(PermanentExtractionError):
    """Raised on HTTP 404 or when a configured file path does not exist.""" 
    def __init__(self, message: str) -> None: 
        super().__init__(message) 

# MalformedFileError 
class MalformedFileError(PermanentExtractionError): 
    """
    Raised when a source file cannot be parsed: bad encoding, malformed CSV 
    structure, or otherwise unreadable content. The file-based sibling of 
    MalformedResponseError.
    """
    def __init__(self, message: str) -> None: 
        super().__init__(message) 

#===================================================

# Max Retry Exceeded Error 
class MaxRetriesExceededError(PipelineError):
    """Maximum retries exceeded for retryable failures""" 
    def __init__(
            self, 
            message: str, 
            operation: str,     # name of the function/operation that was retried 
            attempts: int,      # how many attempts were made 
            duration_seconds: float,    # total elapsed time across all attempts 
            last_exception: Exception,  # the actual exception from the final attempt
    ):
        super().__init__(message) 
        self.operation = operation 
        self.attempts = attempts 
        self.duration_seconds = duration_seconds
        self.last_exception = last_exception 

    def __str__(self) -> str: 
        return (
            f"{self.args[0]} | "
            f"operation={self.operation} | "
            f"attempts={self.attempts} | "
            f"duration={self.duration_seconds:.2f}s | "
            f"last_exception={type(self.last_exception).__name__}: {self.last_exception}"
        )

#===================================================

# ------ Load Exceptions -------

# LoadError
class LoadError(PipelineError): 
    """Base class for all load failures. Catch this to handle any load error."""

# TransientLoadError
class TransientLoadError(LoadError): 
    """Base class for retryable load failures. Mirrors TransientExtractionError on the load side."""


# PermanentLoadError 
class PermanentLoadError(LoadError): 
    """Base class for non-retryable load failures. Mirrors PermanentExtractionError on the load side."""

