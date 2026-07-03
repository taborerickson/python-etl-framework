# tests/test_pydantic_retry_model.py

"""Verification test to ensure `isinstance(e, tuple(config.retry_on))` works with a live Pydantic `RetryConfig`."""

from etl_framework.config.models import RetryConfig 
from etl_framework.exceptions.pipeline_errors import TransientExtractionError, RateLimitError

config = RetryConfig() # default: retry_on = [TransientExtractionError] 

# Simulating a RateLiimitError 
try: 
    raise RateLimitError("429 from API") 
except Exception as e: 
    print(isinstance(e, tuple(config.retry_on))) # should print True
    print(type(config.retry_on)) # should be a list 
    print(config.retry_on) # should show [<class '...TransientExtractionError'>]

