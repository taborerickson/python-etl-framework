# decorators/retry.py 

"""
Retry Decorator Factory 

Provides the `retry` decorator factory, which wraps an extraction method 
with configurable retry logic driven by a RetryConfig instance. 

Behavior: 
- TransientExtractionError (and subclasses): triggers exponential backoff and retry.
- PermanentExtractionError (and subclasses): re-raised immediately, no retry.
- Unexpected exceptions: re-raised immediately. 
- Retries exhausted: raises MaxRetriesExceededError with structured metadata, 
  chained from the last exception. 

Backoff formula: wait = backoff_factor ** attempt_number (exponential scaling). 

Usage: 
    config = RetryConfig(max_retries=3, backoff_factor=2.0) 

    @retry(config) 
    def extract(self): 
        ... 
"""

import time 
from functools import wraps 
from etl_framework.config.models import RetryConfig 
from etl_framework.exceptions.pipeline_errors import (
    PermanentExtractionError, 
    MaxRetriesExceededError, 
)

def retry(config: RetryConfig): 
    """Decorator factory. Accepts config and returns a decorator."""
    
    def retry_decorator(func): 
        """Accepts a function and returns a wrapper with retry logic applied."""
        
        @wraps(func) 
        def wrapper(*args, **kwargs):  
            attempt = 0 
            last_exception = None # initializing as None 
            start_time = time.time() 

            while True:
                try:
                    return func(*args, **kwargs)  
                
                except PermanentExtractionError: 
                    raise # re-raising active exception 
                
                except Exception as e: 
                    last_exception = e

                    if isinstance(e, tuple(config.retry_on)):
                        attempt += 1 

                        if attempt > config.max_retries: 
                            duration = time.time() - start_time 
                            # passing MaxRetriesExceededError parameters from config
                            raise MaxRetriesExceededError(
                                message=f"Max retries exceeded for '{func.__name__}'",
                                operation=func.__name__, 
                                attempts=attempt, 
                                duration_seconds=duration, 
                                last_exception=last_exception, 
                            ) from last_exception 
                        
                        max_wait_seconds = 60 # adding max wait time to avoid unbounded growth 
                        wait = min(config.backoff_factor ** attempt, max_wait_seconds)  
                        time.sleep(wait) 
                         
                    else: 
                        raise # re-raising original exception  
        return wrapper 
    return retry_decorator 
