# tests/test_exceptions.py 

"""Smoke test for custom exception hierarchy."""

from etl_framework.exceptions.pipeline_errors import(
    NetworkError, 
    MaxRetriesExceededError, 
    TransientExtractionError
)

# test message should pass through correctly 
e = NetworkError("Connection timed out") 
print(str(e)) # should print: Connection timed out 

# test that isinstance checks works correctly 
print(isinstance(e, TransientExtractionError)) # True 
print(isinstance(e, NetworkError)) # True

# test for MaxRetriesExceededError __str__
m = MaxRetriesExceededError(
    message="Max retries exceeded", 
    operation="extract_contacts", 
    attempts=3, 
    duration_seconds=12.4, 
    last_exception=e 
)
print(str(m)) 
# should print: Max retries exceeded | operation=extract_contacts | attempts=3 | duration=12.40s | last_exception=NetworkError: Connection timed out

