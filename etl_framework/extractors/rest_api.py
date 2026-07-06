# extractors/rest_api.py 

"""
REST API Extractor 

Concrete extractor for pulling data from REST API endpoints via HTTP GET.
Implements BaseExtractor's abstract extract() method using a persistent
requests.Session for connection reuse and auth header management. 

Maps HTTP-layer failures to the framework's exception hierarchy: 
    - Connection/timeout errors     -> NetworkError (transient) 
    - 5xx responses                 -> ServerError (transient)
    - 429 responses                 -> RateLimitError (transient) 
    - 401/403 responses             -> AuthenticationError (permanent) 
    - 404 responses                 -> SourceNotFoundError (permanent) 
    - Unparseable/malformed body    -> MalformedResponseError (permanent) 
    - 200 status with an error payload in the body is also treated as 
      MalformedResponseError, since a 200 only confirms transport-level 
      success, not application-level success. 

Scop limitations (by design): 
    - Single-page extraction only. Pagination to be implemented in a 
      future revision and is not handled in this implementation. 
    - Retry logic is not implemented here. This module raises the 
      appropriate exception on failure and returns cleanly on success; 
      retry/backoff behavior is orchestrated by BaseExtractor.run() 
      via the retry() decorator (etl_framework/decorators/retry.py). 

Returns extracted records as list[dict] regardless of the source API's 
raw response shape (bare list, {'results': [...]}, or {'data': [...]}), 
so downstream loaders never need to know the source API's response format. 
"""

import requests 
from etl_framework.base.extractor import BaseExtractor 
from etl_framework.config.models import APIConfig 
from etl_framework.exceptions.pipeline_errors import (
    NetworkError, 
    ServerError, 
    RateLimitError, 
    AuthenticationError, 
    SourceNotFoundError, 
    MalformedResponseError, 
)

# RestApiExtractor 
class RestApiExtractor(BaseExtractor): 
    """
    Extracts data from a REST API endpoint via a single-page GET request. 

    Pagination is not yet supported. 
    """

    def __init__(self, config: APIConfig) -> None: 
        super().__init__(config)  
        self.session = requests.Session() 
        self.session.headers.update({
            "Authorization": f"Bearer {config.auth_token}", 
            "Accept": "application/json",
        })
  

    def extract(self) -> list[dict]: 
        """
        Performs a single-page GET request against self.config.url
        and returns the parsed response as a list of records. 

        Raises: 
            NetworkError: on connection failure or timeout (transient)
            ServerError: on 5xx responses (transient) 
            RateLimitError: on 429 responses (transient) 
            AuthenticationError: on 401/403 responses (permanent) 
            SourceNotFoundError: on 404 responses (permanent) 
            MalformedResponseError: on unparseable response body (permanent) 
        """
        try: 
            response = self.session.get(
                self.config.url, 
                timeout=self.config.timeout_seconds, 
            )
        except requests.exceptions.Timeout as e: 
            raise NetworkError(
                f"Request to {self.config.url} timed out after "
                f"{self.config.timeout_seconds}s" 
            ) from e 
        except requests.exceptions.ConnectionError as e: 
            raise NetworkError(
                f"Failed to connect to {self.config.url}"
            ) from e 

        status = response.status_code 

        if status == 429: 
            raise RateLimitError(
                f"Rate limited by {self.config.url} (429)" 
            )
        if 500 <= status < 600: 
            raise ServerError(
                f"Server error from {self.config.url}: HTTP {status}"
            )
        if status in (401, 403): 
            raise AuthenticationError(
                f"Authentication failed for {self.config.url}: HTTP {status}"
            )
        if status == 404: 
            raise SourceNotFoundError(
                f"Resource not found: {self.config.url}"
            )
        if status != 200: 
            # Catch-all for any other unexpected status 
            raise MalformedResponseError(
                f"Unexpected status {status} from {self.config.url}"
            )
        
        try: 
            payload = response.json() 
        except ValueError as e: 
            raise MalformedResponseError(
                f"Could not parse JSON response from {self.config.url}"
            ) from e 
        
        if isinstance(payload, dict) and payload.get('error'): 
            raise MalformedResponseError(
                f"API returned 200 but reported an error: {payload.get('error')}"
            )
        
        if isinstance(payload, list): 
            return payload 
        if isinstance(payload, dict) and 'results' in payload: 
            return payload['results'] 
        if isinstance(payload, dict) and 'data' in payload: 
            return payload['data'] 
        
        raise MalformedResponseError(f"Unexpected payload shape from {self.config.url}")
        


