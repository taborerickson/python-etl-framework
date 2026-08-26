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

Retry design: 
    extract() is a generator that yields records one at a time. The actual 
    network call happens inside _fetch_page(), a private eager helper that 
    either fully succeeds (returns a complete batch) or fully fails (raises). 
    It is this method (not extract() itself) that is wrapped with the retry() 
    decorator (etl_framework/decorators/retry.py). This split is necessary 
    because a generator function's body does not execute until it is iterated: 
    wrapping extract() directly with retry() would only ever protect the (instant,
    always-successful) construction of the generator object, not the HTTP call 
    that happens later once iteration begins. Retrying _fetch_page() instead 
    guarantees any retry is fully resolved before a single record is yielded 
    downstream, so a mid-retry failure can never result in duplicate or partial 
    records reaching a caller. 

Yields records one at a time regardless of the source API's raw response 
shape (bare list, {'results': [...]}, or {'data': [...]}), so downstream 
loaders never need to know the source API's response format. 
"""

import requests

from etl_framework.base.extractor import BaseExtractor
from etl_framework.config.models import APIConfig
from etl_framework.decorators.retry import retry
from etl_framework.exceptions.pipeline_errors import (
    AuthenticationError,
    MalformedResponseError,
    NetworkError,
    RateLimitError,
    ServerError,
    SourceNotFoundError,
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
  

    def _fetch_page(self, page: int) -> list[dict]: 
        """
        Performs a single, retryable GET request against self.config.url 
        and returns the parsed response as a bounded list of records. 

        This method is the retry-protected unit: it either fully succeeds (returns 
        a complete, valid batch) or fully fails (raises), with no partial state 
        escaping. A retry here can never produce duplicate or partial records 
        downstream, because nothing has been yielded to a caller yet when a retry 
        is triggered. 

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
                params={"page": page, "per_page": self.config.page_size}, 
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
            retry_after_header = response.headers.get("Retry-After") 
            retry_after = float(retry_after_header) if retry_after_header else None 
            raise RateLimitError(
                f"Rate limited by {self.config.url} (429)",
                retry_after=retry_after,  
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


    def extract(self):
        """
        Generator. Yields records one at a time from the source. 

        Wraps self._fetch_page() with the configured retry policy (retried if it raises 
        TransientExceptionError). Once a page is successfully fetched, its records are 
        yielded individually to the caller. 

        Stopping condition: the source is presumed exhausted once a page 
        comes back empty, or with fewer records than config.page_size (a 
        short page is the conventional "this was the last page" signal for 
        offset/page-number-style pagination).
        """
        protected_fetch = retry(self.config.retry_config)(self._fetch_page) 

        page_number = 1 
        while True: 
            page_records = protected_fetch(page_number) 

            if not page_records: 
                break 

            yield from page_records 

            if len(page_records) < self.config.page_size: 
                break 

            page_number += 1 

