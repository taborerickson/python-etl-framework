# config/models.py 
# Config layer

"""
Config models for the ETL framework 

Defines the Pydantic-based configuration hierarchy used by all extractors. 
Config objects are validated at instantiation -- fail fast on bad configs. 

Hierarchy: 
    RetryConfig -- retry behavior (attempts, backoff, retryable exceptions) 
    ExtractorConfig -- base config shared by all extractor types 
        APIConfig -- extends ExtractorConfig for REST API sources 
        CSVConfig -- extends ExtractorConfig for CSV file sources 
"""

from pydantic import BaseModel, ConfigDict, Field
from etl_framework.exceptions.pipeline_errors import TransientExtractionError

# RetryConfig 
class RetryConfig(BaseModel): 
    """Retry config for retry behavior"""
    model_config = ConfigDict(arbitrary_types_allowed=True) # allowing Pydantic to validate arbitrary types
    
    max_retries: int = 3 # default 3 retries 
    backoff_factor: float = 2.0  # default multiplier for exponential backoff 
    # creates new list per instance 
    # avoids mutable default bug 
    retry_on: list[type[Exception]] = Field(
        default_factory=lambda: [TransientExtractionError]
    )

# ExtractorConfig 
class ExtractorConfig(BaseModel): 
    """Base identity config for all extractor configs"""
    source_name: str
    pipeline_run_id: str
    retry_config: RetryConfig = Field(default_factory=RetryConfig)

# APIConfig 
class APIConfig(ExtractorConfig): 
    """API-specific config. Extends base for REST API sources"""
    url: str
    auth_token: str
    page_size: int = 100 # default: 100 records per call 
    timeout_seconds: int = 30 # default: 30 seconds timeout 

# CSVConfig 
class CSVConfig(ExtractorConfig): 
    """CSV-specific config. Extends base for file sources"""
    file_path: str 
    delimiter: str = "," # default CSV (,)
    encoding: str = "utf-8" # default utf-8 encoding 



