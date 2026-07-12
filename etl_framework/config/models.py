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
from datetime import datetime, timezone 
from uuid import uuid4 
from etl_framework.exceptions.pipeline_errors import TransientExtractionError

# auto-generated pipeline_run_id 
def _generate_pipeline_run_id() -> str: 
    """
    Generates a sortable, effectively-collision-free pipeline_run_id in 
    the form YYYYMMDDTHHMMSS-<6 hex chars>, e.g. "20260712T151530-a3f9c1".

    The timestamp prefix means run IDs sort chronologically and read 
    naturally in log output at a glance (closer to what Airflow's run_id looks 
    like). The short uuid4 suffix guards against two runs starting in the same 
    second (e.g. parallel or backfill runs) still ending up with distinct IDs. 
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    suffix = uuid4().hex[:6] 
    return f"{timestamp}-{suffix}"

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
    pipeline_run_id: str = Field(default_factory=_generate_pipeline_run_id) 
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



