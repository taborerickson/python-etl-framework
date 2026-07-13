# etl_framework/__init__.py

"""
etl_framework 

Reusable, importable ETO framework: extract -> transform -> load, with 
structured logging, retry/backoff, and a Pydantic-validated config layer. 

Public API: everything below is importable directly from `etl_framework`, 
without reaching into submodules: 

    from etl_framework import RestApiExtractor, CSVExtractor, ParquetLoader
"""

from etl_framework.base.extractor import BaseExtractor 
from etl_framework.base.loader import BaseLoader 
from etl_framework.extractors.rest_api import RestApiExtractor 
from etl_framework.extractors.csv import CSVExtractor 
from etl_framework.loaders.parquet_loader import ParquetLoader 
from etl_framework.transformers.base import BaseTransformer 
from etl_framework.transformers.passthrough import PassThroughTransformer 
from etl_framework.config.models import (
    RetryConfig, 
    ExtractorConfig, 
    APIConfig, 
    CSVConfig, 
)
from etl_framework.decorators.retry import retry 
from etl_framework.logging.logger import configure_logging, get_logger

__all__ = [
    "BaseExtractor", "BaseLoader", "RestApiExtractor", 
    "CSVExtractor", "ParquetLoader", "BaseTransformer", 
    "PassThroughTransformer", "RetryConfig", "ExtractorConfig", 
    "APIConfig", "CSVConfig", "retry", 
    "configure_logging", "get_logger",
]
