# etl_framework/extractors/__init__.py

"""Concrete extractor implementations: REST API and local CSV sourcese."""

from etl_framework.extractors.rest_api import RestApiExtractor 
from etl_framework.extractors.csv import CSVExtractor 

__all__ = ["RestApiExtractor", "CSVExtractor"]
