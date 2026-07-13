# etl_framework/base/__init__.py

"""Abstract base classes defining the extractor and loader contracts."""

from etl_framework.base.extractor import BaseExtractor 
from etl_framework.base.loader import BaseLoader 

__all__ = ["BaseExtractor", "BaseLoader"]
