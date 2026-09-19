# etl_framework/loaders/__init__.py

"""Concrete loader implementations for persisting records."""

from etl_framework.loaders.parquet_loader import ParquetLoader

__all__ = ["ParquetLoader"]
