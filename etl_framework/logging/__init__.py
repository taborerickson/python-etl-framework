# etl_framework/logging/__init__.py

"""Public structured-logging setup and logger factory."""

from etl_framework.logging.logger import configure_logging, get_logger

__all__ = ["configure_logging", "get_logger"]
