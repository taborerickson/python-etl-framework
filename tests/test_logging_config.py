# tests/test_logging_config.py 

"""
Smoke tests for configure_logging() / get_logger()
(etl_framework/logging/logger.py).
"""

import pytest
from structlog.testing import capture_logs

from etl_framework.logging.logger import configure_logging, get_logger


@pytest.mark.parametrize(
    "level, environment",
    [
        ("INFO", "development"),
        ("DEBUG", "development"),
        ("INFO", "production"),
        ("WARNING", "invalid_environment_value"),  # falls back to JSON per docstring
    ],
)
def test_configure_logging_does_not_raise(level, environment):
    configure_logging(level=level, environment=environment)


def test_get_logger_returns_usable_bound_logger():
    logger = get_logger("some.module").bind(source_name="x")
    for method_name in ("info", "error", "warning", "debug", "bind"):
        assert hasattr(logger, method_name)


def test_capture_logs_works_regardless_of_configure_logging_state():
    """
    Confirms capture_logs() (used throughout test_base_extractor.py) isn't
    accidentally dependent on configure_logging() having been called first
    -- or on which environment it was last called with.
    """
    configure_logging(level="INFO", environment="production")
    logger = get_logger("x").bind(a=1)

    with capture_logs() as cap_logs:
        logger.info("hello", b=2)

    assert cap_logs == [{"a": 1, "b": 2, "event": "hello", "log_level": "info"}]

