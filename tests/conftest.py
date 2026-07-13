# tests/conftest.py 

"""
Shared pytest fixtures for the etl_framework test suite.

Fixtures here are intentionally generic (a config *factory*, not a single
fixed config) so individual test modules can override just the fields they
care about (e.g. a short timeout, a specific max_retries) without every
test file re-declaring the full config shape from scratch.
"""

import pytest

from etl_framework.config.models import APIConfig, CSVConfig, RetryConfig

@pytest.fixture
def make_retry_config():
    """Factory fixture: returns a function that builds a RetryConfig with
    sensible test defaults, overridable via kwargs."""

    def _make(**overrides):
        defaults = {"max_retries": 3, "backoff_factor": 2.0}
        defaults.update(overrides)
        return RetryConfig(**defaults)

    return _make


@pytest.fixture
def make_api_config(make_retry_config):
    """Factory fixture for APIConfig with a fast default RetryConfig."""

    def _make(**overrides):
        defaults = {
            "source_name": "test_api",
            "pipeline_run_id": "test-run-001",
            "url": "https://api.example.com/records",
            "auth_token": "test-token",
            "page_size": 100,
            "timeout_seconds": 5,
            "retry_config": make_retry_config(),
        }
        defaults.update(overrides)
        return APIConfig(**defaults)

    return _make


@pytest.fixture
def make_csv_config():
    """Factory fixture for CSVConfig."""

    def _make(**overrides):
        defaults = {
            "source_name": "test_csv",
            "pipeline_run_id": "test-run-002",
            "file_path": "/tmp/does_not_matter.csv",
        }
        defaults.update(overrides)
        return CSVConfig(**defaults)

    return _make


#-------------------------------------------------------------------------
# Sample payload shapes -- RestApiExtractor normalizes all three of these 
# into a flat list of records. 
#-------------------------------------------------------------------------

@pytest.fixture
def bare_list_payload():
    return [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]


@pytest.fixture
def results_wrapped_payload():
    return {"results": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]}


@pytest.fixture
def data_wrapped_payload():
    return {"data": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]}

