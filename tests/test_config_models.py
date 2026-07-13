# tests/test_config_models.py 

"""
Pydantic validation tests for the config layer.

Covers:
- required-field enforcement (fail-fast: missing fields raise ValidationError
  at construction, not a KeyError deep in some extractor later)
- default values apply correctly
- RetryConfig.retry_on's default_factory produces an independent list per
  instance (regression guard against the classic Python mutable-default-arg
  bug, reframed for Pydantic's default_factory=)
- pipeline_run_id's auto-generation: distinct per instance, matches the
  expected format, and can still be explicitly overridden
"""

import re

import pytest
from pydantic import ValidationError

from etl_framework.config.models import (
    RetryConfig,
    ExtractorConfig,
    APIConfig,
    CSVConfig,
)
from etl_framework.exceptions.pipeline_errors import TransientExtractionError


#-------------------------------------------------------------------------
# Required-field / fail-fast validation 
#-------------------------------------------------------------------------

def test_extractor_config_missing_source_name_raises():
    with pytest.raises(ValidationError):
        ExtractorConfig()  # source_name is required, no default


def test_api_config_missing_required_fields_raises():
    with pytest.raises(ValidationError):
        APIConfig(source_name="crm_api")  # missing url and auth_token


def test_csv_config_missing_file_path_raises():
    with pytest.raises(ValidationError):
        CSVConfig(source_name="local_file")  # missing file_path

#-------------------------------------------------------------------------
# Defaults apply correctly 
#-------------------------------------------------------------------------

def test_retry_config_defaults():
    config = RetryConfig()
    assert config.max_retries == 3
    assert config.backoff_factor == 2.0
    assert config.retry_on == [TransientExtractionError]


def test_api_config_defaults():
    config = APIConfig(
        source_name="crm_api", url="https://api.example.com", auth_token="secret"
    )
    assert config.page_size == 100
    assert config.timeout_seconds == 30
    # nested default: retry_config defaults to a fresh RetryConfig
    assert isinstance(config.retry_config, RetryConfig)
    assert config.retry_config.max_retries == 3


def test_csv_config_defaults():
    config = CSVConfig(source_name="local_file", file_path="/tmp/data.csv")
    assert config.delimiter == ","
    assert config.encoding == "utf-8"

#-------------------------------------------------------------------------
# retry_on isinstance-tuple check against a real config 
#-------------------------------------------------------------------------

def test_retry_on_supports_isinstance_tuple_check_against_subclasses():
    """
    The retry decorator does `isinstance(e, tuple(config.retry_on))` to
    decide whether a raised exception is retryable (see
    decorators/retry.py). This test confirms that mechanism actually works
    against a live, default-constructed RetryConfig: retry_on defaults to
    [TransientExtractionError], and a *subclass* of it (RateLimitError)
    must still satisfy the isinstance check, since the whole point of the
    hierarchy is that the retry decorator never needs to know about
    specific subclasses like RateLimitError -- only the
    TransientExtractionError base class.
    """
    config = RetryConfig()  # default: retry_on = [TransientExtractionError]

    from etl_framework.exceptions.pipeline_errors import RateLimitError

    try:
        raise RateLimitError("429 from API")
    except Exception as e:
        assert isinstance(e, tuple(config.retry_on))

    assert isinstance(config.retry_on, list)
    assert config.retry_on == [TransientExtractionError]

#-------------------------------------------------------------------------
# retry_on default_factory independence (mutable-default regression guard) 
#-------------------------------------------------------------------------

def test_retry_on_default_factory_produces_independent_lists():
    """
    If `retry_on` had been declared as a plain mutable default (e.g.
    `retry_on: list = [TransientExtractionError]` instead of
    `Field(default_factory=lambda: [...])`), every RetryConfig instance that
    didn't pass retry_on explicitly would share the *same* underlying list
    object. Mutating one instance's retry_on would silently corrupt every
    other instance's retry_on -- a classic, hard-to-diagnose bug because it
    only shows up when two configs interact, not in isolation.

    default_factory sidesteps this: the factory function runs fresh for each
    instance, so each RetryConfig gets its own list object.
    """
    config_a = RetryConfig()
    config_b = RetryConfig()

    assert config_a.retry_on == config_b.retry_on  # equal in content
    assert config_a.retry_on is not config_b.retry_on  # NOT the same object

    # Prove independence: mutating one must not affect the other.
    config_a.retry_on.append(ValueError)
    assert ValueError in config_a.retry_on
    assert ValueError not in config_b.retry_on

#-------------------------------------------------------------------------
# pipeline_run_id auto-generation
#-------------------------------------------------------------------------

PIPELINE_RUN_ID_PATTERN = re.compile(r"^\d{8}T\d{6}-[0-9a-f]{6}$")


def test_pipeline_run_id_auto_generates_distinct_values():
    """
    Same regression class as retry_on above: pipeline_run_id uses
    default_factory=_generate_pipeline_run_id rather than a plain default=.
    A plain default= would be evaluated ONCE at class-definition time and
    every instance would share the identical run ID -- a silent, severe bug
    for a field whose entire purpose is to be unique per run.
    """
    config_a = ExtractorConfig(source_name="a")
    config_b = ExtractorConfig(source_name="b")
    assert config_a.pipeline_run_id != config_b.pipeline_run_id


def test_pipeline_run_id_matches_expected_format():
    config = ExtractorConfig(source_name="a")
    assert PIPELINE_RUN_ID_PATTERN.match(config.pipeline_run_id), (
        f"pipeline_run_id {config.pipeline_run_id!r} did not match "
        f"YYYYMMDDTHHMMSS-<6 hex chars>"
    )


def test_pipeline_run_id_explicit_override_still_works():
    config = ExtractorConfig(source_name="a", pipeline_run_id="custom-run-001")
    assert config.pipeline_run_id == "custom-run-001"
