# Python ETL Framework - Modular Ingestion Library

> **Status: Active Development** - Core framework components are being built incrementally. 
> Reference the [implementation status](#implementation-status) table below.

---

The focus of this project is creating a reusable, installable Python ETL framework built for production-grade data engineering workflows. 
Rather than a collection of scripts, this is a structured library with abstract base classes, concrete implementations, Pydantic configuration management, retry logic with exponential backoff, structured logging, and full unit test coverage. 

This framework serves as the ingestion layer for the [Sales-Intelligence-Pipeline](https://github.com/taborerickson/sales-intelligence-pipeline) project - a medallion-architecture ETL pipeline with dbt, Airflow, and a RAG/AI layer. The Sales Intelligence Pipeline project's Bronze ingestion layer imports directly from this library. 

---

## Table of Contents 
- [Problem Statement](#problem-statement)
- [Architecture](#architecture) 
- [Project Structure](#project-structure) 
- [Implementation Status](#implementation-status) 
- [Key Design Patterns](#key-design-patterns) 
- [Installation](#installation)  
- [Usage](#usage)  
- [Configuration](#configuration)  
- [Running Tests](#running-tests) 
- [Key Design Decisions & Trade-offs](#key-design-decisions--trade-offs) 
- [Skills Demonstrated](#skills-demonstrated) 

---

## Problem Statement 

With a typical approach to ETL workflows, a Python script is written for each data source. Even if each script works: each one is slightly different, handles errors differently, logs differently, and retries differently. 
Every new data source requires starting from scratch. There is no shared contract for what an "extractor" is. You cannot swap out sources without rewriting the pipeline that uses them. 
<br>

**A framework solves this problem by defining a shared structure and enforced interface contracts:** 
- Every extractor behaves predictably regardless of the source type
- New sources can be added by subclassing `BaseExtractor`, without changing downstream code 
- Cross-cutting concerns (retry logic, logging, error handling) are written once and inherited everywhere 
<br> 

**Goals:**
- Abstract common ingestion patterns (REST API, file I/O) behind a consistent interface 
- Handle failures gracefully with configurable retry logic and exponential backoff 
- Classify failures as transient (retryable) or permanent (non-retryable) to avoid wasted retries 
- Emit structured, machine-readable logs suitable for observability tooling 
- Be installable and importable as a reusable package by downstream pipelines 

---

## Architecture 

> **In progress** - Architecture diagram will be added once the core extractor and loader components are complete. Will cover: ingestion layer, config flow, retry decorator, structured logging, and the relationship to the Sales Intelligence Pipeline Bronze layer. 

**How this framework relates to the spine project:**

```
python-etl-framework -> imported by sales-intelligence-pipeline 

BaseExtractor (ABC) -> Bronze layer ingestion tasks 
RestApiExtractor -> CRM API ingestion 
RetryConfig, APIConfig -> Config-driven Airflow task runs 
configure_logging() -> Structured observability across all layers 
```

**End-to-end call flow (once complete):**

```
Airflow Task
    └── configure_logging(level="INFO", environment="production")
    └── extractor = RestApiExtractor(config)
            └── BaseExtractor.__init__()     # binds logger, stores config
    └── extractor.run()                      # template method
            └── extract()                    # wrapped by @retry(config.retry_config)
                    ├── TransientError  →  backoff → retry → MaxRetriesExceededError
                    ├── PermanentError  →  re-raise immediately
                    └── success         →  yield records → ParquetLoader
```

---

## Project Structure 

```text
python-etl-framework/
│
├── etl_framework/                  # Main installable package 
│   ├── __init__.py
│   ├── base/
│   │   ├── __init__.py
│   │   └── extractor.py            # BaseExtractor ABC - in progress 
│   ├── extractors/
│   │   ├── __init__.py
│   │   ├── rest_api.py             # RestApiExtractor - in progress 
│   │   └── csv.py                  # CSVExtractor - in progress
│   ├── loaders/
│   │   ├── __init__.py
│   │   └── parquet_loader.py       # ParquetLoader - in progress  
│   ├── decorators/
│   │   ├── __init__.py
│   │   └── retry.py                # Retry decoratory factor - COMPLETE
│   ├── config/
│   │   ├── __init__.py
│   │   └── models.py               # Pydantic config models - COMPLETE
│   ├── exceptions/
│   │   ├── __init__.py
│   │   └── pipeline_errors.py      # Custom exception hierarchy - COMPLETE
│   └── logging/
│       ├── __init__.py
│       └── logger.py               # structlog setup - COMPLETE  
│
├── tests/
│   ├── __init__.py
│   ├── conftest.py                 # Shared fixtures
│   └── test_exceptions.py          # Exception hierarchy tests - COMPLETE
│
├── examples/
│   └── api_to_parquet.py           # End-to-end usage example - in progress
│
├── pyproject.toml                  # Package config + dependencies - COMPLETE
├── .gitignore
└── README.md
```

**Implementation status:**

| Component | Status | 
|---|---|
| `pyproject.toml` - package config, dependencies, tooling | COMPLETE | 
| `exceptions/pipeline_errors.py` - custom exception hierarchy | COMPLETE | 
| `tests/test_exceptions.py` - exception hierarchy smoke tests | COMPLETE | 
| `config/models.py` - Pydantic config models | COMPLETE | 
| `logging/logger.py` - structlog setup | COMPLETE | 
| `decorators/retry.py` - retry decorator | COMPLETE | 
| `base/extractor.py` - BaseExtractor (ABC) | In Progress | 
| `extractors/rest_api.py` - RestApiExtractor | Planned | 
| `extractors/csv.py` - CSVExtractor | Planned | 
| `loaders/parquet_loader.py` - ParquetLoader | Planned | 
| `examples/api_to_parquet.py` - end-to-end example | Planned | 

---

## Key Design Patterns 

### 1. Abstract Base Classes (ABC) 
All extractors, transformers, and loaders inherit from an abstract base class that enforces a consistent interface. 
`BaseExtractor` declares `extract()` as an abstract method via `abc.ABC` + `@abstractmethod`.  Adding a new source means subclassing `BaseExtractor` and implementing `.extract()`. Nothing else changes downstream. 

Enforcement happens at **instantiation time**, not when the missing method is first called: attempting to instantiate `BaseExtractor` directly, or any subclass that omits `extract()`, raises `TypeError` immediately. This was verified with a smoke test confirming both `BaseExtractor(config)` and an incomplete subclass correctly fail fast.

### 2. Template Method Pattern 
`BaseExtractor.run()` is a concrete method that handles all shared orchestration: logging extraction start, applying retry logic, calling `extract()`, logging success or failure with duration, and returning the result. Subclasses implement only `extract()` and never override `run()`. The orchestration logic is written exactly once.

### 3. Functional Decorator Application (not `@` syntax)
The retry decorator is applied to `extract()` **inside `run()`, at call time**, rather than with `@retry(...)` syntax above the method definition:

```python
protected_extract = retry(self.config.retry_config)(self.extract)
result = protected_extract()
```

**Why:** `@` decorator syntax executes at class-definition time, before any instance - and therefore any per-instance `RetryConfig` - exists. Applying `retry()` as a plain function call inside `run()` uses `self.config.retry_config`, which is only available once an instance has been constructed. This keeps `extract()` implementations completely unaware that retry logic exists, and centralizes the retry policy in exactly one place (`run()`) rather than duplicating it across every concrete extractor.

### 4. Custom Exception Hierarchy 
Exceptions are classified as **transient** (retryable: network timeout, rate limit, 5xx) or **permanent** (non-retryable: 401, 404, malformed response). The retry decorator uses `isinstance()` checks against the hierarchy to check which branch of the hierarchy it belongs to. 

```
PipelineError
├── ExtractionError
│   ├── TransientExtractionError   → retried with exponential backoff
│   │   ├── NetworkError
│   │   ├── RateLimitError
│   │   └── ServerError
│   └── PermanentExtractionError   → re-raised immediately, no retry
│       ├── AuthenticationError
│       ├── MalformedResponseError
│       └── SourceNotFoundError
└── MaxRetriesExceededError        → raised when retry attempts are exhausted
```

### 5. Retry Decorator with Exponential Backoff 
A decorator factory wraps `extract()` with configurable retry logic driven by `RetryConfig`. 

```
retry(config)                    ← decorator factory: receives RetryConfig
    └── retry_decorator(func)    ← decorator: receives the function to wrap
            └── wrapper()        ← implements the retry loop

Decision logic:
    TransientExtractionError  →  wait (backoff_factor × attempt) → retry
    PermanentExtractionError  →  re-raise immediately, no retry
    Retries exhausted         →  raise MaxRetriesExceededError(
                                     operation, attempts, duration_seconds, last_exception
                                 ) chained from last exception via `from`
    Unexpected exception      →  re-raise as-is, not swallowed
```

Backoff formula: `wait = backoff_factor x attempt_number` (linear scaling). 
`MaxRetriesExceededError` carries structured metadata (operation name, attempt count, total elapsed duration, and the original exception) for observability and debugging. 

### 6. Generator-Based Streaming 
`extract()` returns a `Generator[Dict[str, Any], None, None]` rather than loading all records into memory. Records are yielded one at a time and consumed by the loader, keeping memory usage constant regardless of the dataset size. 

### 7. Pydantic Configuration 
All runtime configuration is validated at instantiation time using Pydantic `BaseModel`. Bad values (missing required fields, invalid types, constraint violations) raise `ValidationError` before any pipeline code runs (fail early, fail fast). Config objects are the single source of truth for extractor behavior. 

### 8. Structured Logging via `structlog` 
All log output is emitted as key-value pairs rather than unstructured strings. `configure_logging()` is called once at application startup and supports two output modes: 
- **Development:** colorized, human-readable console output via `ConsoleRenderer`
- **Production:** single-line JSON output per event via `JSONRenderer`, suitable for ingestion by Datadog, Splunk, CloudWatch, or equivalent 

Each extractor instance binds `extractor_class`, `source_name`, and `pipeline_run_id` to its logger at initialization. Every subsequent log line from that instance includes those fields automatically. 

---

## Installation 

**Prerequisites**: 
- Python 3.11+
- Git 

**Clone the repo and install in editable mode:**

```powershell
git clone https://github.com/taborerickson/python-etl-framework.git
cd python-etl-framework 

# install in editable mode with dev dependencies (pytest, ruff) 
pip install -e ".[dev]"

# verify the install 
pip show etl-framework 
```

---

### Usage 

> **In progress** - Usage examples will be added once `RestApiExtractor` and `ParquetLoader` are complete. The example below shows the intended and planned interface. 

**Configure logging at application startup:**

```python 
from etl_framework.logging.logger import configure_logging

# Development — colorized, human-readable output
configure_logging(level="INFO", environment="development")

# Production — single-line JSON output
configure_logging(level="INFO", environment="production")
```

**Use the framework in your own pipeline:**

```python 
from etl_framework.extractors.rest_api import RestApiExtractor
from etl_framework.config.models import APIConfig, RetryConfig

config = APIConfig(
    source_name="crm_contacts_api",
    pipeline_run_id="run_20260626_001",
    url="https://api.example.com/contacts",
    auth_token="your_token_here",
    page_size=100,
    retry_config=RetryConfig(max_retries=3, backoff_factor=2.0)
)

extractor = RestApiExtractor(config)
extractor.run()  # handles retry, logging, and error classification automatically
```

**Run the end-to-end example** *(once complete)*:

```powershell
python examples/api_to_parquet.py 
```

--- 

### Configuration 

Config models are Pydantic `BaseModel` subclasses defined in `etl_framework/config/models.py`. All fields are validated at instantiation. Missing required fields or invalid types raise `ValidationError` before any pipeline code runs. 

**Config class reference:** 

| Config Class | Inherits From | Key Fields |  
|---|---|---|
| `RetryConfig` | `BaseModel` | `max_retries` (default: 3), `backoff_factor` (default: 2.0), `retry_on` (default: `[TransientExtractionError]`) | 
| `ExtractorConfig` | `BaseModel` | `source_name` (required), `pipeline_run_id` (required), `retry_config` (default: `RetryConfig()`) |
| `APIConfig` | `ExtractorConfig` | `url` (required), `auth_token` (required), `page_size` (default: 100), `timeout_seconds` (default: 30) | 
| `CSVConfig` | `ExtractorConfig` | `file_path` (required), `delimiter` (default: `","`), `encoding` (default: `"utf-8"`) | 

**Example: `APIConfig` with custom retry behavior:**

```python 
from etl_framework.config.models import APIConfig, RetryConfig

config = APIConfig(
    source_name="crm_contacts_api",
    pipeline_run_id="run_20260630_001",
    url="https://api.example.com/contacts",
    auth_token="your_token_here",
    page_size=100,
    retry_config=RetryConfig(max_retries=5, backoff_factor=1.5)
)
```

**Example: using defaults (no explicit `RetryConfig` required):** 

```python 
config = APIConfig(
    source_name="crm_contacts_api",
    pipeline_run_id="run_20260630_001",
    url="https://api.example.com/contacts",
    auth_token="your_token_here"
)
# retry_config defaults to RetryConfig(max_retries=3, backoff_factor=2.0)
```

---

### Running Tests 

```powershell
# Run all tests 
pytest 

# Run with coverage report 
pytest --cov=etl_framework --cov-report=term-missing 

# Run a specific test file 
pytest tests/test_exceptions.py -v 
```

**Current test coverage:**

| Test File | What It Covers | Status |
|---|---|---|
| `tests/test_exceptions.py` | Exception hierarchy: inheritance, `isinstance()` checks, `MaxRetriesExceededError.__str__()` | COMPLETE |
| `tests/test_retry_decorator.py` | Retry logic: backoff, transient vs. permanent, exhaustion | Planned |
| `tests/test_base_extractor.py` | `BaseExtractor.run()`: success path, retry-then-success, permanent failure, ABC enforcement (`TypeError` on missing `extract()`) | Planned |
| `tests/test_api_extractor.py` | `RestApiExtractor`: extraction, pagination, exception translation | Planned |
| `tests/test_parquet_loader.py` | `ParquetLoader`: file output, schema validation | Planned |
| `tests/conftest.py` | Shared fixtures (incl. `configure_logging()` fixture for test-session setup) | Planned |

---

## Key Design Decisions & Trade-offs 

### `run()` uses `try/except/else`, not a bare `try/except`
The initial implementation of `BaseExtractor.run()` used a plain `try/except` with no `else` clause - extraction succeeded correctly, but the result was never returned and the success log was never written, since there was no code path after the `try/except` for the success case. This was caught in review before commit. The fix moves all success-path logic (duration calculation, record-count logging, `return result`) into an `else` clause, which only executes when `try` completes with no exception. This also prevents a subtler bug: if success-path code itself raised an exception, placing it inside `try` would cause it to be misclassified as an extraction failure by the `except` block.

### Retry applied functionally inside `run()`, not via `@retry` on `extract()`
The retry decorator needs a `RetryConfig` instance, which only exists once an `ExtractorConfig` has been constructed - after the class is already defined. `@` decorator syntax runs at class-definition time, before any instance-level config exists, so it cannot consume per-instance retry policy. `run()` instead calls `retry(self.config.retry_config)(self.extract)` as a plain function call at runtime, preserving both per-instance configurability and a clean separation between `extract()` (source logic) and retry orchestration (shared, base-class logic).

### Transient/permanent exception classification
Rather than retrying all exceptions or none, the framework distinguishes retryable from non-retryable failures at the class hierarchy level. This avoids wasting retries on errors that will never succeed (401, malformed JSON) while still recovering from transient conditions (network timeout, rate limit). The trade-off is that a developer adding a new exception must intentionally classify it (the hierarchy enforces this).

### Decorator factory pattern (`retry(config)`) over a plain decorator (`@retry`)
The retry decorator needs access to `RetryConfig`, which varies per extractor instance. A plain `@retry` with no arguments cannot accept configuration, and (per the point above) cannot be applied via `@` syntax at all in this framework's design. The factory pattern (`retry(config)` returns a decorator, which wraps the function) allows full configuration while keeping the underlying decorator mechanics reusable. The call chain is: `retry(config)` → `retry_decorator(func)` → `wrapper()`.

### `MaxRetriesExceededError` sits under `PipelineError`, not `ExtractionError`
The retry decorator is not specific to extractors. When loaders gain retry logic, the same `MaxRetriesExceededError` applies. Placing it under `ExtractionError` would incorrectly scope it and require a parallel class for loaders. `MaxRetriesExceededError` carries structured metadata (operation name, attempt count, total elapsed duration, last exception) chained via `raise ... from` to preserve the full causal chain in tracebacks.

### Pydantic over dataclasses for config
`@dataclass` provides structure but no validation. Pydantic validates field types and constraints at instantiation. A misconfigured `APIConfig` raises `ValidationError` before any network call is made. The trade-off is a heavier dependency, which is acceptable here because Pydantic is already widely used.

### Config passed into `__init__`, not `extract()`
Extractor configuration (URL, auth token, page size) is passed at instantiation, not at call time. This makes the extractor self-contained and allows `run()` to be called with no arguments. This is the intended and planned interface for Airflow task wrappers.

### `configure_logging()` is called by the application, not by the framework
`BaseExtractor` does not call `configure_logging()` internally. Calling it inside `BaseExtractor.__init__()` would reconfigure the global logging state every time an extractor is instantiated. A library should not make global configuration decisions on behalf of the application using it.

--- 

## Skills Demonstrated 

| Skill | Where |
|---|---|
| Python OOP: ABC, inheritance, Template Method pattern | `base/extractor.py`, all extractors |
| ABC enforcement (`TypeError` on missing abstract method) | `base/extractor.py` |
| `try/except/else` for correct success/failure separation | `base/extractor.py` |
| Custom exception hierarchy with `isinstance()` classification | `exceptions/pipeline_errors.py` |
| Decorators: decorator factories, functional (non-`@`) application | `decorators/retry.py`, `base/extractor.py` |
| Defensive handling of unknown/variable return types | `base/extractor.py` (`len()` guarded by `TypeError`) |
| Type hints throughout | All modules |
| Pydantic: config validation, nested models, `default_factory` | `config/models.py` |
| Structured logging: context binding, key-value output | `logging/logger.py`, `base/extractor.py` |
| Unit testing: pytest, mocking, fixtures | `tests/` |
| Package structure and tooling: `pyproject.toml`, editable install | `pyproject.toml` |
| `raise ... from e` exception chaining | `decorators/retry.py` |

<br> 

---

<br> 

<div align="center">

***Author: Tabor Erickson | [LinkedIn](https://www.linkedin.com/in/taborerickson) | [GitHub](https://github.com/taborerickson)***

</div>
