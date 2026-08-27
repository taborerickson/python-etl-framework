# Python ETL Framework - Modular Ingestion Library

The focus of this project is creating a reusable, installable Python ETL framework built for production-grade data engineering workflows. 
Rather than a collection of scripts, this is a structured library with abstract base classes, concrete implementations, Pydantic configuration management, retry logic with exponential backoff, structured logging, and full unit test coverage. 

This framework serves as the ingestion layer for the Sales-Intelligence-Pipeline project - a medallion-architecture ETL pipeline with dbt, Airflow, and a RAG/AI layer. The Sales Intelligence Pipeline project's Bronze ingestion layer imports directly from this library. 

---

## Table of Contents 
- [Problem Statement](#problem-statement)
- [Architecture](#architecture) 
- [Project Structure](#project-structure) 
- [Key Design Patterns](#key-design-patterns) 
- [Streaming & Memory Model](#streaming--memory-model)
- [Public API](#public-api) 
- [Installation](#installation)  
- [Usage](#usage)  
- [Configuration](#configuration)  
- [Testing Strategy](#testing-strategy) 
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

**How this framework relates to the spine project:**

```
python-etl-framework -> imported by sales-intelligence-pipeline 

BaseExtractor (ABC) -> Bronze layer ingestion tasks 
RestApiExtractor -> CRM API ingestion 
RetryConfig, APIConfig -> Config-driven Airflow task runs 
configure_logging() -> Structured observability across all layers 
```

**End-to-end call flow:**

```
Airflow Task
    └── configure_logging(level="INFO", environment="production")
    └── extractor = RestApiExtractor(config)
            └── BaseExtractor.__init__()     # binds logger, stores config
    └── for record in extractor.run()        # template method, Generator 
            └── extract()                    # generator; yields per page/record
                    └── _fetch_page()         # wrapped by retry(config.retry_config)
                            ├── TransientError  →  backoff → retry → MaxRetriesExceededError
                            ├── PermanentError  →  re-raise immediately
                            └── success         →  returns a batch
                    └── yield from batch        →  records stream to caller one at a time
                    └── (loops to next page until a short/empty page signals exhaustion)
    └── transformer.transform(records)        # generator in, generator out
    └── loader.run(transformed_records)       # ParquetLoader: batched writes, no full materialization
```

Retry is resolved fully *before* any record from a page is yielded. A mid-retry failure can never surface as a duplicate or partial record downstream. 

---

## Project Structure 

```text
python-etl-framework/
│
├── etl_framework/                  # Main installable package 
│   ├── __init__.py                 # Public API - re-exports every concrete class 
│   ├── base/
│   │   ├── __init__.py
│   │   ├── extractor.py            # BaseExtractor ABC 
│   │   └── loader.py               # BaseLoader ABC  
│   ├── extractors/
│   │   ├── __init__.py
│   │   ├── rest_api.py             # RestApiExtractor (pagination, Retry-After) 
│   │   └── csv.py                  # CSVExtractor 
│   ├── loaders/
│   │   ├── __init__.py
│   │   └── parquet_loader.py       # ParquetLoader 
│   ├── transformers/
│   │   ├── __init__.py
│   │   ├── base.py                 # BaseTransformer ABC 
│   │   └── passthrough.py          # PassThroughTransformer 
│   ├── decorators/
│   │   ├── __init__.py
│   │   └── retry.py                # Retry decorator factor
│   ├── config/
│   │   ├── __init__.py
│   │   └── models.py               # Pydantic config models 
│   ├── exceptions/
│   │   ├── __init__.py
│   │   └── pipeline_errors.py      # Custom exception hierarchy
│   └── logging/
│       ├── __init__.py
│       └── logger.py               # structlog setup  
│
├── tests/                          # 10 files, 91 tests, 99% coverage 
│   ├── __init__.py
│   ├── conftest.py                 # Shared factory fixtures + payload-shape fixtures
│   ├── test_exceptions.py          # 22 tests
│   ├── test_config_models.py       # 11 tests 
│   ├── test_retry.py               # 8 tests 
│   ├── test_rest_api_extractor.py  # 22 tests 
│   ├── test_csv_extractor.py       # 8 tests 
│   ├── test_base_extractor.py      # 5 tests 
│   ├── test_parquet_loader.py      # 5 tests 
│   ├── test_transformers.py        # 4 tests 
│   └── test_logging_config.py      # 6 tests 
│
├── examples/
│   └── api_to_parquet.py           # End-to-end usage example - runnable
│
├── pyproject.toml                  # Package config + dependencies 
├── .gitignore
└── README.md
```

---

## Key Design Patterns 

### 1. Abstract Base Classes (ABC) 
All extractors, transformers, and loaders inherit from an abstract base class that enforces a consistent interface. 
`BaseExtractor` declares `extract()` as an abstract method via `abc.ABC` + `@abstractmethod`.  Adding a new source means subclassing `BaseExtractor` and implementing `.extract()`. Nothing else changes downstream. 

Enforcement happens at **instantiation time**, not when the missing method is first called: attempting to instantiate `BaseExtractor` directly, or any subclass that omits `extract()`, raises `TypeError` immediately. This was verified with a smoke test confirming both `BaseExtractor(config)` and an incomplete subclass correctly fail fast.

### 2. Template Method Pattern 
`BaseExtractor.run()` is a concrete method that handles all shared orchestration: logging extraction start, iterating `extract()` and yielding each record through, tracking a running record count, and logging success (with final count) or failure (with the count yielded before the error) with duration, timing, and streaming. The orchestration logic is written exactly once. 

Retry orchestration is intentionally **not** part of `run()`. It is each subclass's responsibility, applied internally around whatever unit of work that source's `extract()` depends on. This keeps `run()` a pure streaming/logging layer with no assumptions about how, or whether, a given source needs to retry. 

`RestApiExtractor` is the framework's first concrete implementation of this pattern. It implements `extract()` as a generator (delegating its retryable work to a private `_fetch_page()` helper), and the full loggin/streaming orchestration in `run()` works against it without modification. 

### 3. Functional Decorator Application (not `@` syntax)
The retry decorator is applied **inside each concrete extractor's `extract()`, at call time**, wrapping a private, eager helper method rather than the generator itself, and rather than using `@retry(...)` syntax above a method definition: 

```python
# RestApiExtractor.extract() 
protected_fetch = retry(self.config.retry_config)(self._fetch_page) 
page = protected_fetch() 
yield from page 
```

**Why functional application, not `@` syntax:** `@` decorator syntax executes at class-definition time, before any instance - and therefore any per-instance `RetryConfig` - exists. Applying `retry()` as a plain function call inside `run()` uses `self.config.retry_config`, which is only available once an instance has been constructed. 

**Why the retry wraps `_fetch_page()` and not `extract()` itself:** a generator function's body does not execute when called, only when iterated. If `retry()` wrapped `extract` directly, the wrapped call would just construct a generator object and return successfully every time, since none of `extract()`'s code (including the HTTP call) has run yet - the `try/except` inside the retry decorator would be protecting nothing. Splitting the retryable work into `_fetch_page()` - an eager method that either fully succeeds or fully fails, with no partial state - means the retry decorator wraps something that actually executes at the moment it's called, exactly as it's designed to. `extract()` itself stays a thin generator that calls the retry-wrapped fetch and then streams its result with `yield from`.

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

`RestApiExtractor` is the first component to actually raise these exceptions from a real failure condition (an HTTP call), rather than a synthetic test case. 

### 5. Retry Decorator with Exponential Backoff 
A decorator factory wraps a source's fetch unit (e.g. `RestApiExtractor._fetch_page()`) with configurable retry logic driven by `RetryConfig`. 

```
retry(config)                    ← decorator factory: receives RetryConfig
    └── retry_decorator(func)    ← decorator: receives the function to wrap
            └── wrapper()        ← implements the retry loop

Decision logic:
    TransientExtractionError  →  wait (backoff_factor ** attempt) → retry
    PermanentExtractionError  →  re-raise immediately, no retry
    Retries exhausted         →  raise MaxRetriesExceededError(
                                     operation, attempts, duration_seconds, last_exception
                                 ) chained from last exception via `from`
    Unexpected exception      →  re-raise as-is, not swallowed
```

Backoff formula: `wait = backoff_factor ** attempt_number` (exponential scaling). 
`MaxRetriesExceededError` carries structured metadata (operation name, attempt count, total elapsed duration, and the original exception) for observability and debugging. 

### 6. `Generator-Based Streaming Contract 
`extract()` yields records one at a time via a Python generator, rather than returning a `list[dict]`. This was a deliberate reversal of an earlier design decision. The original rationale ("single-page extraction has a bounded, known-size payload, so a list is fine for now") locked every future extractor (CSV, database cursor, paginated API) into either violating the contract or requiring a rewrite later. A reusable ingestion framework intended for data engineering workflows should default to the memory-safe contract, and let a caller materialize a list at the call site if they sprcifically want on (`list(extractor.run())`), not the other way around. 

### 7. HTTP Status-to-Exception Mapping
`RestApiExtractor.extract()` translates HTTP-layer outcomes into the exception hierarchy above:

| HTTP Condition | Exception Raised | Classification |
|---|---|---|
| Connection failure / timeout | `NetworkError` | Transient |
| `5xx` | `ServerError` | Transient |
| `429` | `RateLimitError` | Transient |
| `401` / `403` | `AuthenticationError` | Permanent |
| `404` | `SourceNotFoundError` | Permanent |
| Unparseable response body | `MalformedResponseError` | Permanent |
| `200` with an error indicator in the body | `MalformedResponseError` | Permanent |

The last row is an edge case: an HTTP `200` only confirms transport-level success, not application-level success. Some APIs return `200` with an error payload in the body, so `extract()` checks the parsed body for an error indicator before returning data, even on a successful status code.

### 8. Pydantic Configuration 
All runtime configuration is validated at instantiation time using Pydantic `BaseModel`. Bad values (missing required fields, invalid types, constraint violations) raise `ValidationError` before any pipeline code runs (fail early, fail fast). Config objects are the single source of truth for extractor behavior. 

### 9. Structured Logging via `structlog` 
All log output is emitted as key-value pairs rather than unstructured strings. `configure_logging()` is called once at application startup and supports two output modes: 
- **Development:** colorized, human-readable console output via `ConsoleRenderer`
- **Production:** single-line JSON output per event via `JSONRenderer`, suitable for ingestion by Datadog, Splunk, CloudWatch, or equivalent 

Each extractor instance binds `extractor_class`, `source_name`, and `pipeline_run_id` to its logger at initialization. Every subsequent log line from that instance includes those fields automatically. 

---

## Streaming & Memory Model 

A reusable ingestion framework can't assume its source data fits in memory. `extract()`
is a generator: it yields records one at a time rather than returning a materialized
`list[dict]`. This means the same code path handles a 5-row CSV and a 5-million-row API
export at constant memory. Nothing downstream ever needs more than the current record
in memory at once. A caller who *wants* a materialized list can still opt in explicitly
(`list(extractor.run())`); the framework doesn't force that cost on every caller by default.

### The generator/retry split

Converting `extract()` to a generator broke naive retry wrapping: calling a generator
function doesn't execute its body. It only constructs a generator object, instantly and
successfully, every time. The real work (the HTTP call, the file read) happens later,
during iteration, outside whatever `try/except` a decorator applied to the call itself.

The fix splits each extractor into two parts:
- **An eager, atomic unit of work** (`RestApiExtractor._fetch_page()`) that either fully
  succeeds (returns a complete batch) or fully fails (raises) the moment it's called.
  This is what `retry()` wraps.
- **A thin generator** (`extract()`) that calls the retry-protected unit and then
  `yield from`s its result.

```python
def _fetch_page(self, page: int) -> list[dict]:
    """Eager. Either fully succeeds (returns a batch) or fully fails (raises).
    This is the method retry() wraps."""
    response = self.session.get(...)
    ...
    return payload

def extract(self):
    """Generator. Thin. Delegates the retryable work, then streams the result."""
    protected_fetch = retry(self.config.retry_config)(self._fetch_page)
    page_number = 1
    while True:
        page_records = protected_fetch(page_number)   # retry fully resolves HERE
        if not page_records:
            break
        yield from page_records                        # THEN streaming begins
        if len(page_records) < self.config.page_size:
            break
        page_number += 1
```

This guarantees retry is fully resolved *before* any record from a page reaches the
caller. A mid-retry failure can never surface as a duplicate or partial record
downstream. Callers never need to reason about retry at all; it's fully contained
inside the extractor.

### The same discipline, applied deliberately everywhere else

- **`CSVExtractor`** carries **no retry logic**, on purpose. A missing file or malformed
  row is a permanent failure. Retrying doesn't fix corrupt bytes or a wrong path.
  `tests/test_csv_extractor.py` includes an explicit regression test locking this decision in, so a future "helpful" addition of retry logic here fails a test rather than silently landing.
- **`BaseTransformer`/`PassThroughTransformer`** are generator-in, generator-out. The
  same non-materialization contract as the extractor layer, with no retry concern since
  pure transformation has nothing external to fail transiently.
- **`ParquetLoader`** can't be a generator (a loader is the terminal end of the
  pipeline), but preserves the same memory discipline via batching: it writes fixed-size
  batches (default 500 records) to disk via `pyarrow.parquet.ParquetWriter` as they
  arrive, rather than materializing the full input first. `tests/test_parquet_loader.py`
  proves this with a custom iterator that raises if `len()` is ever called on it. The
  test only passes because the loader genuinely never materializes its input.

---

## Public API 

Everything below is importable directly from the top-level `etl_framework` package:

```python
from etl_framework import (
    BaseExtractor, BaseLoader,             # ABCs
    RestApiExtractor, CSVExtractor,        # Concrete extractors
    ParquetLoader,                         # Concrete loader
    BaseTransformer, PassThroughTransformer,  # Transformer layer
    RetryConfig, ExtractorConfig, APIConfig, CSVConfig,  # Config models
    retry,                                 # Retry decorator factory
    configure_logging, get_logger,         # Structured logging
)
```

**The exception hierarchy requires a submodule import.** Unlike the classes above, exception classes are not re-exported from the top-level package:

```python
from etl_framework.exceptions import (
    PipelineError, RateLimitError, MalformedFileError, MaxRetriesExceededError, ...
)
```

This is intentional, not an oversight. Most callers construct extractors, loaders, and configs via the top-level import, and only need specific exception classes when writing `except` clauses for particular failure types. This is a less frequent, more targeted need that doesn't justify inflating the top-level namespace with every leaf exception class.

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

The example below wires `RestApiExtractor`, `PassThroughTransformer`, and `ParquetLoader` together into a runnable extract-transform-load pipeline. The full script is available at `examples/api_to_parquet.py`.

**Configure logging at application startup:**

```python 
from etl_framework.logging.logger import configure_logging

# Development - colorized, human-readable output
configure_logging(level="INFO", environment="development")

# Production - single-line JSON output
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
    timeout_seconds=30, 
    retry_config=RetryConfig(max_retries=3, backoff_factor=2.0)
)

extractor = RestApiExtractor(config)

# extractor.run() is a generator - handles retry, logging, and error
# classification automatically, and yields records one at a time
for record in extractor.run():
    ...  # hand each record to a transformer/loader as it arrives

# Or, if you specifically want a materialized list (e.g. for a quick
# script or a small, known-bounded source), the caller can opt in:
records = list(extractor.run())
```

**Run the end-to-end example:**

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
| `ExtractorConfig` | `BaseModel` | `source_name` (required), `pipeline_run_id` (auto-generated via `default_factory` - timestamp + short uuid4 suffix; override optional), `retry_config` (default: `RetryConfig()`) |
| `APIConfig` | `ExtractorConfig` | `url` (required), `auth_token` (required), `page_size` (default: 100), `timeout_seconds` (default: 30) | 
| `CSVConfig` | `ExtractorConfig` | `file_path` (required), `delimiter` (default: `","`), `encoding` (default: `"utf-8"`) | 

> **Note:** `timeout_seconds` is typed as `int` by design - sub-second precision is not needed for production timeout values. When testing timeout behavior, force the failure via mocking (`side_effect=requests.exceptions.Timeout(...)`) rather than passing a fractional-second value.

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

## Testing Strategy

**Running the suite:**

```powershell
# Run all tests
pytest

# Run with coverage report
pytest --cov=etl_framework --cov-report=term-missing

# Run a specific test file
pytest tests/test_retry.py -v
```

**Result: 91 tests, all passing, 99% statement coverage (306/308 lines).** The two uncovered lines are the `raise NotImplementedError` bodies of `BaseLoader.load()` and `BaseTransformer.transform()`. These are unreachable by design, since Python's `abstractmethod` machinery prevents instantiating a subclass that doesn't override them.

Every HTTP interaction is mocked via `responses` (zero real network calls in the suite). Every retry/backoff scenario, including a 60-second backoff cap and a 17-second `Retry-After` wait, is mocked via `unittest.mock.patch` on `time.sleep`, so the full suite runs in well under half a second despite testing behavior that would otherwise take tens of real seconds.

**Test files:**

| Test File | Tests | What It Verifies |
|---|---|---|
| `test_exceptions.py` | 22 | Exception hierarchy, message pass-through, `MaxRetriesExceededError` formatting, `__cause__` chaining |
| `test_config_models.py` | 11 | Fail-fast validation, defaults, `retry_on` mutable-default regression guard, `pipeline_run_id` auto-generation |
| `test_retry.py` | 8 | Retry limits, transient/permanent branching, full exponential backoff sequence, 60s cap, `Retry-After` precedence |
| `test_rest_api_extractor.py` | 22 | HTTP status mapping, all payload shapes, retry recovery, pagination, `Retry-After` timing |
| `test_csv_extractor.py` | 8 | CSV parsing/errors, explicit "no retry" regression guard |
| `test_base_extractor.py` | 5 | Structured logging correctness, partial-iteration edge cases |
| `test_parquet_loader.py` | 5 | Batching (spy on `_write_batch`), non-materialization, error modes |
| `test_transformers.py` | 4 | Generator behavior and explicit laziness proof |
| `test_logging_config.py` | 6 | `configure_logging()` across level/environment combinations |
| `conftest.py` | N/A | Shared fixtures used across the suite |

---

## Key Design Decisions & Trade-offs 

### `run()` uses `try/except/else`, not a bare `try/except`
The initial implementation of `BaseExtractor.run()` used a plain `try/except` with no `else` clause - extraction succeeded correctly, but the success log was never written, since there was no code path after the `try/except` for the success case. This was caught in review before commit. The fix moves all success-path logic (duration calculation, record-count logging, `return result`) into an `else` clause, which only executes when `try` completes with no exception. This also prevents a subtler bug: if success-path code itself raised an exception, placing it inside `try` would cause it to be misclassified as an extraction failure by the `except` block.

This shape carried forward unchanged when `run()` became a generator. The `try` now wraps a `for record in self.extract(): yield record` loop instead of a single call, but the success/failure separation logic is identical: `else` still only runs once the loop is fully exhausted with no exception, and `except` still logs and re-raises on any failure mid-iteration. 

### Retry applied functionally inside `extract()`, wrapping `_fetch_page()` - not via `@retry` on `extract()`, and not inside `run()`
The retry decorator needs a `RetryConfig` instance, which only exists once an `ExtractorConfig` has been constructed - after the class is already defined. `@` decorator syntax runs at class-definition time, before any instance-level config exists, so it cannot consume per-instance retry policy. This part of the original design still holds. 

What changed: retry orchestration was originally applied inside `BaseExtractor.run()`, wrapping `self.extract` directly (`retry(self.config.retry_config)(self.extract)`). Once `extract()` became a generator (see "Generator-Based Streaming Contract" above), this broke silently - calling a generator function doesn't run its body, so the retry wrapper's `try/except` never observed the HTTP call that happens later, during iteration. 

The fix moves retry down into each concrete extractor, wrapping only the specific atomic, eager unit of work that extractor depends on - for `RestApiExtractor`, that's `_fetch_page()`, not `extract()` itself. `run()` no longer knows or cares about retry at all; it's a pure streaming/logging layer. This is arguably a cleaner separation of concerns than the original design: retry policy now lives directly next to the operation it protects, and a future extractor with a different atomic unit of work (e.g. a paginated fetch, or a single database cursor batch) applies retry to *its* atomic unit without `BaseExtractor` needing to know anything about it.

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

### No intermediate `HttpExtractor` ABC between `BaseExtractor` and `RestApiExtractor`
An intermediate ABC for HTTP-specific concerns (session setup, default headers, auth injection) was considered, since a future `GraphQLExtractor` or `WebhookExtractor` could theoretically share that logic. With exactly one concrete HTTP-based extractor currently in the framework, this was deferred. Adding this now would add a layer of indirection with no current code-reuse benefit. If a second HTTP-based extractor is added and real duplication emerges, extracting a shared `HttpExtractor` at that point is a low-cost, mechanical refactor informed by two real implementations rather than a guess.

### `requests.Session()` created once in `__init__`, not per-call in `extract()`
`RestApiExtractor` creates a single `requests.Session()` at instantiation and reuses it across all calls to `extract()`. A `Session` reuses the underlying TCP connection instead of renegotiating it on every request, and allows auth headers to be set once rather than passed into every individual HTTP call. Creating a new session inside `extract()` would silently defeat this benefit.

### 200-with-error-body treated as `MalformedResponseError`
Some APIs return HTTP `200` with an error condition described inside the JSON body rather than via the status code. `extract()` checks the parsed response body for an error indicator before returning data, even on a `200` status, and raises `MalformedResponseError` if found. This is classified as permanent (not retried) since the request itself was well-formed and successfully transported - retrying an identical request would produce an identical application-level error.

### `Retry-After` takes precedence over calculated backoff when present 
`RateLimitError` carries an optional `retry_after` field, populated from the `Retry-After` HTTP header when a 429 response includes one. The retry decorator checks for this attribute (`getattr(e, "retry_after", None)`) before falling back to its own exponential formula. If the server tells you exactly how long to wait, trust that over a guess. APIs that don't return the header (or exceptions with no `retry_after` at all) fall back to the standard `backoff_factor ** attempt` calculation, capped at 60 seconds. Both paths are covered by `test_retry.py` and `test_rest_api_extractor.py`.

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
| REST API integration: HTTP methods, status codes, headers, sessions | `extractors/rest_api.py` |
| HTTP failure classification (network, 4xx, 5xx) mapped to a custom exception hierarchy | `extractors/rest_api.py` |
| Defensive API integration: handling 200-with-error-body and unknown response shapes | `extractors/rest_api.py` |
| Type hints throughout | All modules |
| Pydantic: config validation, nested models, `default_factory` | `config/models.py` |
| Structured logging: context binding, key-value output | `logging/logger.py`, `base/extractor.py` |
| Unit testing: pytest, mocking (`unittest.mock.patch`), fixtures | `tests/` |
| Package structure and tooling: `pyproject.toml`, editable install | `pyproject.toml` |
| `raise ... from e` exception chaining | `decorators/retry.py`, `extractors/rest_api.py` |

<br> 

---

<br> 

<div align="center">

***Author: Tabor Erickson | [LinkedIn](https://www.linkedin.com/in/taborerickson) | [GitHub](https://github.com/taborerickson)***

</div>
