# Python ETL Framework - Modular Ingestion Library

---

The focus of this project is creating a reusable, installable Python ETL framework built for production-grade data engineering workflows. Rather than a collection of scripts, it is a structured library with abstract base classes, concrete implementations, configuration management, retry logic, structured logging, and full unit test coverage. 

This framework serves as the ingestion layer for the Sales-Intelligence-Pipeline project. 

---

## Table of Contents 
- Problem Statement 
- Architecture 
- Project Structure 
- Key Design Patterns 
- Installation 
- Usage 
- Configuration 
- Running Tests 
- Key Design Decisions & Trade-offs 
- Demonstrated Skills 

---

### Problem Statement 


**Goals:**
- Abstract common ingestion patterns (REST API, file I/O) behind a consistent interface 
- Handle failures gracefully with configurable retry logic and exponential backoff 
- Emit structured, machine-readable logs suitable for observability tooling 
- Be importable as a reusable package 

---

### Architecture 

> Placeholder: Insert structured architecture diagram here 

---

### Project Structure 

```text
python-etl-framework/
│
├── etl_framework/                  # Main installable package 
│   ├── __init__.py
│   ├── base/
│   │   ├── __init__.py
│   │   ├── extractor.py            # BaseExtractor ABC
│   │   ├── transformer.py          # BaseTransformer ABC
│   │   └── loader.py               # BaseLoader ABC 
│   ├── extractors/
│   │   ├── __init__.py
│   │   ├── api_extractor.py        # REST API with pagination + retry
│   │   └── file_extractor.py       # CSV / Parquet file reader 
│   ├── transformers/
│   │   ├── __init__.py
│   │   └── crm_transformer.py      # CRM record cleaning + type casting
│   ├── loaders/
│   │   ├── __init__.py
│   │   └── parquet_loader.py       # Local Parquet writer 
│   ├── decorators/
│   │   ├── __init__.py
│   │   ├── retry.py                # Exponential backoff retry decorator
│   │   ├── timer.py                # Execution timer decorator 
│   │   └── logging_decorator.py    # Structured log entry/exit decorator
│   ├── config/
│   │   ├── __init__.py
│   │   └── models.py               # Pydantic config + record schemas 
│   ├── exceptions/
│   │   ├── __init__.py
│   │   └── pipeline_errors.py      # Custom exception hierarchy 
│   ├── logging/
│   │   ├── __init__.py
│   │   └── logger.py               # structlog setup 
│   └── runner.py                   # PipelineRunner orchestration class 
│
├── tests/
│   ├── __init__.py
│   ├── conftest.py                 # Shared fixtures
│   ├── test_api_extractor.py
│   ├── test_crm_transformer.py
│   ├── test_parquet_loader.py
│   ├── test_retry_decorator.py
│   └── test_pipeline_runner.py
│
├── examples/
│   └── run_crm_ingestion.py        # End-to-end usage example 
│
├── pyproject.toml                  # Package config + dependencies
├── .gitignore
└── README.md
```

---

### Key Design Patterns 

1. **Abstract Base Classes (ABC)** <br>
All extractors, transformers, and loaders inherit from an abstract base class that enforces a consistent interface. Adding a new source means subclassing `BaseExtractor` and implementing `.extract()`. Noting else changes downstream. 

2. **Retry Decorators with Exponential Backoff** <br>

3. **Generator-Based Streaming** 

4. **Pydantic Configuration and Record Schemas**

5. **Custom Exception Hierarchy**

6. **Structured Logging via `structlog`**

---

### Installation 

**Prerequisites**: 

```powershell

```

---

### Usage 

**Run the Example Pipeline** <br>

```powershell

```

**Use the Framework in Your Own Pipeline**

```python

```

--- 

### Configuration 

---

### Running Tests 

```powershell

```

---

### Key Design Decisions & Trade-offs 

--- 

<center> 

### Skills Demonstrated 

| Skill | Where | 
|---|---|
| Python OOP: ABC, inheritance, composition | --- | 
| Decorators: retry, timer, logging | --- | 
| Generators: memory-efficient streaming | --- | 
| Type hints | --- | 
| Pydantic: config validation + record schemas | --- | 
| Custom exception hierarchy | --- | 
| Context managers | --- | 
| Unit testing: pytest, mocking, fixtures | --- | 
| Structured logging: structlog | --- | 
| Package structure: pyproject.toml | --- | 

</center> 

<br> 

---

<br> 

<center> 

***Author: Tabor Erickson | LinkedIn | GitHub***

</center> 
