# etl_framework/exceptions/pipeline_errors.py 
# Custom Exception Hierarchy 

"""
Exception Hierarchy Design 

PipelineError
|-- ExtractionError 
|    |-- TransientExtractionError   # retry decorator retries these errors 
|    |    |-- RateLimitError
|    |    |-- NetworkError
|    |-- PermanentExtractionError   # retry decorator re-raises immediately
|    |    |-- AuthenticationError
|    |    |-- MalformedResponseError
|    |    |-- SourceNotFoundError
|
|-- Load Error
|    |-- TransientLoadError
|    |-- PermanentLoadError
|    |    |-- SchemaError
|
|-- MaxRetriesExceededError         # wraps last exception + attempt count + duration

"""






