# logging/logger.py 

"""
Structured logging configuration for etl_framework. 

Configures structlog to emit key-value  structured log events rather 
than unstructued strings. Supports two output modes: 
    - development: human-readable, colorized console output 
    - production: single-line JSON output suitable for log aggregators

Usage: 
    Call configure_logging() once at application startup before instantiating
    any extractors. Do not call it from within library components. 

    from etl_framework.logging.logger import configure_logging, get_logger 

    configure_logging(level="INFO", environment="development") 
    logger = get_logger(__name__) 
    logger.info("pipeline started", pipeline_run_id="run_001") 

Each extractor instance binds extractor_class, source_name, and 
pipeline_run_id to its logger at initalization. Every subsequent log
event from that instance carries those fields automatically. 
"""

import logging
import sys

import structlog


# setup function 
def configure_logging(
        level: str = "INFO", 
        environment: str = "development"
) -> None: 
    """
    Configures structlog and the standard library logging module.
    
    Must be called once at application startup before any log events are 
    emitted. Calling it multiple times is safe but redundant; structlog
    will be reconfigured on each call. 

    Args: 
        level: Minimum log level to emit. Accepts standard level names:
            "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL". 
            Defaults to "INFO". Invalid values fall back to INFO.
        environment: Output mode selector. 
            "developtment": colorized, human-readable console output.
            Any other value: single-line JSON output for log aggregators.
            Defaults to "development". 

    Returns: 
        None 
    """
    # Configures standard library root logger 
    # structlog hands off to stdlib logging for actual output 
    logging.basicConfig(
        format="%(message)s", # structlog handles all formatting 
        stream=sys.stdout, 
        level=getattr(logging, level.upper(), logging.INFO),
    )
    # shared processor chain (run in both development and production) 
    shared_processors = [
        structlog.stdlib.add_log_level, # adds "level" field 
        structlog.stdlib.add_logger_name, # adds "logger" field 
        structlog.processors.TimeStamper(fmt="iso"), # adds "timestamp" field
        structlog.processors.StackInfoRenderer(), # formats stack_info if present
    ]
    # Environment-specific renderer appended to the shared chain
    if environment == "production": 
        processors = shared_processors + [
            structlog.processors.ExceptionRenderer(), # renders exc_info as structured fields 
            structlog.processors.JSONRenderer(), # serializes event dict to JSON strings 
        ]
    else: 
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(), # colorized, aligned human-readable output
        ]
    # registering with structlog globally 
    structlog.configure(
        processors=processors, 
        wrapper_class=structlog.stdlib.BoundLogger, # standard library-compatible wrapper
        context_class=dict, # internal storage for bound context 
        logger_factory=structlog.stdlib.LoggerFactory(), 
        cache_logger_on_first_use=True, # performance optimization (frozen and cached configured processor chain) 
    )

# Factory function 
def get_logger(
        name: str | None = None
) -> structlog.stdlib.BoundLogger: 
    """
    Returns a structlog BoundLogger instance. 

    A thin wrapper around structlog.get_logger() that provides a single 
    import path for all logging within etl_framework and exposes the 
    return type for IDE autocompletion. 

    Args: 
        name: Optional logger name, typically __name__ of the calling module. 
            If None, structlog infers the name from the call site. 

    Returns 
        A BoundLogger instance with .info(), .error(), .debug(), 
        .warning(), and .bind() available. 

    Example: 
        logger = get_logger(__name__) 
        logger = logger.bind(source_name="crm_api", pipeline_run_id="run_001")
        logger.info("extraction started", page=1) 
    """
    return structlog.get_logger(name) 
