# etl_framework/base/extractor.py 
# BaseExtractor (ABC) 

"""
BaseExtractor (ABC) 

Defines the BaseExtractor ABC: the template that every concrete extractor 
(RestApiExtractor, CSVExtractor, ...) inherits from. Enforces a consistent 
run() lifecycle (logging, timing, retry) while leaving the source-specific 
extraction logic (extract()) to each subclass. 
"""

from abc import ABC, abstractmethod 
import time 

from etl_framework.config.models import ExtractorConfig
from etl_framework.logging.logger import get_logger 
from etl_framework.decorators.retry import retry 

# BaseExtractor (ABC) 
class BaseExtractor(ABC): 
    """
    Abstract base class for all data extractors. 

    Subclasses must implement extract(). run() is the template method: 
    it handles logging, timing, and retry orchestration around extract(), 
    and should not be overridden by subclasses. 
    """

    def __init__(self, config: ExtractorConfig) -> None:  
        self.config = config 
        self.logger = get_logger(__name__).bind(
            source_name=config.source_name, 
            pipeline_run_id=config.pipeline_run_id, 
        )

    def run(self): 
        """
        Executes the extraction lifecycle: 
        - Logs extraction_started with a captured start timestamp. 
        - Calls self.extract(), wrapped with retry logic built from 
            self.config.retry_config. 
        - On success: logs extraction_succeeded with duration and a
            best-effort record count, then returns the extracted data. 
        - On failure that survives retries: logs extraction_failed with 
            duration, then re-raises the original exception. 
        """

        start_time = time.time() 
        self.logger.info("extraction_started") 

        try: 
            protected_extract = retry(self.config.retry_config)(self.extract) 
            result = protected_extract() 
        except Exception: 
            duration = time.time() - start_time 
            self.logger.error("extraction_failed", duration_seconds=duration) 
            raise
        else: 
            duration = time.time() - start_time 
            try: 
                record_count = len(result) 
            except TypeError: 
                record_count = None 
            self.logger.info(
                "extraction_succeeded", 
                duration_seconds=duration, 
                record_count=record_count,
            ) 
            return result 

    @abstractmethod 
    def extract(self): 
        """
        Pull data from the source and return it. 

        Must be implemented by every concrete extractor. Should raise 
        TransientExtractionError (or a subclass) for retryable failures, 
        and PermanentExtractionError (or a subclass) for non-retryable ones. 
        """
 
