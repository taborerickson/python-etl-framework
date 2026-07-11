# etl_framework/base/extractor.py 
# BaseExtractor (ABC) 

"""
BaseExtractor (ABC) 

Defines the BaseExtractor ABC: the template that every concrete extractor 
(RestApiExtractor, CSVExtractor, ...) inherits from. Enforces a consistent 
generator-based run() lifecycle (logging, timing, retry) while leaving the 
source-specific extraction logic (extract()) to each subclass. 

Retry orchestration is not handled here. Each concrete extractor is responsible 
for wrapping its own retryable unit with the retry() decorator internally, then 
exposing a generator via extract(). This keeps run() a pure streaming/logging 
layer with no dependency on how a given source retries. 
"""

from abc import ABC, abstractmethod 
import time 

from etl_framework.config.models import ExtractorConfig
from etl_framework.logging.logger import get_logger 

# BaseExtractor (ABC) 
class BaseExtractor(ABC): 
    """
    Abstract base class for all data extractors. 

    Subclasses must implement extract() as a generator. run() is the template 
    method: it handles logging, timing, and record counting around extract(), 
    and should not be overridden by subclasses. Retry orchestration is the 
    responsibility of each subclass's extract() implementation, not run(). 
    """

    def __init__(self, config: ExtractorConfig) -> None:  
        self.config = config 
        self.logger = get_logger(__name__).bind(
            source_name=config.source_name, 
            pipeline_run_id=config.pipeline_run_id, 
        )

    def run(self): 
        """
        Executes the extraction lifecycle as a generator. 

        - Logs extraction_started before any iteration begins.
        - Iterates self.extract(), yielding each record through to the caller 
            as it arrives (no materialization of the full result set).  
        - On success: logs extraction_succeeded with duration and final 
            record count once iteration is exhausted. 
        - On failure during iteration: logs extraction_failed with duration 
            and how many records were yielded before the failure, then 
            re-raises the original exception. 

        run() is a generator. Calling it does not execute any of this 
        logic. It only begins running once you start iterating the result 
        (e.g., `for record in extractor.run(): ...`). 
        """

        start_time = time.time() 
        self.logger.info("extraction_started") 
        record_count = 0 

        try: 
            for record in self.extract(): 
                record_count += 1 
                yield record 
        except Exception: 
            duration = time.time() - start_time 
            self.logger.error(
                "extraction_failed", 
                duration_seconds=duration, 
                records_yielded_before_failure=record_count, 
            )
            raise 
        else: 
            duration = time.time() - start_time 
            self.logger.info(
                "extraction_succeeded", 
                duration_seconds=duration, 
                record_count=record_count, 
            )


    @abstractmethod 
    def extract(self): 
        """
        Yeild records from the source one at a time. 

        Must be implemented by every concrete extractor as a generator. Any retryable, 
        atomic unit of work should be implemented as a separate, eager helper method 
        that this method calls and then yields from. 
        Should raise TransientExtractionError (or a subclass) for retryable failures, 
        and PermanentExtractionError (or a subclass) for non-retryable ones. 
        """
 
