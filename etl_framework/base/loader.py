# etl_framework/base/loader.py
# BaseLoader (ABC) 

"""
BaseLoader (ABC) 

Mirrors BaseExtractor's template method pattern for the load side of the 
pipeline. Concrete loaders (e.g. ParquetLoader) implement load() to consume 
an iterable of records and persist them somewhere. run() wraps load() with 
the same logging/timing lifecycle used on the extractor side, so both 
halves of the pipeline produce consistent, structured logs. 

Unlike BaseExtractor.run() (a generator that yields records through to a 
caller), BaseLoader.run() is a terminal operation. A loader is the end of 
the pipeline: it consumes the incoming iterable fully (in batches, not all 
at once) and returns a count of what it wrote, rather than yielding 
anything onward. 
"""

import time
from abc import ABC, abstractmethod
from typing import Iterable

from etl_framework.logging.logger import get_logger


# BaseLoader(ABC) 
class BaseLoader(ABC): 
    """Abstract base class for all data loaders."""

    def __init__(self) -> None: 
        self.logger = get_logger(__name__) 

    def run(self, records: Iterable[dict]) -> int: 
        """
        Executes the load lifecycle. 

        Logs load_started before consuming anything, delegates to 
        self.load(records), and logs load_succeeded / load_failed around it
        (matching the extractor side's extraction_started / extraction_succeeded / 
        extraction_failed contract). Returns the total number of records written. 
        """
        start_time = time.time() 
        self.logger.info('load_started') 

        try: 
            record_count = self.load(records) 
        except Exception: 
            duration = time.time() - start_time 
            self.logger.error('load_failed', duration_seconds=duration) 
            raise 
        else: 
            duration = time.time() - start_time 
            self.logger.info(
                'load_succeeded', 
                duration_seconds=duration, 
                record_count=record_count, 
            )
            return record_count 
        
    @abstractmethod 
    def load(self, records: Iterable[dict]) -> int: 
        """
        Consume an iterable of records and persist them. 

        Must be implemented by evrey concrete loader. Should consume 
        `records` lazily/in batches rather than materializing the full 
        iterable into memory at once. Must return the total number of 
        records written. 
        """
        raise NotImplementedError

