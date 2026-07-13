# etl_framework/transformers/base.py 
# BaseTransformer 

"""
BaseTransformer (ABC) 

Defines the transform() contract for the middle stage of the pipeline: 
generator in, generator out. A transformer takes an iterable of raw records 
and yields transformed records lazily, preserving the streaming property 
established by the extractor layer. 

If a transformer buffered its input into a list before transforming, it 
would silently reintroduce the memory problem generators were introduced to solve. 
The pipeline would only be 'streaming' at the extractor, defeating the point for 
any dataset too large to fit in memory. 
"""

from abc import ABC, abstractmethod 
from typing import Iterable, Iterator 

# BaseTransformer (ABC) 
class BaseTransformer(ABC): 
    """Abstract base class for all record transformers."""

    @abstractmethod
    def transform(self, records: Iterable[dict]) -> Iterator[dict]: 
        """
        Transform an iterable of records into another iterable of records. 

        Must be implemented as a generator (or otherwise return a lazy iterator). 
        Implementations should iterate `records` once, on demand, rather than 
        consuming it into a list up front. 
        """
        raise NotImplementedError 
