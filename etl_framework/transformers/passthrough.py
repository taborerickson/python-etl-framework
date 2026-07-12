# etl_framework/transformers/passthrough.py 
# PassThroughTransformer 

"""
PassThroughTransformer 

The minimal concrete transformer: yields every input record unchanged. 
Exists to make sure the extract -> transform -> load pipeline shape structurally 
complete even before any real transformation logic is needed. 
"""

from typing import Iterable, Iterator 

from etl_framework.transformers.base import BaseTransformer 

# PassThroughTransformer 
class PassThroughTransformer(BaseTransformer): 
    """Yields each input record unchanged.""" 

    def transform(self, records: Iterable[dict]) -> Iterator[dict]: 
        for record in records: 
            yield record 

