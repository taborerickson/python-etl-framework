# etl_framework/loaders/parquet_loader.py 

"""
ParquetLoader 

Concrete loader that batch-writes an iterable of dict records to a Parquet 
file using pyarrow. Writes in fixed-size batches rather than materializing 
the full input generator into memory at once. The loader-side equivalent 
of the streaming discipline established by the extractor generator. 

Uses pyarrow.parquet.ParquetWriter to append batches incrementally to a 
single output file, rather than building one giant pyarrow.Table from the 
entire dataset up front (which would defeat the purpose of batching). 

Schema handling: pyarrow needs a schema before it can open a ParquetWriter, 
but this loader doesn't know the schema until it has seen at least one batch 
of real records. So the writer is created lazily, from the first batch's 
inferred schema, rather than up fron in __init__. 
"""

from typing import Iterable

import pyarrow as pa
import pyarrow.parquet as pq

from etl_framework.base.loader import BaseLoader
from etl_framework.exceptions.pipeline_errors import PermanentLoadError


# ParquetLoader 
class ParquetLoader(BaseLoader): 
    """Batch-writes records to a Parquet file."""

    def __init__(self, output_path: str, batch_size: int = 500) -> None: 
        super().__init__() 
        self.output_path = output_path 
        self.batch_size = batch_size 

    def load(self, records: Iterable[dict]) -> int: 
        """
        Consumes `records` in batches of self.batch_size and writes them to 
        self.output_path as Parquet. Returns the total number of records 
        written. 

        Raises: 
            PermanentLoadError: if `records` yields nothing at all, or if a 
                batch cannot be converted into a valide Arrow table (e.g.
                inconsistent fields/types across records in the batch).
        """
        writer = None 
        total_written = 0 
        batch: list[dict] = []

        try: 
            for record in records: 
                batch.append(record) 
                if len(batch) >= self.batch_size: 
                    writer, written = self._write_batch(batch, writer) 
                    total_written += written 
                    batch = [] 

            if batch: 
                writer, written = self._write_batch(batch, writer) 
                total_written += written 

            if writer is None: 
                raise PermanentLoadError(
                    "No records to load. Input iterable was empty."
                )
        finally: 
            if writer is not None: 
                writer.close() 

        return total_written
    
    def _write_batch(self, batch: list[dict], writer): 
        """
        Converts one batch of dicts into an Arrow Table and writes it, 
        lazily creating the ParquetWriter (and locking in the output schema)
        from the first batch seen. 
        """
        try: 
            table = pa.Table.from_pylist(batch) 
        except (pa.ArrowInvalid, pa.ArrowTypeError) as e: 
            raise PermanentLoadError(
                f"Batch could not be converted to an Arrow table: {e}"
            ) from e 
        
        if writer is None: 
            writer = pq.ParquetWriter(self.output_path, table.schema) 

        writer.write_table(table) 
        return writer, len(batch) 

