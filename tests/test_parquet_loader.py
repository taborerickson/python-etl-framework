# tests/test_parquet_loader.py 

"""
Tests for ParquetLoader (etl_framework/loaders/parquet_loader.py).

Covers:
- a generator of records is correctly batch-written to a valid Parquet
  file and the returned count matches records written
- batching: with a small batch_size, _write_batch() is invoked multiple
  times (once per full batch, plus once for a final partial batch) rather
  than once for the whole input
- an empty input iterable raises PermanentLoadError
- inconsistent record shapes that pyarrow can't convert raise
  PermanentLoadError
- the loader never materializes the full input into a list before writing
"""

import pyarrow.parquet as pq
import pytest

from etl_framework.exceptions.pipeline_errors import PermanentLoadError
from etl_framework.loaders.parquet_loader import ParquetLoader


class NoMaterializeIterator:
    """
    An iterator that raises if anything tries to consume it eagerly via
    list()/len()/tuple() -- but supports normal one-at-a-time iteration.
    Used to prove ParquetLoader.load() never buffers its full input.
    """

    def __init__(self, records):
        self._records = records
        self._index = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self._index >= len(self._records):
            raise StopIteration
        record = self._records[self._index]
        self._index += 1
        return record

    def __len__(self):
        raise AssertionError("len() was called -- input was materialized eagerly")


def _sample_records(n):
    return [{"id": i, "value": f"row-{i}"} for i in range(n)]


def test_writes_all_records_and_returns_correct_count(tmp_path):
    output_path = tmp_path / "output.parquet"
    loader = ParquetLoader(output_path=str(output_path), batch_size=500)
    records = _sample_records(10)

    written = loader.run(iter(records))

    assert written == 10
    table = pq.read_table(str(output_path))
    assert table.num_rows == 10
    assert table.to_pylist() == records


def test_batching_invokes_write_batch_multiple_times(tmp_path, monkeypatch):
    output_path = tmp_path / "output.parquet"
    loader = ParquetLoader(output_path=str(output_path), batch_size=3)
    records = _sample_records(7)  # 3 + 3 + 1 -> 3 calls to _write_batch

    call_sizes = []
    original_write_batch = loader._write_batch

    def spy_write_batch(batch, writer):
        call_sizes.append(len(batch))
        return original_write_batch(batch, writer)

    monkeypatch.setattr(loader, "_write_batch", spy_write_batch)

    written = loader.run(iter(records))

    assert written == 7
    assert call_sizes == [3, 3, 1]  # two full batches, one partial final batch


def test_empty_input_raises_permanent_load_error(tmp_path):
    output_path = tmp_path / "output.parquet"
    loader = ParquetLoader(output_path=str(output_path))

    with pytest.raises(PermanentLoadError, match="No records to load"):
        loader.run(iter([]))


def test_inconsistent_records_raise_permanent_load_error(tmp_path):
    output_path = tmp_path / "output.parquet"
    loader = ParquetLoader(output_path=str(output_path), batch_size=10)
    # id is an int in one record, a string in another -- pyarrow can't infer
    # one consistent column type across the batch
    bad_records = [{"id": 1, "name": "Alice"}, {"id": "not-an-int", "name": "Bob"}]

    with pytest.raises(PermanentLoadError):
        loader.run(iter(bad_records))


def test_does_not_materialize_full_input_before_writing(tmp_path):
    output_path = tmp_path / "output.parquet"
    loader = ParquetLoader(output_path=str(output_path), batch_size=2)
    records = _sample_records(5)

    written = loader.run(NoMaterializeIterator(records))

    assert written == 5
    table = pq.read_table(str(output_path))
    assert table.to_pylist() == records

