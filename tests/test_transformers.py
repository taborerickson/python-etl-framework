# tests/test_transformers.py 

"""
Tests for BaseTransformer / PassThroughTransformer
(etl_framework/transformers/base.py, etl_framework/transformers/passthrough.py).

Covers:
- transform() is confirmed to be a real generator
- PassThroughTransformer yields records unchanged, in the same order
- PassThroughTransformer does not materialize its input
"""

import inspect

from etl_framework.transformers.passthrough import PassThroughTransformer


class NoMaterializeIterator:
    """Same technique as test_parquet_loader.py -- duplicated here rather
    than imported so this test file has no dependency on another test
    file's internals."""

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


def test_transform_is_a_generator_function():
    assert inspect.isgeneratorfunction(PassThroughTransformer.transform)


def test_passthrough_yields_records_unchanged_and_in_order():
    transformer = PassThroughTransformer()
    records = [{"id": 1}, {"id": 2}, {"id": 3}]

    result = list(transformer.transform(iter(records)))

    assert result == records


def test_passthrough_does_not_materialize_input():
    transformer = PassThroughTransformer()
    records = [{"id": 1}, {"id": 2}, {"id": 3}]

    gen = transformer.transform(NoMaterializeIterator(records))
    first = next(gen)
    assert first == {"id": 1}

    remaining = list(gen)
    assert remaining == [{"id": 2}, {"id": 3}]


def test_passthrough_is_lazy_nothing_pulled_until_iterated():
    """
    Confirms transform() doesn't eagerly pull even the FIRST record at
    construction time -- only once the caller starts iterating.
    """
    pulled = []

    def tracking_source():
        for i in range(3):
            pulled.append(i)
            yield {"id": i}

    transformer = PassThroughTransformer()
    gen = transformer.transform(tracking_source())

    assert pulled == []  # nothing pulled yet -- generator construction is lazy
    next(gen)
    assert pulled == [0]  # exactly one record pulled after one next() call

