"""Edge cases: empty streams, infinite sources, bad arguments, reuse."""

from itertools import count

import pytest
from hypothesis import given
from hypothesis import strategies as st

from streamlet import Stream, StreamConsumedError

# --- empty streams ------------------------------------------------------


def test_every_op_survives_an_empty_stream() -> None:
    assert Stream.empty().map(lambda n: n * 2).to_list() == []
    assert Stream.empty().filter(lambda n: True).to_list() == []
    assert Stream.empty().take(5).to_list() == []
    assert Stream.empty().skip(5).to_list() == []
    assert Stream.empty().flat_map(lambda n: [n]).to_list() == []
    assert Stream.empty().distinct().to_list() == []


def test_terminal_ops_on_an_empty_stream() -> None:
    assert Stream.empty().to_list() == []
    assert Stream.empty().count() == 0
    assert Stream[int].empty().sum() == 0
    assert Stream.empty().first() is None
    assert Stream.empty().reduce(lambda a, b: a + b, 42) == 42
    assert Stream.empty().group_by(lambda n: n) == {}


def test_empty_conventions_for_any_and_all() -> None:
    """``all`` is vacuously true on nothing; ``any`` is false."""
    assert Stream.empty().all(lambda n: False) is True
    assert Stream.empty().any(lambda n: True) is False


# --- infinite sources ---------------------------------------------------


def test_take_bounds_an_infinite_source() -> None:
    assert Stream.from_iterable(count()).take(4).to_list() == [0, 1, 2, 3]


def test_skip_then_take_on_an_infinite_source() -> None:
    assert Stream.from_iterable(count()).skip(10).take(3).to_list() == [10, 11, 12]


def test_distinct_stays_lazy_on_an_infinite_source() -> None:
    repeating = (n % 3 for n in count())
    assert Stream(repeating).distinct().take(3).to_list() == [0, 1, 2]


def test_flat_map_stays_lazy_on_an_infinite_source() -> None:
    assert Stream.from_iterable(count()).flat_map(lambda n: [n, n]).take(5).to_list() == [
        0,
        0,
        1,
        1,
        2,
    ]


# --- argument validation ------------------------------------------------


@given(st.integers(max_value=-1))
def test_take_rejects_negative_counts(n: int) -> None:
    with pytest.raises(ValueError, match="non-negative"):
        Stream.of(1, 2, 3).take(n)


@given(st.integers(max_value=-1))
def test_skip_rejects_negative_counts(n: int) -> None:
    with pytest.raises(ValueError, match="non-negative"):
        Stream.of(1, 2, 3).skip(n)


def test_take_zero_is_empty_and_skip_zero_is_everything() -> None:
    assert Stream.of(1, 2, 3).take(0).to_list() == []
    assert Stream.of(1, 2, 3).skip(0).to_list() == [1, 2, 3]


def test_take_and_skip_beyond_the_end() -> None:
    assert Stream.of(1, 2).take(99).to_list() == [1, 2]
    assert Stream.of(1, 2).skip(99).to_list() == []


# --- unhashable items ---------------------------------------------------


def test_distinct_requires_hashable_items() -> None:
    with pytest.raises(TypeError):
        Stream.of([1], [2]).distinct().to_list()


def test_group_by_requires_hashable_keys() -> None:
    with pytest.raises(TypeError):
        Stream.of(1, 2).group_by(lambda n: [n])  # type: ignore[type-var]


def test_flat_map_requires_an_iterable_result() -> None:
    with pytest.raises(TypeError):
        Stream.of(1, 2).flat_map(lambda n: n).to_list()  # type: ignore[arg-type,return-value]


# --- reuse --------------------------------------------------------------


def test_partial_iteration_still_consumes_the_stream() -> None:
    """Breaking out of a loop does not make the stream reusable."""
    stream = Stream.of(1, 2, 3)
    for _ in stream:
        break
    with pytest.raises(StreamConsumedError):
        stream.to_list()


def test_building_a_pipeline_consumes_the_source() -> None:
    source = Stream.of(1, 2, 3)
    source.map(lambda n: n)
    with pytest.raises(StreamConsumedError):
        source.to_list()


def test_each_derived_stream_is_independently_single_use() -> None:
    derived = Stream.of(1, 2, 3).map(lambda n: n * 2)
    assert derived.to_list() == [2, 4, 6]
    with pytest.raises(StreamConsumedError):
        derived.to_list()
