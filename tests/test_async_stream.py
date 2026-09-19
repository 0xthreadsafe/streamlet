"""``AsyncStream`` core: ``__aiter__``, constructors, ops, laziness."""

import asyncio
from collections.abc import AsyncIterator

import pytest

from streamlet import AsyncStream, StreamConsumedError


async def double(n: int) -> int:
    await asyncio.sleep(0)
    return n * 2


async def is_even(n: int) -> bool:
    await asyncio.sleep(0)
    return n % 2 == 0


async def arange(stop: int) -> AsyncIterator[int]:
    for n in range(stop):
        yield n


# --- protocol -----------------------------------------------------------


async def test_supports_async_for() -> None:
    assert [item async for item in AsyncStream.of(1, 2, 3)] == [1, 2, 3]


async def test_aiter_returns_an_async_iterator() -> None:
    stream = AsyncStream.of(1, 2, 3)
    iterator = stream.__aiter__()
    assert await iterator.__anext__() == 1


async def test_is_single_use() -> None:
    stream = AsyncStream.of(1, 2, 3)
    assert await stream.to_list() == [1, 2, 3]
    with pytest.raises(StreamConsumedError):
        await stream.to_list()


async def test_repr_reflects_state() -> None:
    stream = AsyncStream.of(1, 2)
    assert repr(stream) == "<AsyncStream lazy>"
    await stream.to_list()
    assert repr(stream) == "<AsyncStream consumed>"


# --- constructors -------------------------------------------------------


async def test_of_and_from_iterable() -> None:
    assert await AsyncStream.of(1, 2).to_list() == [1, 2]
    assert await AsyncStream.from_iterable(range(3)).to_list() == [0, 1, 2]


async def test_of_treats_an_iterable_as_one_item() -> None:
    assert await AsyncStream.of([1, 2]).to_list() == [[1, 2]]


async def test_from_async_iterable() -> None:
    assert await AsyncStream.from_async_iterable(arange(3)).to_list() == [0, 1, 2]


async def test_empty() -> None:
    assert await AsyncStream.empty().to_list() == []


async def test_iterate_with_sync_and_async_steps() -> None:
    assert await AsyncStream.iterate(1, lambda n: n * 2).take(4).to_list() == [1, 2, 4, 8]
    assert await AsyncStream.iterate(1, double).take(4).to_list() == [1, 2, 4, 8]


async def test_generate() -> None:
    counter = iter(range(100))
    assert await AsyncStream.generate(lambda: next(counter)).take(3).to_list() == [0, 1, 2]


async def test_concat_mixes_sync_and_async_sources() -> None:
    result = await AsyncStream.concat([1, 2], arange(2), AsyncStream.of(9)).to_list()
    assert result == [1, 2, 0, 1, 9]


async def test_concat_with_no_sources_is_empty() -> None:
    assert await AsyncStream.concat().to_list() == []


# --- ops accept sync or async functions ---------------------------------


async def test_map_accepts_both_kinds_of_function() -> None:
    assert await AsyncStream.of(1, 2).map(lambda n: n * 2).to_list() == [2, 4]
    assert await AsyncStream.of(1, 2).map(double).to_list() == [2, 4]


async def test_filter_accepts_both_kinds_of_function() -> None:
    assert await AsyncStream.from_iterable(range(5)).filter(lambda n: n % 2 == 0).to_list() == [
        0,
        2,
        4,
    ]
    assert await AsyncStream.from_iterable(range(5)).filter(is_even).to_list() == [0, 2, 4]


async def test_flat_map() -> None:
    assert await AsyncStream.of([1, 2], [3]).flat_map(lambda xs: xs).to_list() == [1, 2, 3]


async def test_distinct_preserves_first_seen_order() -> None:
    assert await AsyncStream.of(3, 1, 3, 2, 1).distinct().to_list() == [3, 1, 2]


async def test_take_and_skip() -> None:
    assert await AsyncStream.from_iterable(range(10)).take(3).to_list() == [0, 1, 2]
    assert await AsyncStream.from_iterable(range(5)).skip(3).to_list() == [3, 4]
    assert await AsyncStream.of(1, 2).take(99).to_list() == [1, 2]
    assert await AsyncStream.of(1, 2).skip(99).to_list() == []
    assert await AsyncStream.of(1, 2).take(0).to_list() == []


@pytest.mark.parametrize("n", [-1, -5])
async def test_take_and_skip_reject_negative_counts(n: int) -> None:
    with pytest.raises(ValueError, match="non-negative"):
        AsyncStream.of(1).take(n)
    with pytest.raises(ValueError, match="non-negative"):
        AsyncStream.of(1).skip(n)


async def test_take_while_and_drop_while() -> None:
    assert await AsyncStream.of(1, 2, 9, 3).take_while(lambda n: n < 5).to_list() == [1, 2]
    assert await AsyncStream.of(1, 2, 9, 3).drop_while(lambda n: n < 5).to_list() == [9, 3]


async def test_peek_does_not_change_items() -> None:
    seen: list[int] = []
    assert await AsyncStream.of(1, 2).peek(seen.append).to_list() == [1, 2]
    assert seen == [1, 2]


# --- terminal ops -------------------------------------------------------


async def test_collectors() -> None:
    assert await AsyncStream.of(1, 2).to_tuple() == (1, 2)
    assert await AsyncStream.of(1, 2, 2).to_set() == {1, 2}
    assert await AsyncStream.of("ab", "c").to_dict(len) == {2: "ab", 1: "c"}
    assert await AsyncStream.of("ab", "c").to_dict(len, str.upper) == {2: "AB", 1: "C"}
    assert await AsyncStream.of("a", "b").join("-") == "a-b"


async def test_aggregates() -> None:
    assert await AsyncStream.of(1, 2, 3).count() == 3
    assert await AsyncStream.of(1, 2, 3).sum() == 6
    assert await AsyncStream.of(3, 1, 2).min() == 1
    assert await AsyncStream.of(3, 1, 2).max() == 3
    assert await AsyncStream.of("bb", "a").max(key=len) == "bb"


async def test_reduce_with_sync_and_async_folds() -> None:
    assert await AsyncStream.of(1, 2, 3).reduce(lambda acc, n: acc + n, 0) == 6

    async def add(acc: int, n: int) -> int:
        await asyncio.sleep(0)
        return acc + n

    assert await AsyncStream.of(1, 2, 3).reduce(add, 0) == 6


async def test_reduce_can_change_the_result_type() -> None:
    assert await AsyncStream.of(1, 2).reduce(lambda acc, n: acc + str(n), "") == "12"


async def test_group_by() -> None:
    groups = await AsyncStream.of("apple", "avocado", "fig").group_by(lambda w: w[0])
    assert groups == {"a": ["apple", "avocado"], "f": ["fig"]}


async def test_matching_ops() -> None:
    assert await AsyncStream.of(1, 2).any(lambda n: n > 1) is True
    assert await AsyncStream.of(1, 2).all(lambda n: n > 0) is True
    assert await AsyncStream.of(1, 2).none(lambda n: n > 5) is True
    assert await AsyncStream.of(1, 2).any(is_even) is True


async def test_empty_conventions() -> None:
    assert await AsyncStream[int].empty().first() is None
    assert await AsyncStream[int].empty().all(lambda n: False) is True
    assert await AsyncStream[int].empty().any(lambda n: True) is False
    assert await AsyncStream[int].empty().none(lambda n: True) is True
    assert await AsyncStream[int].empty().sum() == 0
    assert await AsyncStream[int].empty().min() is None
    assert await AsyncStream[int].empty().group_by(lambda n: n) == {}


async def test_for_each() -> None:
    seen: list[int] = []
    await AsyncStream.of(1, 2, 3).for_each(seen.append)
    assert seen == [1, 2, 3]


async def test_pipe_operator() -> None:
    def evens(stream: AsyncStream[int]) -> AsyncStream[int]:
        return stream.filter(lambda n: n % 2 == 0)

    assert await (AsyncStream.from_iterable(range(6)) | evens).to_list() == [0, 2, 4]


# --- laziness -----------------------------------------------------------


async def test_building_a_pipeline_runs_nothing() -> None:
    calls: list[int] = []

    def record(n: int) -> int:
        calls.append(n)
        return n

    AsyncStream.from_iterable(range(10)).map(record).take(3)
    assert calls == []


async def test_only_pulls_what_is_needed() -> None:
    pulled: list[int] = []

    async def tracked() -> AsyncIterator[int]:
        for n in range(1000):
            pulled.append(n)
            yield n

    assert await AsyncStream(tracked()).take(3).to_list() == [0, 1, 2]
    assert pulled == [0, 1, 2]


async def test_first_pulls_exactly_one_item() -> None:
    pulled: list[int] = []

    async def tracked() -> AsyncIterator[int]:
        for n in range(1000):
            pulled.append(n)
            yield n

    assert await AsyncStream(tracked()).first() == 0
    assert pulled == [0]


async def test_any_short_circuits_on_an_infinite_source() -> None:
    assert await AsyncStream.iterate(0, lambda n: n + 1).any(lambda n: n == 5) is True


async def test_take_bounds_an_infinite_source() -> None:
    assert await AsyncStream.iterate(0, lambda n: n + 1).take(3).to_list() == [0, 1, 2]


async def test_mapper_runs_once_per_pulled_item() -> None:
    calls: list[int] = []

    async def tracked_double(n: int) -> int:
        calls.append(n)
        return n * 2

    assert await AsyncStream.from_iterable(range(100)).map(tracked_double).take(3).to_list() == [
        0,
        2,
        4,
    ]
    assert calls == [0, 1, 2]


# --- exception propagation ----------------------------------------------


async def test_an_exception_in_a_mapper_propagates() -> None:
    async def explode(n: int) -> int:
        raise ValueError(f"boom {n}")

    with pytest.raises(ValueError, match="boom 0"):
        await AsyncStream.of(0, 1).map(explode).to_list()


async def test_an_exception_in_the_source_propagates() -> None:
    async def broken() -> AsyncIterator[int]:
        yield 1
        raise RuntimeError("source failed")

    with pytest.raises(RuntimeError, match="source failed"):
        await AsyncStream(broken()).to_list()


async def test_a_failing_source_still_yields_earlier_items() -> None:
    async def broken() -> AsyncIterator[int]:
        yield 1
        raise RuntimeError("late failure")

    assert await AsyncStream(broken()).take(1).to_list() == [1]


async def test_cancellation_propagates() -> None:
    async def slow() -> AsyncIterator[int]:
        while True:
            await asyncio.sleep(10)
            yield 1

    task = asyncio.create_task(AsyncStream(slow()).to_list())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
