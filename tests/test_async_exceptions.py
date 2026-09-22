"""Exception propagation and sync/async parity for ``AsyncStream``.

``test_async_stream`` covers the happy paths; this file pins down what
happens when a user callback or a source blows up, and checks that an
``AsyncStream`` chain agrees with the equivalent ``Stream`` chain.
"""

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator
from typing import Any, NoReturn, cast

import pytest
from hypothesis import given
from hypothesis import strategies as st

from streamlet import AsyncStream, Stream, StreamConsumedError

ints = st.lists(st.integers())
counts = st.integers(min_value=0, max_value=100)


class Boom(Exception):
    """Distinct exception type, so a test cannot pass on a coincidence."""


def explode(*_: object) -> NoReturn:
    raise Boom("sync boom")


async def explode_async(*_: object) -> NoReturn:
    await asyncio.sleep(0)
    raise Boom("async boom")


# --- a failing callback propagates, whatever the op ---------------------


@pytest.mark.parametrize("failing", [explode, explode_async], ids=["sync", "async"])
async def test_a_failing_intermediate_callback_propagates(failing: Any) -> None:
    stream: AsyncStream[int] = AsyncStream.of(1, 2, 3)
    pipelines = [
        stream.map(failing),
        AsyncStream.of(1, 2, 3).filter(failing),
        AsyncStream.of(1, 2, 3).flat_map(failing),
        AsyncStream.of(1, 2, 3).take_while(failing),
        AsyncStream.of(1, 2, 3).drop_while(failing),
        AsyncStream.of(1, 2, 3).peek(failing),
    ]
    for pipeline in pipelines:
        with pytest.raises(Boom):
            await pipeline.to_list()


@pytest.mark.parametrize("failing", [explode, explode_async], ids=["sync", "async"])
async def test_a_failing_terminal_callback_propagates(failing: Any) -> None:
    with pytest.raises(Boom):
        await AsyncStream.of(1, 2, 3).for_each(failing)
    with pytest.raises(Boom):
        await AsyncStream.of(1, 2, 3).any(failing)
    with pytest.raises(Boom):
        await AsyncStream.of(1, 2, 3).all(failing)
    with pytest.raises(Boom):
        await AsyncStream.of(1, 2, 3).none(failing)
    with pytest.raises(Boom):
        await AsyncStream.of(1, 2, 3).reduce(failing, 0)


async def test_a_failing_collector_key_propagates() -> None:
    with pytest.raises(Boom):
        await AsyncStream.of(1, 2, 3).to_dict(explode)
    with pytest.raises(Boom):
        await AsyncStream.of(1, 2, 3).group_by(explode)


async def test_the_original_exception_instance_reaches_the_caller() -> None:
    error = Boom("the one instance")

    async def raise_it(_: int) -> NoReturn:
        raise error

    with pytest.raises(Boom) as caught:
        await AsyncStream.of(1).map(raise_it).to_list()
    assert caught.value is error


async def test_a_concurrent_failure_is_not_wrapped_in_an_exception_group() -> None:
    """``TaskGroup`` would raise an ``ExceptionGroup``; ``map_concurrent`` does not."""
    error = Boom("single failure")

    async def raise_it(_: int) -> NoReturn:
        raise error

    with pytest.raises(Boom) as caught:
        await AsyncStream.from_iterable(range(20)).map_concurrent(raise_it, limit=4).to_list()
    assert caught.value is error
    assert not isinstance(caught.value, BaseExceptionGroup)


async def test_a_base_exception_is_not_swallowed() -> None:
    async def interrupt(_: int) -> NoReturn:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        await AsyncStream.of(1).map(interrupt).to_list()


async def test_items_before_a_failure_are_still_delivered() -> None:
    seen: list[int] = []

    async def fail_on_third(n: int) -> int:
        if n == 2:
            raise Boom("third item")
        return n

    with pytest.raises(Boom):
        async for item in AsyncStream.from_iterable(range(10)).map(fail_on_third):
            seen.append(item)
    assert seen == [0, 1]


async def test_a_failure_deep_in_a_chain_propagates_through_every_stage() -> None:
    pipeline = (
        AsyncStream.from_iterable(range(10))
        .filter(lambda n: n % 2 == 0)
        .map(explode_async)
        .take(2)
        .distinct()
    )
    with pytest.raises(Boom, match="async boom"):
        await pipeline.to_list()


# --- source finalisation ------------------------------------------------


async def test_closing_a_stream_finalises_its_source() -> None:
    cleaned: list[str] = []

    async def tracked() -> AsyncIterator[int]:
        try:
            for n in range(100):
                yield n
        finally:
            cleaned.append("closed")

    iterator = AsyncStream(tracked()).__aiter__()
    assert await iterator.__anext__() == 0
    assert cleaned == []

    await cast("AsyncGenerator[int, None]", iterator).aclose()
    assert cleaned == ["closed"]


async def test_closing_a_stage_does_not_yet_reach_through_to_the_source() -> None:
    """Documents current behaviour: ``aclose`` stops at the stage it is called on.

    An abandoned multi-stage pipeline leaves the source suspended until the
    event loop shuts its async generators down (or the interpreter collects
    them), rather than closing it right away. Tracked in THR-34.
    """
    cleaned: list[str] = []

    async def tracked() -> AsyncIterator[int]:
        try:
            for n in range(100):
                yield n
        finally:
            cleaned.append("closed")

    iterator = AsyncStream(tracked()).map(lambda n: n * 2).__aiter__()
    assert await iterator.__anext__() == 0

    await cast("AsyncGenerator[int, None]", iterator).aclose()
    assert cleaned == []


async def test_a_failing_source_finalises_itself() -> None:
    cleaned: list[str] = []

    async def broken() -> AsyncIterator[int]:
        try:
            yield 0
            raise Boom("source failed")
        finally:
            cleaned.append("closed")

    with pytest.raises(Boom, match="source failed"):
        await AsyncStream(broken()).to_list()
    assert cleaned == ["closed"]


# --- single use ---------------------------------------------------------


async def test_a_partially_iterated_stream_cannot_be_reused() -> None:
    stream = AsyncStream.from_iterable(range(10))
    assert await stream.first() == 0
    with pytest.raises(StreamConsumedError):
        await stream.to_list()


async def test_a_failed_stream_cannot_be_reused() -> None:
    stream = AsyncStream.of(1, 2).map(explode_async)
    with pytest.raises(Boom):
        await stream.to_list()
    with pytest.raises(StreamConsumedError):
        await stream.to_list()


# --- parity with the synchronous Stream ---------------------------------


@given(ints)
def test_map_and_filter_match_the_sync_stream(items: list[int]) -> None:
    async def run() -> list[int]:
        return (
            await AsyncStream.from_iterable(items).filter(lambda n: n % 2 == 0).map(abs).to_list()
        )

    expected = Stream(items).filter(lambda n: n % 2 == 0).map(abs).to_list()
    assert asyncio.run(run()) == expected


@given(ints, counts)
def test_take_and_skip_match_the_sync_stream(items: list[int], n: int) -> None:
    async def run() -> tuple[list[int], list[int]]:
        taken = await AsyncStream.from_iterable(items).take(n).to_list()
        skipped = await AsyncStream.from_iterable(items).skip(n).to_list()
        return taken, skipped

    assert asyncio.run(run()) == (items[:n], items[n:])


@given(ints)
def test_terminal_ops_match_the_sync_stream(items: list[int]) -> None:
    async def run() -> tuple[int, int, int | None, dict[bool, list[int]]]:
        return (
            await AsyncStream.from_iterable(items).count(),
            await AsyncStream.from_iterable(items).sum(),
            await AsyncStream.from_iterable(items).max(),
            await AsyncStream.from_iterable(items).group_by(lambda n: n % 2 == 0),
        )

    expected = (
        Stream(items).count(),
        Stream(items).sum(),
        Stream(items).max(),
        Stream(items).group_by(lambda n: n % 2 == 0),
    )
    assert asyncio.run(run()) == expected


@given(ints, counts)
def test_concurrent_map_matches_a_sequential_map(items: list[int], limit: int) -> None:
    async def run() -> tuple[list[int], list[int]]:
        async def double(n: int) -> int:
            await asyncio.sleep(0)
            return n * 2

        ordered = await (
            AsyncStream.from_iterable(items).map_concurrent(double, limit=limit + 1).to_list()
        )
        unordered = await (
            AsyncStream.from_iterable(items)
            .map_concurrent(double, limit=limit + 1, ordered=False)
            .to_list()
        )
        return ordered, unordered

    ordered, unordered = asyncio.run(run())
    expected = [n * 2 for n in items]
    assert ordered == expected
    assert sorted(unordered) == sorted(expected)
