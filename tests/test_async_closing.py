"""Deterministic source cleanup for ``AsyncStream``.

Every stage closes the one behind it, so abandoning a pipeline -- a bounded
``take``, a terminal op that short-circuits, an exception, an explicit
``aclose`` -- unwinds the whole chain to the source instead of leaving it
suspended until the event loop shuts its async generators down.

The one case Python does not hand us is a bare ``break`` out of an
``async for``: that leaves the outermost generator suspended, with nothing
to trigger the unwind. ``aclosing`` around the loop is the fix, and the
chain then closes the whole way down.
"""

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import aclosing
from typing import cast

import pytest

from pystreamlet import AsyncStream


class Source:
    """An async source that records when it is finalised."""

    def __init__(self, stop: int = 100) -> None:
        self.stop = stop
        self.closed = False
        self.pulled: list[int] = []

    async def __aiter__(self) -> AsyncIterator[int]:
        try:
            for n in range(self.stop):
                self.pulled.append(n)
                yield n
        finally:
            self.closed = True


def stream(source: Source) -> AsyncStream[int]:
    return AsyncStream(source.__aiter__())


# --- abandoning a pipeline ----------------------------------------------


async def test_take_closes_the_source_once_it_has_enough() -> None:
    source = Source()
    assert await stream(source).map(lambda n: n * 2).take(3).to_list() == [0, 2, 4]
    assert source.closed
    assert source.pulled == [0, 1, 2]


async def test_breaking_out_needs_aclosing_and_then_unwinds() -> None:
    source = Source()
    pipeline = stream(source).map(lambda n: n * 2).filter(lambda n: True)
    async with aclosing(cast("AsyncGenerator[int, None]", pipeline.__aiter__())) as items:
        async for _ in items:
            break
    assert source.closed


async def test_a_bare_break_leaves_the_outermost_stage_suspended() -> None:
    """Python's own semantics: ``break`` does not close what it iterated.

    Nothing in the chain can fix this -- there is no hook -- so the pipeline
    stays suspended until the loop finalises it. Use ``aclosing`` when the
    source holds something that must be released promptly.
    """
    source = Source()
    async for _ in stream(source).map(lambda n: n * 2):
        break
    assert not source.closed


async def test_a_short_circuiting_terminal_closes_the_source() -> None:
    source = Source()
    assert await stream(source).map(lambda n: n * 2).first() == 0
    assert source.closed

    source = Source()
    assert await stream(source).any(lambda n: n == 2) is True
    assert source.closed

    source = Source()
    assert await stream(source).all(lambda n: n < 2) is False
    assert source.closed


async def test_take_while_closes_the_source_at_the_first_failure() -> None:
    source = Source()
    assert await stream(source).take_while(lambda n: n < 3).to_list() == [0, 1, 2]
    assert source.closed


async def test_exhausting_a_chain_closes_the_source() -> None:
    source = Source(stop=5)
    assert await stream(source).map(lambda n: n * 2).to_list() == [0, 2, 4, 6, 8]
    assert source.closed


async def test_a_failure_mid_chain_closes_the_source() -> None:
    source = Source()

    def explode(n: int) -> int:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await stream(source).map(explode).to_list()
    assert source.closed


async def test_closing_a_long_chain_unwinds_every_stage() -> None:
    source = Source()
    iterator = (
        stream(source)
        .filter(lambda n: n % 2 == 0)
        .map(lambda n: n * 2)
        .peek(lambda n: None)
        .distinct()
        .skip(1)
        .__aiter__()
    )
    assert await iterator.__anext__() == 4
    assert not source.closed

    await cast("AsyncGenerator[int, None]", iterator).aclose()
    assert source.closed


# --- map_concurrent -----------------------------------------------------


async def test_concurrent_map_closes_the_source_when_bounded() -> None:
    source = Source()

    async def double(n: int) -> int:
        await asyncio.sleep(0)
        return n * 2

    assert await stream(source).map_concurrent(double, limit=4).take(3).to_list() == [0, 2, 4]
    assert source.closed


async def test_unordered_concurrent_map_closes_the_source() -> None:
    source = Source()

    async def double(n: int) -> int:
        await asyncio.sleep(0)
        return n * 2

    result = await stream(source).map_concurrent(double, limit=4, ordered=False).take(3).to_list()
    # Unordered yields whichever of the in-flight calls finish first, so the
    # three values are not fixed -- only that they are doubled source items.
    assert len(result) == 3
    assert all(value % 2 == 0 for value in result)
    assert source.closed


async def test_a_failing_concurrent_mapper_closes_the_source() -> None:
    source = Source()

    async def explode(n: int) -> int:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await stream(source).map_concurrent(explode, limit=4).to_list()
    assert source.closed


# --- sources with nothing to close --------------------------------------


async def test_a_plain_async_iterator_source_still_works() -> None:
    """A hand-written iterator has no ``aclose``; closing must not trip over it."""

    class Counter:
        def __init__(self) -> None:
            self.n = 0

        def __aiter__(self) -> "Counter":
            return self

        async def __anext__(self) -> int:
            if self.n >= 5:
                raise StopAsyncIteration
            self.n += 1
            return self.n

    assert await AsyncStream(Counter()).map(lambda n: n * 2).take(2).to_list() == [2, 4]
    assert await AsyncStream(Counter()).first() == 1


async def test_a_sync_backed_stream_closes_cleanly() -> None:
    assert await AsyncStream.from_iterable(range(100)).take(2).to_list() == [0, 1]
