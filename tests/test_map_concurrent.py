"""``AsyncStream.map_concurrent``: bounded, cancel-safe concurrency.

Concurrency is asserted with an in-flight counter rather than wall-clock
timing, so the results are deterministic under a loaded CI runner.
"""

import asyncio
from collections.abc import AsyncIterator

import pytest

from pystreamlet import AsyncStream


class Tracker:
    """Records peak concurrency and how many calls were cancelled."""

    def __init__(self, delay: float = 0.01) -> None:
        self.delay = delay
        self.inflight = 0
        self.peak = 0
        self.started: list[int] = []
        self.cancelled = 0

    async def __call__(self, n: int) -> int:
        self.started.append(n)
        self.inflight += 1
        self.peak = max(self.peak, self.inflight)
        try:
            await asyncio.sleep(self.delay)
        except asyncio.CancelledError:
            self.cancelled += 1
            raise
        finally:
            self.inflight -= 1
        return n * 10


# --- results ------------------------------------------------------------


@pytest.mark.parametrize("limit", [1, 2, 5, 50])
async def test_ordered_results_match_a_sequential_map(limit: int) -> None:
    tracker = Tracker()
    result = (
        await AsyncStream.from_iterable(range(10)).map_concurrent(tracker, limit=limit).to_list()
    )
    assert result == [n * 10 for n in range(10)]


@pytest.mark.parametrize("limit", [1, 3, 20])
async def test_unordered_yields_the_same_items(limit: int) -> None:
    tracker = Tracker()
    result = (
        await AsyncStream.from_iterable(range(10))
        .map_concurrent(tracker, limit=limit, ordered=False)
        .to_list()
    )
    assert sorted(result) == [n * 10 for n in range(10)]


async def test_empty_stream() -> None:
    tracker = Tracker()
    assert await AsyncStream[int].empty().map_concurrent(tracker).to_list() == []
    assert tracker.started == []


async def test_single_item() -> None:
    assert await AsyncStream.of(1).map_concurrent(Tracker()).to_list() == [10]


# --- concurrency bound --------------------------------------------------


@pytest.mark.parametrize("limit", [1, 2, 4, 7])
async def test_never_exceeds_the_limit(limit: int) -> None:
    tracker = Tracker()
    await AsyncStream.from_iterable(range(20)).map_concurrent(tracker, limit=limit).to_list()
    assert tracker.peak <= limit


async def test_actually_runs_concurrently() -> None:
    """With room to spare, several calls really are in flight at once."""
    tracker = Tracker()
    await AsyncStream.from_iterable(range(10)).map_concurrent(tracker, limit=5).to_list()
    assert tracker.peak == 5


async def test_limit_of_one_is_sequential() -> None:
    tracker = Tracker()
    await AsyncStream.from_iterable(range(5)).map_concurrent(tracker, limit=1).to_list()
    assert tracker.peak == 1
    assert tracker.started == [0, 1, 2, 3, 4]


@pytest.mark.parametrize("limit", [0, -1])
async def test_rejects_a_limit_below_one(limit: int) -> None:
    with pytest.raises(ValueError, match="at least 1"):
        AsyncStream.of(1).map_concurrent(Tracker(), limit=limit)


# --- laziness -----------------------------------------------------------


async def test_builds_nothing_until_awaited() -> None:
    tracker = Tracker()
    AsyncStream.from_iterable(range(10)).map_concurrent(tracker, limit=3)
    await asyncio.sleep(0)
    assert tracker.started == []


async def test_works_on_an_infinite_source() -> None:
    tracker = Tracker()
    result = await (
        AsyncStream.iterate(0, lambda n: n + 1).map_concurrent(tracker, limit=3).take(4).to_list()
    )
    assert result == [0, 10, 20, 30]


async def test_does_not_run_far_ahead_of_the_consumer() -> None:
    """Only ``limit`` items are pulled beyond what has been yielded."""
    tracker = Tracker()
    await AsyncStream.iterate(0, lambda n: n + 1).map_concurrent(tracker, limit=3).take(2).to_list()
    await asyncio.sleep(0.05)
    assert len(tracker.started) <= 5


# --- cancellation -------------------------------------------------------


async def test_early_stop_cancels_in_flight_calls() -> None:
    tracker = Tracker(delay=0.2)
    await AsyncStream.iterate(0, lambda n: n + 1).map_concurrent(tracker, limit=4).take(1).to_list()
    await asyncio.sleep(0.01)
    assert tracker.inflight == 0


async def test_breaking_out_leaves_nothing_running() -> None:
    tracker = Tracker(delay=0.2)
    stream = AsyncStream.iterate(0, lambda n: n + 1).map_concurrent(tracker, limit=4)
    async for _ in stream:
        break
    await asyncio.sleep(0.01)
    assert tracker.inflight == 0


async def test_outer_cancellation_propagates() -> None:
    tracker = Tracker(delay=10)
    task = asyncio.create_task(
        AsyncStream.from_iterable(range(10)).map_concurrent(tracker, limit=3).to_list()
    )
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0.01)
    assert tracker.inflight == 0


# --- exceptions ---------------------------------------------------------


async def test_a_failing_mapper_propagates_the_original_exception() -> None:
    """Not wrapped in an ExceptionGroup -- callers catch what they threw."""

    async def boom(n: int) -> int:
        await asyncio.sleep(0.01)
        if n == 2:
            raise ValueError(f"boom {n}")
        return n

    with pytest.raises(ValueError, match="boom 2"):
        await AsyncStream.from_iterable(range(6)).map_concurrent(boom, limit=3).to_list()


async def test_a_failing_mapper_cancels_its_siblings() -> None:
    state = Tracker(delay=0.2)

    async def boom(n: int) -> int:
        if n == 0:
            await asyncio.sleep(0.01)
            raise RuntimeError("first one fails")
        return await state(n)

    with pytest.raises(RuntimeError, match="first one fails"):
        await AsyncStream.from_iterable(range(10)).map_concurrent(boom, limit=4).to_list()
    await asyncio.sleep(0.01)
    assert state.inflight == 0


async def test_unordered_also_propagates_failures() -> None:
    async def boom(n: int) -> int:
        await asyncio.sleep(0.01)
        raise ValueError("nope")

    with pytest.raises(ValueError, match="nope"):
        await (
            AsyncStream.from_iterable(range(4))
            .map_concurrent(boom, limit=2, ordered=False)
            .to_list()
        )


async def test_a_failing_source_propagates() -> None:
    async def broken() -> AsyncIterator[int]:
        yield 1
        raise RuntimeError("source failed")

    with pytest.raises(RuntimeError, match="source failed"):
        await AsyncStream(broken()).map_concurrent(Tracker(), limit=2).to_list()


# --- composition --------------------------------------------------------


async def test_composes_with_other_ops() -> None:
    tracker = Tracker()
    result = await (
        AsyncStream.from_iterable(range(20))
        .filter(lambda n: n % 2 == 0)
        .map_concurrent(tracker, limit=4)
        .take(3)
        .to_list()
    )
    assert result == [0, 20, 40]


async def test_chained_concurrent_maps() -> None:
    async def add_one(n: int) -> int:
        await asyncio.sleep(0.001)
        return n + 1

    result = await (
        AsyncStream.from_iterable(range(5))
        .map_concurrent(add_one, limit=3)
        .map_concurrent(add_one, limit=3)
        .to_list()
    )
    assert result == [2, 3, 4, 5, 6]
