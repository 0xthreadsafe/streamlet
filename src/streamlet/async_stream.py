"""Lazy asynchronous streams.

``AsyncStream`` mirrors :class:`~streamlet.stream.Stream`, but pulls from an
async source and is driven with ``async for`` / ``await``. Every operation
that takes a function accepts either a plain function or a coroutine
function, so ``map(str)`` and ``map(fetch)`` both work.
"""

from __future__ import annotations

import asyncio
import builtins
from collections import defaultdict, deque
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Hashable,
    Iterable,
)
from typing import Any, Generic, TypeVar, cast, overload

from streamlet.stream import StreamConsumedError

T = TypeVar("T")
R = TypeVar("R")
V = TypeVar("V")
K = TypeVar("K", bound=Hashable)
H = TypeVar("H", bound=Hashable)
Num = TypeVar("Num", int, float, complex)

MaybeAsync = Callable[[T], "R | Awaitable[R]"]
MaybeAsync2 = Callable[[R, T], "R | Awaitable[R]"]


async def _cancel_all(tasks: Iterable[asyncio.Task[Any]]) -> None:
    """Cancel every task and wait for the cancellations to settle."""
    remaining = [task for task in tasks if not task.done()]
    for task in remaining:
        task.cancel()
    if remaining:
        await asyncio.gather(*remaining, return_exceptions=True)


async def _resolve(value: R | Awaitable[R]) -> R:
    """Await ``value`` if it is awaitable, otherwise return it unchanged."""
    if isinstance(value, Awaitable):
        return await cast("Awaitable[R]", value)
    return value


class AsyncStream(Generic[T]):
    """A lazy, single-use stream over an async source.

    Nothing runs until a terminal op is awaited::

        total = await AsyncStream.from_iterable(urls).map(fetch).count()

    Like :class:`~streamlet.stream.Stream`, a stream is consumed once;
    iterating a second time raises :class:`StreamConsumedError`.
    """

    __slots__ = ("__weakref__", "_consumed", "_iterator")

    def __init__(self, source: AsyncIterable[T]) -> None:
        """Wrap an async source, without pulling anything from it yet."""
        self._iterator: AsyncIterator[T] = source.__aiter__()
        self._consumed = False

    def __aiter__(self) -> AsyncIterator[T]:
        """Return the underlying async iterator, marking the stream consumed.

        Raises:
            StreamConsumedError: if the stream has already been iterated.

        """
        if self._consumed:
            raise StreamConsumedError("this stream has already been consumed")
        self._consumed = True
        return self._iterator

    def __repr__(self) -> str:
        """Show the class and whether the stream still has items to give."""
        state = "consumed" if self._consumed else "lazy"
        return f"<{type(self).__name__} {state}>"

    # --- constructors ---------------------------------------------------

    @classmethod
    def of(cls, *items: T) -> AsyncStream[T]:
        """Build a stream from individual arguments."""
        return cls.from_iterable(items)

    @classmethod
    def from_iterable(cls, iterable: Iterable[T]) -> AsyncStream[T]:
        """Build a stream from a plain synchronous iterable."""

        async def generate() -> AsyncIterator[T]:
            for item in iterable:
                yield item

        return cls(generate())

    @classmethod
    def from_async_iterable(cls, source: AsyncIterable[T]) -> AsyncStream[T]:
        """Build a stream from an existing async iterable."""
        return cls(source)

    @classmethod
    def empty(cls) -> AsyncStream[T]:
        """Build a stream with no items."""
        return cls.from_iterable(())

    @classmethod
    def iterate(cls, seed: T, fn: MaybeAsync[T, T]) -> AsyncStream[T]:
        """Infinite stream: ``seed``, ``fn(seed)``, ``fn(fn(seed))``, ..."""

        async def generate() -> AsyncIterator[T]:
            current = seed
            while True:
                yield current
                current = await _resolve(fn(current))

        return cls(generate())

    @classmethod
    def generate(cls, fn: Callable[[], T | Awaitable[T]]) -> AsyncStream[T]:
        """Infinite stream produced by repeated calls to ``fn``."""

        async def produce() -> AsyncIterator[T]:
            while True:
                yield await _resolve(fn())

        return cls(produce())

    @classmethod
    def concat(cls, *sources: Iterable[T] | AsyncIterable[T]) -> AsyncStream[T]:
        """Join sources end to end, lazily."""

        async def generate() -> AsyncIterator[T]:
            for source in sources:
                if isinstance(source, AsyncIterable):
                    async for item in source:
                        yield item
                else:
                    for item in source:
                        yield item

        return cls(generate())

    # --- intermediate ops -----------------------------------------------

    def __or__(self, fn: Callable[[AsyncStream[T]], R]) -> R:
        """Pipe this stream into ``fn`` -- ``stream | fn`` means ``fn(stream)``."""
        return fn(self)

    @overload
    def map(self, mapper: Callable[[T], Awaitable[R]]) -> AsyncStream[R]: ...

    @overload
    def map(self, mapper: Callable[[T], R]) -> AsyncStream[R]: ...

    def map(self, mapper: MaybeAsync[T, R]) -> AsyncStream[R]:
        """Apply ``mapper`` to every item, one at a time::

            await AsyncStream.of("a", "b").map(fetch).to_list()

        ``mapper`` may be a plain function or a coroutine function; an
        awaitable result is awaited before it is yielded.

        See :meth:`map_concurrent` to run an async mapper over several items
        at once.
        """

        async def generate() -> AsyncIterator[R]:
            async for item in self:
                yield await _resolve(mapper(item))

        return AsyncStream(generate())

    def map_concurrent(
        self,
        mapper: Callable[[T], Awaitable[R]],
        *,
        limit: int = 8,
        ordered: bool = True,
    ) -> AsyncStream[R]:
        """Apply an async ``mapper`` to several items at once.

        At most ``limit`` calls are in flight at any moment, and items are
        only pulled from the source as slots free up -- so this stays lazy and
        works on infinite sources.

        With ``ordered=True`` (the default) results come out in source order,
        which costs some latency: a slow item holds back the ones behind it.
        With ``ordered=False`` each result is yielded as soon as it is ready.

        If the mapper raises, the exception propagates as-is and every
        in-flight call is cancelled. Abandoning the stream early -- a
        ``take``, a ``break`` -- cancels them too.

        Args:
            mapper: an async callable applied to each item.
            limit: maximum number of concurrent calls. Must be at least 1.
            ordered: whether to preserve source order.

        Raises:
            ValueError: if ``limit`` is less than 1.

        """
        if limit < 1:
            raise ValueError(f"map_concurrent() requires a limit of at least 1, got {limit}")

        generate = self._map_ordered if ordered else self._map_unordered
        return AsyncStream(generate(mapper, limit))

    async def _map_ordered(
        self, mapper: Callable[[T], Awaitable[R]], limit: int
    ) -> AsyncIterator[R]:
        """Sliding window of in-flight tasks, yielded in source order."""
        window: deque[asyncio.Task[R]] = deque()
        try:
            async for item in self:
                window.append(asyncio.ensure_future(mapper(item)))
                if len(window) >= limit:
                    yield await window.popleft()
            while window:
                yield await window.popleft()
        finally:
            await _cancel_all(window)

    async def _map_unordered(
        self, mapper: Callable[[T], Awaitable[R]], limit: int
    ) -> AsyncIterator[R]:
        """Same window, but each result is yielded as soon as it is ready."""
        pending: set[asyncio.Task[R]] = set()
        source_done = False
        iterator = self.__aiter__()
        try:
            while True:
                while not source_done and len(pending) < limit:
                    try:
                        item = await iterator.__anext__()
                    except StopAsyncIteration:
                        source_done = True
                    else:
                        pending.add(asyncio.ensure_future(mapper(item)))
                if not pending:
                    return
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    yield task.result()
        finally:
            await _cancel_all(pending)

    def filter(self, predicate: MaybeAsync[T, bool]) -> AsyncStream[T]:
        """Keep only items where ``predicate`` is true."""

        async def generate() -> AsyncIterator[T]:
            async for item in self:
                if await _resolve(predicate(item)):
                    yield item

        return AsyncStream(generate())

    def flat_map(self, fn: MaybeAsync[T, Iterable[R]]) -> AsyncStream[R]:
        """Map each item to an iterable and flatten one level."""

        async def generate() -> AsyncIterator[R]:
            async for item in self:
                for result in await _resolve(fn(item)):
                    yield result

        return AsyncStream(generate())

    def distinct(self) -> AsyncStream[T]:
        """Yield items the first time they are seen, preserving order."""

        async def generate() -> AsyncIterator[T]:
            seen: set[T] = set()
            async for item in self:
                if item not in seen:
                    seen.add(item)
                    yield item

        return AsyncStream(generate())

    def take(self, n: int) -> AsyncStream[T]:
        """Yield at most the first ``n`` items.

        Raises:
            ValueError: if ``n`` is negative.

        """
        if n < 0:
            raise ValueError(f"take() requires a non-negative count, got {n}")

        async def generate() -> AsyncIterator[T]:
            if n == 0:
                return
            taken = 0
            async for item in self:
                yield item
                taken += 1
                if taken >= n:
                    return

        return AsyncStream(generate())

    def skip(self, n: int) -> AsyncStream[T]:
        """Discard the first ``n`` items.

        Raises:
            ValueError: if ``n`` is negative.

        """
        if n < 0:
            raise ValueError(f"skip() requires a non-negative count, got {n}")

        async def generate() -> AsyncIterator[T]:
            skipped = 0
            async for item in self:
                if skipped < n:
                    skipped += 1
                    continue
                yield item

        return AsyncStream(generate())

    def take_while(self, predicate: MaybeAsync[T, bool]) -> AsyncStream[T]:
        """Yield items until ``predicate`` first fails, then stop."""

        async def generate() -> AsyncIterator[T]:
            async for item in self:
                if not await _resolve(predicate(item)):
                    return
                yield item

        return AsyncStream(generate())

    def drop_while(self, predicate: MaybeAsync[T, bool]) -> AsyncStream[T]:
        """Discard items until ``predicate`` first fails, then yield the rest."""

        async def generate() -> AsyncIterator[T]:
            dropping = True
            async for item in self:
                if dropping and await _resolve(predicate(item)):
                    continue
                dropping = False
                yield item

        return AsyncStream(generate())

    def peek(self, action: MaybeAsync[T, object]) -> AsyncStream[T]:
        """Run ``action`` on each item as it passes, yielding it unchanged."""

        async def generate() -> AsyncIterator[T]:
            async for item in self:
                await _resolve(action(item))
                yield item

        return AsyncStream(generate())

    # --- terminal ops ---------------------------------------------------

    async def to_list(self) -> list[T]:
        """Materialise the stream into a list."""
        return [item async for item in self]

    async def to_tuple(self) -> tuple[T, ...]:
        """Materialise the stream into a tuple."""
        return tuple(await self.to_list())

    async def to_set(self: AsyncStream[H]) -> set[H]:
        """Collect the items into a set. Only for hashable items."""
        return {item async for item in self}

    @overload
    async def to_dict(self, key: Callable[[T], K]) -> dict[K, T]: ...

    @overload
    async def to_dict(self, key: Callable[[T], K], value: Callable[[T], V]) -> dict[K, V]: ...

    async def to_dict(
        self,
        key: Callable[[T], K],
        value: Callable[[T], V] | None = None,
    ) -> dict[K, T] | dict[K, V]:
        """Collect into a dict keyed by ``key``. Later items win."""
        if value is None:
            return {key(item): item async for item in self}
        return {key(item): value(item) async for item in self}

    async def join(self: AsyncStream[str], separator: str = "") -> str:
        """Concatenate the items with ``separator``. Only for string streams."""
        return separator.join(await self.to_list())

    async def group_by(self, key: Callable[[T], K]) -> dict[K, list[T]]:
        """Group items by ``key``, consuming the stream."""
        groups: defaultdict[K, list[T]] = defaultdict(list)
        async for item in self:
            groups[key(item)].append(item)
        return dict(groups)

    async def count(self) -> int:
        """Count the items, consuming the stream."""
        total = 0
        async for _ in self:
            total += 1
        return total

    async def sum(self: AsyncStream[Num]) -> Num:
        """Add the items together. Only for numeric streams."""
        return builtins.sum(await self.to_list())

    async def min(self, key: Callable[[T], Any] | None = None) -> T | None:
        """Return the smallest item, or ``None`` if the stream is empty."""
        return builtins.min(await self.to_list(), key=key, default=None)  # type: ignore[type-var,arg-type]

    async def max(self, key: Callable[[T], Any] | None = None) -> T | None:
        """Return the largest item, or ``None`` if the stream is empty."""
        return builtins.max(await self.to_list(), key=key, default=None)  # type: ignore[type-var,arg-type]

    async def reduce(self, fn: MaybeAsync2[R, T], initial: R) -> R:
        """Fold the stream into a single value, starting from ``initial``."""
        accumulator = initial
        async for item in self:
            accumulator = await _resolve(fn(accumulator, item))
        return accumulator

    async def first(self) -> T | None:
        """Return the first item, or ``None`` if the stream is empty."""
        async for item in self:
            return item
        return None

    async def any(self, predicate: MaybeAsync[T, bool]) -> bool:
        """True if any item matches. Stops at the first match."""
        async for item in self:
            if await _resolve(predicate(item)):
                return True
        return False

    async def all(self, predicate: MaybeAsync[T, bool]) -> bool:
        """True if every item matches. Stops at the first failure."""
        async for item in self:
            if not await _resolve(predicate(item)):
                return False
        return True

    async def none(self, predicate: MaybeAsync[T, bool]) -> bool:
        """True if no item matches. Stops at the first match."""
        return not await self.any(predicate)

    async def for_each(self, action: MaybeAsync[T, object]) -> None:
        """Run ``action`` on every item, consuming the stream."""
        async for item in self:
            await _resolve(action(item))
