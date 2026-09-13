from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from itertools import islice
from typing import Generic, TypeVar

T = TypeVar("T")
R = TypeVar("R")


class StreamConsumedError(RuntimeError):
    """Raised when a stream is iterated more than once."""


class Stream(Generic[T]):
    """A lazy, single-use stream over an iterable.

    Wraps an iterator and defers all work until iteration begins.
    """

    __slots__ = ("_consumed", "_iterator")

    def __init__(self, iterable: Iterable[T]) -> None:
        self._iterator: Iterator[T] = iter(iterable)
        self._consumed = False

    def __iter__(self) -> Iterator[T]:
        if self._consumed:
            raise StreamConsumedError("this stream has already been consumed")
        self._consumed = True
        return self._iterator

    def __repr__(self) -> str:
        state: str = "consumed" if self._consumed else "lazy"
        return f"<{type(self).__name__} {state}>"

    def map(self, mapper: Callable[[T], R]) -> Stream[R]:
        """Apply ``mapper`` to every item."""
        return Stream(mapper(item) for item in self)

    def filter(self, predicate: Callable[[T], bool]) -> Stream[T]:
        """Keep only items where ``predicate`` is true."""
        return Stream(item for item in self if predicate(item))

    def take(self, n: int) -> Stream[T]:
        """Yield at most the first ``n`` items."""
        return Stream(islice(self, n))

    def skip(self, n: int) -> Stream[T]:
        """Discard the first ``n`` items."""
        return Stream(islice(self, n, None))

    def flat_map(self, fn: Callable[[T], Iterable[R]]) -> Stream[R]:
        """Map each item to an iterable and flatten one level."""
        return Stream(result for item in self for result in fn(item))

    def distinct(self) -> Stream[T]:
        """Yield items the first time they are seen, preserving order."""

        def generate() -> Iterator[T]:
            seen: set[T] = set()
            for item in self:
                if item not in seen:
                    seen.add(item)
                    yield item

        return Stream(generate())
