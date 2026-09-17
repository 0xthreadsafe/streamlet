from __future__ import annotations

import builtins
import functools
from collections.abc import Callable, Iterable, Iterator
from itertools import islice
from typing import Generic, TypeVar

T = TypeVar("T")
R = TypeVar("R")
Num = TypeVar("Num", int, float, complex)


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

    def to_list(self) -> list[T]:
        """Materialise the stream into a list."""
        return list(self)

    def count(self) -> int:
        """Count the items, consuming the stream."""
        return builtins.sum(1 for _ in self)

    def sum(self: Stream[Num]) -> Num:
        """Add the items together. Only available on numeric streams."""
        return builtins.sum(self)

    def reduce(self, fn: Callable[[R, T], R], initial: R) -> R:
        """Fold the stream into a single value, starting from ``initial``."""
        return functools.reduce(fn, self, initial)

    def first(self) -> T | None:
        """Return the first item, or ``None`` if the stream is empty."""
        return next(iter(self), None)

    def any(self, predicate: Callable[[T], bool]) -> bool:
        """True if any item matches. Stops at the first match."""
        return builtins.any(predicate(item) for item in self)

    def all(self, predicate: Callable[[T], bool]) -> bool:
        """True if every item matches. Stops at the first failure."""
        return builtins.all(predicate(item) for item in self)
