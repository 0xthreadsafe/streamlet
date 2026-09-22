"""The synchronous stream: :class:`Stream` and its ops.

A stream wraps an iterator and defers every operation until a terminal op
asks for items, so a chain reads in the order it runs and an infinite
source stays safe as long as something bounds it.
"""

from __future__ import annotations

import builtins
import functools
from collections import defaultdict
from collections.abc import Callable, Hashable, Iterable, Iterator
from itertools import chain, dropwhile, islice, takewhile
from os import PathLike
from typing import TYPE_CHECKING, Any, Generic, TypeVar, overload

if TYPE_CHECKING:
    from pystreamlet.file_stream import ClosableIterable, FileStream, ResourceStream

T = TypeVar("T")
R = TypeVar("R")
Num = TypeVar("Num", int, float, complex)
K = TypeVar("K", bound=Hashable)
H = TypeVar("H", bound=Hashable)
V = TypeVar("V")


class StreamConsumedError(RuntimeError):
    """Raised when a stream is iterated more than once.

    A stream is single-use, like the iterator underneath it. Rather than
    silently yielding nothing the second time, it raises -- which turns a
    quiet wrong answer into an obvious error.
    """


class Stream(Generic[T]):
    """A lazy, single-use stream over an iterable.

    Intermediate ops (:meth:`map`, :meth:`filter`, :meth:`take`, ...) return
    a new ``Stream`` and run nothing; a terminal op (:meth:`to_list`,
    :meth:`count`, :meth:`reduce`, ...) pulls items through the chain::

        Stream.of(1, 2, 3).map(lambda n: n * 2).to_list()  # [2, 4, 6]

    Being lazy is what makes an infinite source usable -- bound it with
    :meth:`take` or :meth:`take_while`. Being single-use is what makes the
    cost obvious: iterating twice raises :class:`StreamConsumedError`
    instead of quietly yielding nothing.

    Element types flow through a chain, so ``Stream.of(1, 2).map(str)`` is a
    ``Stream[str]``, and a few ops are restricted by self-type -- ``sum()``
    to numbers, ``join()`` to strings, ``to_set()`` to hashables.
    """

    __slots__ = ("_consumed", "_iterator")

    def __init__(self, iterable: Iterable[T]) -> None:
        """Wrap ``iterable``, without consuming anything from it yet.

        Ownership is unchanged: a handle passed here is still the caller's to
        close. Use :meth:`from_handle` to hand that duty over.
        """
        self._iterator: Iterator[T] = iter(iterable)
        self._consumed = False

    @classmethod
    def of(cls, *items: T) -> Stream[T]:
        """Build a stream from individual arguments::

            Stream.of(1, 2, 3)

        Note that a single iterable argument becomes one item, not the
        contents of that iterable -- use :meth:`from_iterable` for that.
        """
        return cls(items)

    @classmethod
    def from_iterable(cls, iterable: Iterable[T]) -> Stream[T]:
        """Build a stream from an existing iterable::

        Stream.from_iterable(range(10))
        """
        return cls(iterable)

    @classmethod
    def from_file(
        cls,
        path: str | PathLike[str],
        *,
        encoding: str = "utf-8",
        errors: str | None = None,
        newline: str | None = None,
    ) -> FileStream:
        """Stream the lines of a file, closing the handle when done.

        Returns a :class:`~pystreamlet.file_stream.FileStream`, which closes the
        handle on every exit path and is also a context manager::

            with Stream.from_file("app.log") as lines:
                first_error = lines.filter(lambda line: "ERROR" in line).first()

        Lines keep their trailing newline, matching ``open()``.
        """
        from pystreamlet.file_stream import open_file_stream

        return open_file_stream(path, encoding=encoding, errors=errors, newline=newline)

    @classmethod
    def from_handle(
        cls,
        handle: ClosableIterable[T],
        *,
        close: bool = True,
    ) -> ResourceStream[T]:
        """Stream a handle you already have, saying who closes it.

        ``Stream(handle)`` leaves the handle to whoever opened it, which is
        the right default but leaks it if nobody follows up. This transfers
        that duty explicitly::

            Stream.from_handle(sock.makefile())  # stream closes it
            Stream.from_handle(sys.stdin, close=False)  # caller keeps it

        With ``close=True`` (the default) the resource is closed on every exit
        path -- exhaustion, an early ``break``, an exception, or leaving the
        returned stream's ``with`` block. With ``close=False`` nothing is
        closed for you.

        Works for anything iterable with a ``close()``: socket files,
        ``os.popen`` pipes, ``io.StringIO``, database cursors. For a path,
        use :meth:`from_file` instead.

        Raises:
            TypeError: if the handle has no ``close()`` method.

        """
        from pystreamlet.file_stream import resource_stream

        return resource_stream(handle, close=close)

    @classmethod
    def iterate(cls, seed: T, fn: Callable[[T], T]) -> Stream[T]:
        """Build an infinite stream by repeatedly applying ``fn``.

        Yields ``seed``, ``fn(seed)``, ``fn(fn(seed))``, ... Pair it with
        :meth:`take` or :meth:`take_while` to bound it::

            Stream.iterate(1, lambda n: n * 2).take(5)  # 1, 2, 4, 8, 16
        """

        def generate() -> Iterator[T]:
            current = seed
            while True:
                yield current
                current = fn(current)

        return cls(generate())

    @classmethod
    def generate(cls, fn: Callable[[], T]) -> Stream[T]:
        """Build an infinite stream by calling ``fn`` for each item.

        ``fn`` takes no arguments, so it is only useful when it returns
        something different each call -- a counter, a random value, a read.
        """

        def produce() -> Iterator[T]:
            while True:
                yield fn()

        return cls(produce())

    @classmethod
    def concat(cls, *streams: Iterable[T]) -> Stream[T]:
        """Join iterables end to end, lazily.

        Later sources are not touched until the earlier ones are exhausted,
        so an infinite source anywhere makes everything after it unreachable.
        """
        return cls(chain.from_iterable(streams))

    @classmethod
    def empty(cls) -> Stream[T]:
        """Build a stream with no items."""
        return cls(())

    def __iter__(self) -> Iterator[T]:
        """Return the underlying iterator, marking the stream consumed.

        This is the single point where laziness ends: every terminal op goes
        through it, so every one of them is also what makes the stream
        single-use.

        Raises:
            StreamConsumedError: if the stream has already been iterated.

        """
        if self._consumed:
            raise StreamConsumedError("this stream has already been consumed")
        self._consumed = True
        return self._iterator

    def __repr__(self) -> str:
        """Show the class and whether the stream still has items to give.

        Deliberately says nothing about the contents: inspecting them would
        consume the very stream being inspected.
        """
        state: str = "consumed" if self._consumed else "lazy"
        return f"<{type(self).__name__} {state}>"

    def __or__(self, fn: Callable[[Stream[T]], R]) -> R:
        """Pipe this stream into ``fn`` -- ``stream | fn`` means ``fn(stream)``.

        Lets any function that accepts a ``Stream`` act as a pipeline stage::

            Stream(range(20)) | evens | (lambda s: s.take(3)) | list
        """
        return fn(self)

    def map(self, mapper: Callable[[T], R]) -> Stream[R]:
        """Apply ``mapper`` to every item, lazily::

            Stream.of(1, 2, 3).map(str).to_list()  # ["1", "2", "3"]

        ``mapper`` runs once per item that is actually pulled, not once per
        item in the source.
        """
        return Stream(mapper(item) for item in self)

    def filter(self, predicate: Callable[[T], bool]) -> Stream[T]:
        """Keep only items where ``predicate`` is true::

        Stream(range(6)).filter(lambda n: n % 2 == 0).to_list()  # [0, 2, 4]
        """
        return Stream(item for item in self if predicate(item))

    def take(self, n: int) -> Stream[T]:
        """Yield at most the first ``n`` items.

        Raises:
            ValueError: if ``n`` is negative.

        """
        if n < 0:
            raise ValueError(f"take() requires a non-negative count, got {n}")
        return Stream(islice(self, n))

    def skip(self, n: int) -> Stream[T]:
        """Discard the first ``n`` items.

        Raises:
            ValueError: if ``n`` is negative.

        """
        if n < 0:
            raise ValueError(f"skip() requires a non-negative count, got {n}")
        return Stream(islice(self, n, None))

    def take_while(self, predicate: Callable[[T], bool]) -> Stream[T]:
        """Yield items until ``predicate`` first fails, then stop.

        Unlike :meth:`filter`, this stops at the first failure instead of
        skipping it and carrying on.
        """
        return Stream(takewhile(predicate, self))

    def drop_while(self, predicate: Callable[[T], bool]) -> Stream[T]:
        """Discard items until ``predicate`` first fails, then yield the rest.

        Once dropping stops it never resumes, even if later items would
        satisfy ``predicate`` again.
        """
        return Stream(dropwhile(predicate, self))

    def peek(self, action: Callable[[T], object]) -> Stream[T]:
        """Run ``action`` on each item as it passes, yielding it unchanged.

        Intended for debugging and logging. Because the stream is lazy,
        ``action`` never runs for items that are not pulled.
        """

        def generate() -> Iterator[T]:
            for item in self:
                action(item)
                yield item

        return Stream(generate())

    def flat_map(self, fn: Callable[[T], Iterable[R]]) -> Stream[R]:
        """Map each item to an iterable and flatten one level::

            Stream.of("ab", "cd").flat_map(list).to_list()  # ["a", "b", "c", "d"]

        One level only -- a stream of lists of lists stays nested inside.
        """
        return Stream(result for item in self for result in fn(item))

    def sorted(
        self,
        key: Callable[[T], Any] | None = None,
        *,
        reverse: bool = False,
    ) -> Stream[T]:
        """Sort the items, optionally by ``key``.

        This op must read the whole stream before it can emit anything, so it
        buffers every item in memory and never finishes on an infinite source.

        Raises:
            TypeError: if the items (or their keys) are not comparable.

        """
        return Stream(sorted(self, key=key, reverse=reverse))  # type: ignore[type-var,arg-type]

    def reverse(self) -> Stream[T]:
        """Reverse the item order.

        Like :meth:`sorted`, this buffers the whole stream and so never
        finishes on an infinite source.
        """
        return Stream(reversed(self.to_list()))

    def distinct(self) -> Stream[T]:
        """Yield items the first time they are seen, preserving order."""

        def generate() -> Iterator[T]:
            seen: set[T] = set()
            for item in self:
                if item not in seen:
                    seen.add(item)
                    yield item

        return Stream(generate())

    def min(self, key: Callable[[T], Any] | None = None) -> T | None:
        """Return the smallest item, or ``None`` if the stream is empty.

        Raises:
            TypeError: if the items (or their keys) are not comparable.

        """
        return builtins.min(self, key=key, default=None)  # type: ignore[type-var,arg-type]

    def max(self, key: Callable[[T], Any] | None = None) -> T | None:
        """Return the largest item, or ``None`` if the stream is empty.

        Raises:
            TypeError: if the items (or their keys) are not comparable.

        """
        return builtins.max(self, key=key, default=None)  # type: ignore[type-var,arg-type]

    def none(self, predicate: Callable[[T], bool]) -> bool:
        """True if no item matches. Stops at the first match."""
        return not builtins.any(predicate(item) for item in self)

    def for_each(self, action: Callable[[T], object]) -> None:
        """Run ``action`` on every item, consuming the stream."""
        for item in self:
            action(item)

    def to_tuple(self) -> tuple[T, ...]:
        """Materialise the stream into a tuple."""
        return tuple(self)

    def to_set(self: Stream[H]) -> set[H]:
        """Collect the items into a set. Only available for hashable items."""
        return set(self)

    @overload
    def to_dict(self, key: Callable[[T], K]) -> dict[K, T]: ...

    @overload
    def to_dict(self, key: Callable[[T], K], value: Callable[[T], V]) -> dict[K, V]: ...

    def to_dict(
        self,
        key: Callable[[T], K],
        value: Callable[[T], V] | None = None,
    ) -> dict[K, T] | dict[K, V]:
        """Collect into a dict keyed by ``key``.

        ``value`` defaults to the item itself. When two items produce the same
        key the later one wins, matching ``dict`` construction.
        """
        if value is None:
            return {key(item): item for item in self}
        return {key(item): value(item) for item in self}

    def join(self: Stream[str], separator: str = "") -> str:
        """Concatenate the items with ``separator``. Only for string streams."""
        return separator.join(self)

    def group_by(self, key: Callable[[T], K]) -> dict[K, list[T]]:
        """Group items by ``key``, consuming the stream.

        Returns a plain ``dict`` mapping each key to the items that produced
        it, with keys in first-seen order and members in stream order.

        Unlike :func:`itertools.groupby`, items do not need to be adjacent --
        every item sharing a key lands in the same group.

        This is a terminal op and reads the whole stream, so it never
        finishes on an infinite source.
        """
        groups: defaultdict[K, list[T]] = defaultdict(list)
        for item in self:
            groups[key(item)].append(item)
        return dict(groups)

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
        """Fold the stream into a single value, starting from ``initial``::

            Stream.of(1, 2, 3).reduce(lambda total, n: total + n, 0)  # 6

        ``initial`` is required, which is what lets the result type differ
        from the item type and removes the empty-stream special case.
        """
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
