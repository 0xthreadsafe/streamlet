"""Context-managed streams over resources that need closing."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from os import PathLike
from pathlib import Path
from types import TracebackType
from typing import IO, Protocol, Self, TypeVar, runtime_checkable

from pystreamlet.stream import Stream

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


@runtime_checkable
class ClosableIterable(Protocol[T_co]):
    """Anything iterable that also needs closing.

    Files satisfy it, and so do sockets opened with ``makefile()``,
    ``os.popen()`` pipes, ``io.StringIO`` buffers, and database cursors.
    """

    def __iter__(self) -> Iterator[T_co]: ...

    def close(self) -> None:
        """Release the resource."""
        ...


class _NoClose(Iterator[T]):
    """An iterator with no ``close()``, wrapped around one that has it.

    ``yield from`` closes whatever it delegates to, which is exactly wrong
    for a resource the stream does not own. Hiding ``close()`` behind this
    wrapper keeps an unowned resource open.
    """

    __slots__ = ("_iterator",)

    def __init__(self, iterable: Iterable[T]) -> None:
        self._iterator = iter(iterable)

    def __iter__(self) -> Iterator[T]:
        return self

    def __next__(self) -> T:
        return next(self._iterator)


class ResourceStream(Stream[T]):
    """A lazy stream over a resource, with explicit ownership.

    With ``close=True`` the stream owns the resource and closes it on every
    exit path: exhausting the stream, stopping early with a ``break`` or a
    partial read, an exception escaping iteration, or leaving a ``with``
    block. With ``close=False`` the resource is never closed for you --
    whoever opened it stays responsible for it.

    Reach for this through :meth:`Stream.from_handle`; use
    :meth:`Stream.from_file` when you have a path rather than a handle.
    """

    __slots__ = ("__weakref__", "_owned", "_resource")

    def __init__(self, resource: ClosableIterable[T], *, close: bool = True) -> None:
        self._resource = resource
        self._owned = close
        super().__init__(self._drain(resource) if close else _NoClose(resource))

    @staticmethod
    def _drain(resource: ClosableIterable[T]) -> Iterator[T]:
        try:
            yield from resource
        finally:
            resource.close()

    def __iter__(self) -> Iterator[T]:
        """Iterate the items, closing an owned resource if anything goes wrong.

        The generator's own ``finally`` covers exhaustion, but an exception
        raised in the *caller's* loop body only suspends the generator -- it
        does not finalise it. Closing here covers that case too.
        """
        iterator = super().__iter__()
        try:
            yield from iterator
        except BaseException:
            if self._owned:
                self.close()
            raise

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._owned:
            self.close()

    @property
    def owned(self) -> bool:
        """Whether this stream is responsible for closing the resource."""
        return self._owned

    def close(self) -> None:
        """Close the underlying resource. Safe to call more than once.

        Explicit, so it closes even a resource the stream does not own.
        """
        self._resource.close()


class FileStream(ResourceStream[str]):
    """A lazy stream of lines that owns its file handle.

    The handle is closed on every exit path: exhausting the stream, stopping
    early with a ``break`` or a partial read, an exception escaping iteration,
    or leaving a ``with`` block.

    ``with`` is still the clearest way to express the intent, and it closes
    the file even if you never iterate at all::

        with Stream.from_file("app.log") as lines:
            first_error = lines.filter(lambda line: "ERROR" in line).first()

    Lines keep their trailing newline, matching ``open()``. Use
    ``.map(str.rstrip)`` if you want them stripped.
    """

    __slots__ = ("_handle",)

    def __init__(self, handle: IO[str]) -> None:
        self._handle = handle
        super().__init__(handle)

    @property
    def closed(self) -> bool:
        """Whether the underlying file has been closed."""
        return self._handle.closed


def open_file_stream(
    path: str | PathLike[str],
    *,
    encoding: str = "utf-8",
    errors: str | None = None,
    newline: str | None = None,
) -> FileStream:
    """Open ``path`` for reading and return a :class:`FileStream` of its lines."""
    # SIM115: the returned FileStream owns this handle and closes it on every
    # exit path -- exhaustion, exception, or leaving its ``with`` block.
    handle = Path(path).open(encoding=encoding, errors=errors, newline=newline)  # noqa: SIM115
    return FileStream(handle)


def resource_stream(
    resource: ClosableIterable[T] | Iterable[T],
    *,
    close: bool = True,
) -> ResourceStream[T]:
    """Wrap ``resource`` in a :class:`ResourceStream`.

    Raises:
        TypeError: if the resource has no ``close()`` method.

    """
    if not isinstance(resource, ClosableIterable):
        raise TypeError(
            f"from_handle() needs an iterable with a close() method, got {type(resource).__name__}"
        )
    return ResourceStream(resource, close=close)
