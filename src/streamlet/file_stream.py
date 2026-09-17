"""Context-managed streams over file contents."""

from __future__ import annotations

from collections.abc import Iterator
from os import PathLike
from pathlib import Path
from types import TracebackType
from typing import IO

from streamlet.stream import Stream


class FileStream(Stream[str]):
    """A lazy stream of lines that owns its file handle.

    The handle is closed when the stream is exhausted, when an exception
    escapes iteration, or when a ``with`` block exits.

    If you stop early *without* ``with`` -- a ``break``, a ``first()`` -- the
    file stays open until the stream object itself is garbage collected, which
    is not a moment you control. Use ``with`` whenever you might not read to
    the end::

        with Stream.from_file("app.log") as lines:
            first_error = lines.filter(lambda line: "ERROR" in line).first()

    Lines keep their trailing newline, matching ``open()``. Use
    ``.map(str.rstrip)`` if you want them stripped.
    """

    __slots__ = ("_handle",)

    def __init__(self, handle: IO[str]) -> None:
        self._handle = handle
        super().__init__(self._drain(handle))

    @staticmethod
    def _drain(handle: IO[str]) -> Iterator[str]:
        try:
            yield from handle
        finally:
            handle.close()

    def __enter__(self) -> FileStream:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying file. Safe to call more than once."""
        self._handle.close()

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
