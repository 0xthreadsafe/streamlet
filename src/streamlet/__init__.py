"""Streamlet — a fluent, lazy stream-processing library for Python.

Start from :class:`~streamlet.stream.Stream` for synchronous pipelines and
:class:`~streamlet.async_stream.AsyncStream` for async ones::

    from streamlet import Stream

    Stream.from_iterable(range(100)).filter(lambda n: n % 2 == 0).take(5).to_list()

:class:`~streamlet.file_stream.ResourceStream` (and its ``FileStream``
specialisation) covers sources that hold a resource open.
"""

from streamlet.async_stream import AsyncStream
from streamlet.file_stream import ClosableIterable, FileStream, ResourceStream
from streamlet.stream import Stream, StreamConsumedError

__all__ = [
    "AsyncStream",
    "ClosableIterable",
    "FileStream",
    "ResourceStream",
    "Stream",
    "StreamConsumedError",
]


def main() -> None:
    """Entry point for the ``streamlet`` console script."""
    print("Hello from streamlet!")
