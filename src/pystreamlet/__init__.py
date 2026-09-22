"""Streamlet — a fluent, lazy stream-processing library for Python.

Start from :class:`~pystreamlet.stream.Stream` for synchronous pipelines and
:class:`~pystreamlet.async_stream.AsyncStream` for async ones::

    from pystreamlet import Stream

    Stream.from_iterable(range(100)).filter(lambda n: n % 2 == 0).take(5).to_list()

:class:`~pystreamlet.file_stream.ResourceStream` (and its ``FileStream``
specialisation) covers sources that hold a resource open.
"""

from pystreamlet.async_stream import AsyncStream
from pystreamlet.file_stream import ClosableIterable, FileStream, ResourceStream
from pystreamlet.stream import Stream, StreamConsumedError

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
    print("Hello from pystreamlet!")
