"""Streamlet — a fluent, lazy stream-processing library for Python."""

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
    print("Hello from streamlet!")
