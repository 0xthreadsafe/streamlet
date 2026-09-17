"""Streamlet — a fluent, lazy stream-processing library for Python."""

from streamlet.file_stream import FileStream
from streamlet.stream import Stream, StreamConsumedError

__all__ = ["FileStream", "Stream", "StreamConsumedError"]


def main() -> None:
    print("Hello from streamlet!")
