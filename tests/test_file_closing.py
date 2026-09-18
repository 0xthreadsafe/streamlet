"""Closing guarantees for ``Stream.from_file``.

These pin down exactly when the file handle is released -- including the
cases where it is *not*, which matter more than the happy path because they
are the ones that leak descriptors in production.
"""

import gc
import sys
import weakref
from pathlib import Path

import pytest

from streamlet import FileStream, Stream, StreamConsumedError

LINES = "a\nb\nc\nd\n"


@pytest.fixture
def log_file(tmp_path: Path) -> Path:
    path = tmp_path / "lines.txt"
    path.write_text(LINES, encoding="utf-8")
    return path


# --- deterministic closing ---------------------------------------------


def test_closes_when_exhausted(log_file: Path) -> None:
    stream = Stream.from_file(log_file)
    stream.to_list()
    assert stream.closed


def test_closes_when_a_with_block_exits(log_file: Path) -> None:
    with Stream.from_file(log_file) as stream:
        assert not stream.closed
    assert stream.closed


def test_closes_when_a_with_block_exits_after_an_early_stop(log_file: Path) -> None:
    with Stream.from_file(log_file) as stream:
        assert stream.first() == "a\n"
    assert stream.closed


def test_closes_when_breaking_out_inside_a_with_block(log_file: Path) -> None:
    with Stream.from_file(log_file) as stream:
        for _ in stream:
            break
    assert stream.closed


def test_closes_when_an_exception_escapes_iteration(log_file: Path) -> None:
    stream = Stream.from_file(log_file)
    with pytest.raises(RuntimeError):
        for _ in stream:
            raise RuntimeError("boom")
    assert stream.closed


def test_closes_when_an_exception_escapes_a_with_block(log_file: Path) -> None:
    stream = Stream.from_file(log_file)
    with pytest.raises(RuntimeError), stream:
        raise RuntimeError("boom")
    assert stream.closed


def test_closes_when_a_mapper_raises(log_file: Path) -> None:
    def explode(line: str) -> str:
        raise ValueError(line)

    stream = Stream.from_file(log_file)
    with pytest.raises(ValueError, match="a"):
        stream.map(explode).to_list()
    assert stream.closed


def test_close_is_idempotent(log_file: Path) -> None:
    stream = Stream.from_file(log_file)
    stream.close()
    stream.close()
    assert stream.closed


def test_explicit_close_stops_further_reading(log_file: Path) -> None:
    stream = Stream.from_file(log_file)
    stream.close()
    with pytest.raises(ValueError, match="closed file"):
        stream.to_list()


# --- closing without an explicit ``with`` -------------------------------


def test_early_break_closes_the_file(log_file: Path) -> None:
    """Breaking out throws GeneratorExit into __iter__, which closes."""
    stream = Stream.from_file(log_file)
    for _ in stream:
        break
    assert stream.closed


def test_a_partial_read_closes_the_file(log_file: Path) -> None:
    stream = Stream.from_file(log_file)
    assert stream.map(str.rstrip).take(1).to_list() == ["a"]
    assert stream.closed


def test_first_closes_the_file(log_file: Path) -> None:
    stream = Stream.from_file(log_file)
    assert stream.first() == "a\n"
    assert stream.closed


def test_early_break_releases_the_handle_after_collection(log_file: Path) -> None:
    stream = Stream.from_file(log_file)
    handle = stream._handle
    for _ in stream:
        break
    del stream
    gc.collect()
    assert handle.closed


def test_the_stream_is_not_kept_alive_by_a_reference_cycle(log_file: Path) -> None:
    stream = Stream.from_file(log_file)
    ref = weakref.ref(stream)
    stream.to_list()
    del stream
    gc.collect()
    assert ref() is None


# --- descriptor accounting ----------------------------------------------


def test_repeated_reads_do_not_leak_descriptors(log_file: Path) -> None:
    """A thousand full reads must not exhaust the descriptor table."""
    for _ in range(1000):
        Stream.from_file(log_file).to_list()
    assert Stream.from_file(log_file).count() == 4


def test_repeated_with_blocks_do_not_leak_descriptors(log_file: Path) -> None:
    for _ in range(1000):
        with Stream.from_file(log_file) as stream:
            stream.first()
    assert Stream.from_file(log_file).count() == 4


@pytest.mark.skipif(sys.platform == "win32", reason="fd numbering is POSIX-specific")
def test_the_underlying_descriptor_is_released(log_file: Path) -> None:
    """Not merely marked closed -- the OS-level fd is given back."""
    stream = Stream.from_file(log_file)
    fd = stream._handle.fileno()
    stream.to_list()
    with pytest.raises(ValueError, match="closed file"):
        stream._handle.fileno()
    reopened = Stream.from_file(log_file)
    assert reopened._handle.fileno() == fd
    reopened.close()


# --- interaction with the stream API ------------------------------------


def test_closing_does_not_bypass_single_use(log_file: Path) -> None:
    with Stream.from_file(log_file) as stream:
        stream.to_list()
        with pytest.raises(StreamConsumedError):
            stream.to_list()


def test_a_derived_stream_still_closes_the_source_file(log_file: Path) -> None:
    source = Stream.from_file(log_file)
    derived = source.map(str.rstrip).filter(lambda line: line != "b")
    assert derived.to_list() == ["a", "c", "d"]
    assert source.closed


def test_from_file_is_a_file_stream_and_a_stream(log_file: Path) -> None:
    stream = Stream.from_file(log_file)
    assert isinstance(stream, FileStream)
    assert isinstance(stream, Stream)
    stream.close()
