"""``Stream.from_handle``: explicit ownership for any closable iterable.

``Stream(handle)`` leaves the handle to its opener; ``from_handle`` lets that
duty be transferred on purpose, for files and for anything else that is
iterable and needs closing.
"""

from __future__ import annotations

import io
import os
from collections.abc import Iterator
from pathlib import Path
from typing import cast

import pytest

from streamlet import ClosableIterable, ResourceStream, Stream, StreamConsumedError


class Tracked:
    """An iterable that records whether it was closed."""

    def __init__(self, items: list[int]) -> None:
        self._items = items
        self.close_calls = 0

    def __iter__(self) -> Iterator[int]:
        yield from self._items

    def close(self) -> None:
        self.close_calls += 1

    @property
    def closed(self) -> bool:
        return self.close_calls > 0


# --- ownership ----------------------------------------------------------


def test_close_true_closes_on_exhaustion() -> None:
    handle = Tracked([1, 2, 3])
    assert Stream.from_handle(handle).to_list() == [1, 2, 3]
    assert handle.closed


def test_close_true_closes_on_an_early_stop() -> None:
    handle = Tracked([1, 2, 3])
    assert Stream.from_handle(handle).first() == 1
    assert handle.closed


def test_close_true_closes_on_a_break() -> None:
    handle = Tracked([1, 2, 3])
    for _ in Stream.from_handle(handle):
        break
    assert handle.closed


def test_close_true_closes_when_the_loop_body_raises() -> None:
    handle = Tracked([1, 2, 3])
    with pytest.raises(RuntimeError, match="body"):
        for _ in Stream.from_handle(handle):
            raise RuntimeError("body")
    assert handle.closed


def test_close_true_closes_on_leaving_the_with_block() -> None:
    handle = Tracked([1, 2, 3])
    with Stream.from_handle(handle) as stream:
        assert not handle.closed
        assert stream.to_list() == [1, 2, 3]
    assert handle.closed


def test_close_true_closes_even_without_iterating() -> None:
    handle = Tracked([1, 2, 3])
    with Stream.from_handle(handle):
        pass
    assert handle.closed


def test_close_false_never_closes() -> None:
    handle = Tracked([1, 2, 3])
    assert Stream.from_handle(handle, close=False).to_list() == [1, 2, 3]
    assert not handle.closed


def test_close_false_leaves_the_handle_open_on_every_path() -> None:
    handle = Tracked([1, 2, 3])
    with Stream.from_handle(handle, close=False) as stream:
        assert stream.first() == 1
    assert not handle.closed

    handle = Tracked([1, 2, 3])
    with pytest.raises(RuntimeError, match="body"):
        for _ in Stream.from_handle(handle, close=False):
            raise RuntimeError("body")
    assert not handle.closed


def test_plain_construction_still_leaves_the_handle_alone() -> None:
    handle = Tracked([1, 2, 3])
    assert Stream(handle).to_list() == [1, 2, 3]
    assert not handle.closed


def test_ownership_is_reported() -> None:
    handle = Tracked([1])
    assert Stream.from_handle(handle).owned is True
    assert Stream.from_handle(Tracked([1]), close=False).owned is False


def test_explicit_close_works_either_way() -> None:
    handle = Tracked([1, 2, 3])
    Stream.from_handle(handle, close=False).close()
    assert handle.close_calls == 1


def test_closing_twice_is_harmless() -> None:
    handle = Tracked([1, 2, 3])
    stream = Stream.from_handle(handle)
    stream.close()
    stream.close()
    assert handle.close_calls == 2


# --- typing and stream semantics ----------------------------------------


def test_returns_a_resource_stream() -> None:
    assert isinstance(Stream.from_handle(Tracked([1])), ResourceStream)


def test_is_a_full_stream() -> None:
    handle = Tracked([1, 2, 3, 4])
    result = Stream.from_handle(handle).filter(lambda n: n % 2 == 0).map(str).to_list()
    assert result == ["2", "4"]
    assert handle.closed


def test_is_still_single_use() -> None:
    stream = Stream.from_handle(Tracked([1, 2]))
    assert stream.to_list() == [1, 2]
    with pytest.raises(StreamConsumedError):
        stream.to_list()


def test_is_lazy() -> None:
    pulled: list[int] = []

    class Counting(Tracked):
        def __iter__(self) -> Iterator[int]:
            for n in range(100):
                pulled.append(n)
                yield n

    stream = Stream.from_handle(Counting([])).take(3)
    assert pulled == []
    assert stream.to_list() == [0, 1, 2]
    assert pulled == [0, 1, 2]


def test_rejects_an_iterable_with_no_close() -> None:
    with pytest.raises(TypeError, match="close"):
        Stream.from_handle([1, 2, 3])  # type: ignore[arg-type]


# --- real resources -----------------------------------------------------


def test_works_with_a_string_buffer() -> None:
    buffer = io.StringIO("a\nb\n")
    assert Stream.from_handle(buffer).map(str.rstrip).to_list() == ["a", "b"]
    assert buffer.closed


def test_close_false_keeps_a_string_buffer_readable() -> None:
    buffer = io.StringIO("a\nb\n")
    assert Stream.from_handle(buffer, close=False).first() == "a\n"
    assert not buffer.closed
    assert buffer.read() == "b\n"


def test_works_with_an_open_file() -> None:
    handle = io.StringIO("x\ny\n")
    with Stream.from_handle(handle) as lines:
        assert lines.count() == 2
    assert handle.closed


def test_works_with_a_popen_pipe() -> None:
    pipe = os.popen("printf 'one\\ntwo\\n'")
    handle = cast("ClosableIterable[str]", pipe)
    assert Stream.from_handle(handle).map(str.rstrip).to_list() == ["one", "two"]
    # _wrap_close has no .closed; closing twice would raise if it were still open.
    assert pipe.close() is None


def test_works_with_a_real_file_handle(tmp_path: Path) -> None:
    path = tmp_path / "data.txt"
    path.write_text("1\n2\n3\n", encoding="utf-8")

    handle = path.open(encoding="utf-8")
    assert Stream.from_handle(handle).map(str.rstrip).map(int).sum() == 6
    assert handle.closed
