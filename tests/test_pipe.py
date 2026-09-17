"""``__or__`` pipe-chaining: ``stream | fn`` means ``fn(stream)``."""

import pytest
from hypothesis import given
from hypothesis import strategies as st

from streamlet import Stream, StreamConsumedError

ints = st.lists(st.integers())


def evens(stream: Stream[int]) -> Stream[int]:
    return stream.filter(lambda n: n % 2 == 0)


@given(ints)
def test_pipe_applies_the_function(items: list[int]) -> None:
    assert (Stream(items) | list) == items


@given(ints)
def test_pipe_matches_the_method_form(items: list[int]) -> None:
    piped = (Stream(items) | evens).to_list()
    chained = Stream(items).filter(lambda n: n % 2 == 0).to_list()
    assert piped == chained


@given(ints)
def test_pipes_chain_left_to_right(items: list[int]) -> None:
    result = Stream(items) | evens | (lambda s: s.map(str)) | list
    assert result == [str(n) for n in items if n % 2 == 0]


def test_pipe_into_a_builtin() -> None:
    assert (Stream(range(5)) | sum) == 10


def test_pipe_can_change_the_result_type() -> None:
    assert (Stream(range(5)) | (lambda s: s.count())) == 5


def test_pipe_is_lazy_until_the_last_stage() -> None:
    from itertools import count

    result = Stream(count()) | evens | (lambda s: s.take(3)) | list
    assert result == [0, 2, 4]


def test_pipe_consumes_the_source_stream() -> None:
    stream = Stream([1, 2, 3])
    _ = stream | list
    with pytest.raises(StreamConsumedError):
        stream.to_list()


def test_pipe_into_a_non_callable_raises() -> None:
    with pytest.raises(TypeError):
        _ = Stream([1, 2, 3]) | 5  # type: ignore[operator]
