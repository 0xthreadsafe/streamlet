"""Terminal ops: min, max, none, for_each."""

from itertools import count

import pytest
from hypothesis import given
from hypothesis import strategies as st

from pystreamlet import Stream, StreamConsumedError

ints = st.lists(st.integers())
words = st.lists(st.text())


@given(ints)
def test_min_matches_builtin(items: list[int]) -> None:
    assert Stream(items).min() == (min(items) if items else None)


@given(ints)
def test_max_matches_builtin(items: list[int]) -> None:
    assert Stream(items).max() == (max(items) if items else None)


@given(words)
def test_min_and_max_with_a_key(items: list[str]) -> None:
    assert Stream(items).min(key=len) == (min(items, key=len) if items else None)
    assert Stream(items).max(key=len) == (max(items, key=len) if items else None)


@given(st.lists(st.integers(), min_size=1))
def test_min_is_at_most_max(items: list[int]) -> None:
    smallest = Stream(items).min()
    largest = Stream(items).max()
    assert smallest is not None and largest is not None
    assert smallest <= largest


def test_min_and_max_return_none_on_an_empty_stream() -> None:
    assert Stream.empty().min() is None
    assert Stream.empty().max() is None


def test_min_and_max_return_the_first_of_equal_keys() -> None:
    assert Stream.of("bb", "aa").min(key=len) == "bb"
    assert Stream.of("bb", "aa").max(key=len) == "bb"


@given(ints)
def test_none_is_the_negation_of_any(items: list[int]) -> None:
    assert Stream(items).none(lambda n: n > 0) == (not Stream(items).any(lambda n: n > 0))


@given(ints)
def test_none_matches_builtin(items: list[int]) -> None:
    assert Stream(items).none(lambda n: n > 0) == (not any(n > 0 for n in items))


def test_none_is_true_for_an_empty_stream() -> None:
    assert Stream.empty().none(lambda n: True) is True


def test_none_short_circuits_on_an_infinite_source() -> None:
    assert Stream.from_iterable(count()).none(lambda n: n == 3) is False


@given(ints)
def test_for_each_visits_every_item_in_order(items: list[int]) -> None:
    seen: list[int] = []
    Stream(items).for_each(seen.append)
    assert seen == items


def test_for_each_consumes_the_stream() -> None:
    stream = Stream.of(1, 2, 3)
    stream.for_each(lambda n: None)
    with pytest.raises(StreamConsumedError):
        stream.to_list()


def test_for_each_on_an_empty_stream_does_nothing() -> None:
    seen: list[int] = []
    Stream[int].empty().for_each(seen.append)
    assert seen == []


def test_min_max_compose_after_intermediate_ops() -> None:
    assert Stream.from_iterable(range(10)).filter(lambda n: n % 3 == 0).max() == 9
