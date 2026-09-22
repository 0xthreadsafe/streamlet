"""Constructors: ``Stream.of``, ``Stream.from_iterable``, ``Stream.empty``."""

from itertools import count

from hypothesis import given
from hypothesis import strategies as st

from pystreamlet import Stream

ints = st.lists(st.integers())


@given(ints)
def test_of_wraps_its_arguments(items: list[int]) -> None:
    assert Stream.of(*items).to_list() == items


@given(ints)
def test_from_iterable_matches_the_constructor(items: list[int]) -> None:
    assert Stream.from_iterable(items).to_list() == Stream(items).to_list()


def test_of_treats_an_iterable_as_a_single_item() -> None:
    """``of`` never unpacks -- that is what ``from_iterable`` is for."""
    assert Stream.of([1, 2, 3]).to_list() == [[1, 2, 3]]
    assert Stream.from_iterable([1, 2, 3]).to_list() == [1, 2, 3]


def test_of_with_no_arguments_is_empty() -> None:
    assert Stream.of().to_list() == []


def test_empty_has_no_items() -> None:
    assert Stream.empty().to_list() == []


def test_empty_supports_the_full_api() -> None:
    empty: Stream[int] = Stream.empty()
    assert empty.map(lambda n: n * 2).filter(lambda n: n > 0).to_list() == []


def test_from_iterable_stays_lazy() -> None:
    assert Stream.from_iterable(count()).take(3).to_list() == [0, 1, 2]


def test_from_iterable_accepts_a_generator() -> None:
    assert Stream.from_iterable(n * 2 for n in range(3)).to_list() == [0, 2, 4]


def test_constructors_are_single_use_like_the_class() -> None:
    stream = Stream.of(1, 2, 3)
    assert stream.to_list() == [1, 2, 3]
    assert repr(stream) == "<Stream consumed>"
