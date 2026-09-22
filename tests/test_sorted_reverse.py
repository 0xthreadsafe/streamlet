"""Buffering intermediate ops: sorted and reverse."""

from itertools import pairwise

import pytest
from hypothesis import given
from hypothesis import strategies as st

from pystreamlet import Stream, StreamConsumedError

ints = st.lists(st.integers())
words = st.lists(st.text())


@given(ints)
def test_sorted_matches_builtin(items: list[int]) -> None:
    assert Stream(items).sorted().to_list() == sorted(items)


@given(ints)
def test_sorted_reverse_matches_builtin(items: list[int]) -> None:
    assert Stream(items).sorted(reverse=True).to_list() == sorted(items, reverse=True)


@given(words)
def test_sorted_with_key_matches_builtin(items: list[str]) -> None:
    assert Stream(items).sorted(key=len).to_list() == sorted(items, key=len)


@given(ints)
def test_sorted_is_a_permutation_of_the_input(items: list[int]) -> None:
    assert sorted(Stream(items).sorted().to_list()) == sorted(items)


@given(ints)
def test_sorted_is_non_decreasing(items: list[int]) -> None:
    result = Stream(items).sorted().to_list()
    assert all(a <= b for a, b in pairwise(result))


@given(words)
def test_sorted_is_stable(items: list[str]) -> None:
    """Equal keys keep their original relative order."""
    assert Stream(items).sorted(key=len).to_list() == sorted(items, key=len)


@given(ints)
def test_reverse_matches_slicing(items: list[int]) -> None:
    assert Stream(items).reverse().to_list() == items[::-1]


@given(ints)
def test_reverse_twice_is_the_identity(items: list[int]) -> None:
    assert Stream(items).reverse().reverse().to_list() == items


def test_sorted_and_reverse_on_an_empty_stream() -> None:
    assert Stream.empty().sorted().to_list() == []
    assert Stream.empty().reverse().to_list() == []


def test_sorted_composes_with_lazy_ops() -> None:
    result = Stream.of(5, 3, 9, 1).sorted().map(lambda n: n * 2).take(2).to_list()
    assert result == [2, 6]


def test_reverse_composes_with_lazy_ops() -> None:
    assert Stream.of(1, 2, 3, 4).reverse().take(2).to_list() == [4, 3]


def test_sorted_consumes_the_source() -> None:
    source = Stream.of(3, 1, 2)
    source.sorted()
    with pytest.raises(StreamConsumedError):
        source.to_list()


def test_sorted_requires_comparable_items_without_a_key() -> None:
    with pytest.raises(TypeError):
        Stream.of(1, "a").sorted().to_list()
