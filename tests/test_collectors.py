"""Collector terminal ops: to_tuple, to_set, to_dict, join."""

import pytest
from hypothesis import given
from hypothesis import strategies as st

from pystreamlet import Stream, StreamConsumedError

ints = st.lists(st.integers())
words = st.lists(st.text())


@given(ints)
def test_to_tuple_matches_builtin(items: list[int]) -> None:
    assert Stream(items).to_tuple() == tuple(items)


@given(ints)
def test_to_set_matches_builtin(items: list[int]) -> None:
    assert Stream(items).to_set() == set(items)


@given(ints)
def test_to_set_equals_distinct_without_order(items: list[int]) -> None:
    assert Stream(items).to_set() == set(Stream(items).distinct().to_list())


@given(words)
def test_to_dict_keys_match_the_key_function(items: list[str]) -> None:
    result = Stream(items).to_dict(len)
    assert set(result) == {len(w) for w in items}


@given(words)
def test_to_dict_is_last_wins_on_duplicate_keys(items: list[str]) -> None:
    expected = {len(w): w for w in items}
    assert Stream(items).to_dict(len) == expected


def test_to_dict_defaults_the_value_to_the_item() -> None:
    assert Stream.of("apple", "fig").to_dict(lambda w: w[0]) == {"a": "apple", "f": "fig"}


def test_to_dict_accepts_a_value_function() -> None:
    assert Stream.of("apple", "fig").to_dict(lambda w: w[0], len) == {"a": 5, "f": 3}


def test_to_dict_later_item_wins() -> None:
    assert Stream.of("ant", "ape").to_dict(lambda w: w[0]) == {"a": "ape"}


@given(words, st.text())
def test_join_matches_str_join(items: list[str], separator: str) -> None:
    assert Stream(items).join(separator) == separator.join(items)


def test_join_defaults_to_no_separator() -> None:
    assert Stream.of("a", "b", "c").join() == "abc"


def test_collectors_on_an_empty_stream() -> None:
    assert Stream.empty().to_tuple() == ()
    assert Stream[int].empty().to_set() == set()
    assert Stream[int].empty().to_dict(lambda n: n) == {}
    assert Stream[str].empty().join("-") == ""


def test_to_set_requires_hashable_items() -> None:
    with pytest.raises(TypeError):
        Stream.of([1], [2]).to_set()


def test_collectors_consume_the_stream() -> None:
    stream = Stream.of(1, 2, 3)
    stream.to_tuple()
    with pytest.raises(StreamConsumedError):
        stream.to_list()


def test_collectors_compose_after_intermediate_ops() -> None:
    assert Stream.from_iterable(range(6)).filter(lambda n: n % 2 == 0).to_tuple() == (0, 2, 4)
    assert Stream.from_iterable(range(4)).map(str).join(",") == "0,1,2,3"
