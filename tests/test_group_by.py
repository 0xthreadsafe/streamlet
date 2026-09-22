"""``group_by``: the terminal op that must read the whole stream."""

import pytest
from hypothesis import given
from hypothesis import strategies as st

from pystreamlet import Stream, StreamConsumedError

ints = st.lists(st.integers())


@given(ints)
def test_every_item_lands_in_exactly_one_group(items: list[int]) -> None:
    groups = Stream(items).group_by(lambda n: n % 3)
    flattened = [item for group in groups.values() for item in group]
    assert sorted(flattened) == sorted(items)


@given(ints)
def test_group_members_all_share_the_key(items: list[int]) -> None:
    groups = Stream(items).group_by(lambda n: n % 3)
    for key, group in groups.items():
        assert all(n % 3 == key for n in group)


@given(ints)
def test_keys_are_the_distinct_key_values(items: list[int]) -> None:
    groups = Stream(items).group_by(lambda n: n % 3)
    assert set(groups) == {n % 3 for n in items}


@given(ints)
def test_grouping_by_identity_matches_distinct(items: list[int]) -> None:
    groups = Stream(items).group_by(lambda n: n)
    assert list(groups) == Stream(items).distinct().to_list()


def test_groups_non_adjacent_items_together() -> None:
    """Unlike itertools.groupby, items need not be consecutive."""
    assert Stream.of(1, 2, 1, 3, 2).group_by(lambda n: n) == {1: [1, 1], 2: [2, 2], 3: [3]}


def test_keys_are_in_first_seen_order() -> None:
    groups = Stream.of("cherry", "apple", "banana", "avocado").group_by(lambda w: w[0])
    assert list(groups) == ["c", "a", "b"]


def test_members_keep_stream_order() -> None:
    groups = Stream.of(5, 1, 4, 2, 3).group_by(lambda n: n % 2)
    assert groups == {1: [5, 1, 3], 0: [4, 2]}


def test_empty_stream_gives_an_empty_dict() -> None:
    assert Stream.empty().group_by(lambda n: n) == {}


def test_returns_a_plain_dict_not_a_defaultdict() -> None:
    """A missing key must raise, not silently create an empty group."""
    groups = Stream.of(1, 2).group_by(lambda n: n)
    with pytest.raises(KeyError):
        _ = groups[99]


def test_is_terminal_and_consumes_the_stream() -> None:
    stream = Stream.of(1, 2, 3)
    stream.group_by(lambda n: n)
    with pytest.raises(StreamConsumedError):
        stream.to_list()


def test_composes_after_intermediate_ops() -> None:
    groups = Stream.from_iterable(range(10)).filter(lambda n: n % 2 == 0).group_by(lambda n: n % 3)
    assert groups == {0: [0, 6], 2: [2, 8], 1: [4]}
