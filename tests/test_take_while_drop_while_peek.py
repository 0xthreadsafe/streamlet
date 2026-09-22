"""Lazy intermediate ops: take_while, drop_while, peek."""

from itertools import count

from hypothesis import given
from hypothesis import strategies as st

from pystreamlet import Stream

ints = st.lists(st.integers())


@given(ints)
def test_take_while_matches_itertools(items: list[int]) -> None:
    import itertools

    expected = list(itertools.takewhile(lambda n: n > 0, items))
    assert Stream(items).take_while(lambda n: n > 0).to_list() == expected


@given(ints)
def test_drop_while_matches_itertools(items: list[int]) -> None:
    import itertools

    expected = list(itertools.dropwhile(lambda n: n > 0, items))
    assert Stream(items).drop_while(lambda n: n > 0).to_list() == expected


@given(ints)
def test_take_while_and_drop_while_partition_the_stream(items: list[int]) -> None:
    head = Stream(items).take_while(lambda n: n > 0).to_list()
    tail = Stream(items).drop_while(lambda n: n > 0).to_list()
    assert head + tail == items


def test_take_while_stops_at_the_first_failure() -> None:
    """Unlike filter, it stops rather than skipping."""
    assert Stream.of(1, 2, 9, 3, 4).take_while(lambda n: n < 5).to_list() == [1, 2]
    assert Stream.of(1, 2, 9, 3, 4).filter(lambda n: n < 5).to_list() == [1, 2, 3, 4]


def test_drop_while_never_resumes_dropping() -> None:
    assert Stream.of(1, 2, 9, 1, 2).drop_while(lambda n: n < 5).to_list() == [9, 1, 2]


def test_take_while_on_an_infinite_source() -> None:
    assert Stream.from_iterable(count()).take_while(lambda n: n < 4).to_list() == [0, 1, 2, 3]


def test_drop_while_on_an_infinite_source() -> None:
    result = Stream.from_iterable(count()).drop_while(lambda n: n < 4).take(3).to_list()
    assert result == [4, 5, 6]


def test_take_while_false_immediately_is_empty() -> None:
    assert Stream.of(1, 2, 3).take_while(lambda n: False).to_list() == []


def test_drop_while_true_forever_is_empty() -> None:
    assert Stream.of(1, 2, 3).drop_while(lambda n: True).to_list() == []


@given(ints)
def test_peek_does_not_change_the_items(items: list[int]) -> None:
    assert Stream(items).peek(lambda n: None).to_list() == items


@given(ints)
def test_peek_sees_every_item_that_is_pulled(items: list[int]) -> None:
    seen: list[int] = []
    Stream(items).peek(seen.append).to_list()
    assert seen == items


def test_peek_runs_nothing_until_a_terminal_op() -> None:
    seen: list[int] = []
    Stream.of(1, 2, 3).peek(seen.append)
    assert seen == []


def test_peek_only_runs_for_items_actually_pulled() -> None:
    seen: list[int] = []
    result = Stream.from_iterable(count()).peek(seen.append).take(3).to_list()
    assert result == [0, 1, 2]
    assert seen == [0, 1, 2]


def test_peek_placement_in_the_chain_matters() -> None:
    before: list[int] = []
    after: list[int] = []
    Stream.of(1, 2, 3, 4).peek(before.append).filter(lambda n: n % 2 == 0).peek(
        after.append
    ).to_list()
    assert before == [1, 2, 3, 4]
    assert after == [2, 4]
