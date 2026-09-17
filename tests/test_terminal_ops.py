import functools
from itertools import count

from hypothesis import given
from hypothesis import strategies as st

from streamlet import Stream

ints = st.lists(st.integers())


@given(ints)
def test_to_list_returns_the_items(items: list[int]) -> None:
    assert Stream(items).to_list() == items


@given(ints)
def test_count_matches_len(items: list[int]) -> None:
    assert Stream(items).count() == len(items)


@given(ints)
def test_sum_matches_builtin(items: list[int]) -> None:
    assert Stream(items).sum() == sum(items)


@given(ints, st.integers())
def test_reduce_matches_functools(items: list[int], initial: int) -> None:
    expected = functools.reduce(lambda a, n: a + n, items, initial)
    assert Stream(items).reduce(lambda a, n: a + n, initial) == expected


def test_reduce_can_change_the_result_type() -> None:
    assert Stream([1, 2, 3]).reduce(lambda acc, n: acc + str(n), "") == "123"


@given(ints)
def test_first_returns_the_head_or_none(items: list[int]) -> None:
    assert Stream(items).first() == (items[0] if items else None)


@given(ints)
def test_any_matches_builtin(items: list[int]) -> None:
    assert Stream(items).any(lambda n: n > 0) == any(n > 0 for n in items)


@given(ints)
def test_all_matches_builtin(items: list[int]) -> None:
    assert Stream(items).all(lambda n: n > 0) == all(n > 0 for n in items)


def test_all_is_true_for_an_empty_stream() -> None:
    assert Stream([]).all(lambda n: n > 0) is True


def test_any_is_false_for_an_empty_stream() -> None:
    assert Stream([]).any(lambda n: n > 0) is False


def test_first_short_circuits_on_an_infinite_source() -> None:
    assert Stream(count()).first() == 0


def test_any_short_circuits_on_an_infinite_source() -> None:
    assert Stream(count()).any(lambda n: n > 100) is True


def test_all_short_circuits_on_an_infinite_source() -> None:
    assert Stream(count()).all(lambda n: n < 100) is False
