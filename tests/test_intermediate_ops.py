from itertools import count, islice

from hypothesis import given
from hypothesis import strategies as st

from pystreamlet import Stream

ints = st.lists(st.integers())
counts = st.integers(min_value=0, max_value=1000)


@given(ints)
def test_map_matches_comprehension(items: list[int]) -> None:
    assert list(Stream(items).map(lambda n: n * 10)) == [n * 10 for n in items]


@given(ints)
def test_filter_matches_comprehension(items: list[int]) -> None:
    assert list(Stream(items).filter(lambda n: n % 2 == 0)) == [n for n in items if n % 2 == 0]


@given(ints, counts)
def test_take_matches_slice(items: list[int], n: int) -> None:
    assert list(Stream(items).take(n)) == items[:n]


@given(ints, counts)
def test_skip_matches_slice(items: list[int], n: int) -> None:
    assert list(Stream(items).skip(n)) == items[n:]


@given(ints, counts)
def test_take_and_skip_partition_the_stream(items: list[int], n: int) -> None:
    taken = list(Stream(items).take(n))
    skipped = list(Stream(items).skip(n))
    assert taken + skipped == items


@given(st.lists(st.lists(st.integers())))
def test_flat_map_flattens_one_level(nested: list[list[int]]) -> None:
    expected = [n for sub in nested for n in sub]
    assert list(Stream(nested).flat_map(lambda xs: xs)) == expected


@given(ints)
def test_distinct_preserves_first_seen_order(items: list[int]) -> None:
    assert list(Stream(items).distinct()) == list(dict.fromkeys(items))


@given(ints)
def test_distinct_has_no_duplicates(items: list[int]) -> None:
    result = list(Stream(items).distinct())
    assert len(result) == len(set(result))


@given(counts)
def test_chain_stays_lazy_over_infinite_source(n: int) -> None:
    result = Stream(count()).map(lambda x: x * 3).filter(lambda x: x % 2 == 0).take(n)
    expected = list(islice((x * 3 for x in count() if (x * 3) % 2 == 0), n))
    assert list(result) == expected
