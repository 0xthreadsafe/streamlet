"""Source factories: Stream.iterate, Stream.generate, Stream.concat."""

import itertools

from hypothesis import given
from hypothesis import strategies as st

from streamlet import Stream

ints = st.lists(st.integers())
counts = st.integers(min_value=0, max_value=200)


@given(st.integers(), counts)
def test_iterate_matches_repeated_application(seed: int, n: int) -> None:
    expected = []
    current = seed
    for _ in range(n):
        expected.append(current)
        current += 3
    assert Stream.iterate(seed, lambda x: x + 3).take(n).to_list() == expected


def test_iterate_yields_the_seed_first() -> None:
    assert Stream.iterate(1, lambda n: n * 2).take(5).to_list() == [1, 2, 4, 8, 16]


def test_iterate_is_infinite_and_lazy() -> None:
    assert Stream.iterate(0, lambda n: n + 1).take(3).to_list() == [0, 1, 2]


def test_iterate_pairs_with_take_while() -> None:
    result = Stream.iterate(1, lambda n: n * 3).take_while(lambda n: n < 100).to_list()
    assert result == [1, 3, 9, 27, 81]


def test_iterate_calls_the_function_lazily() -> None:
    calls = []

    def step(n: int) -> int:
        calls.append(n)
        return n + 1

    Stream.iterate(0, step).take(3).to_list()
    assert calls == [0, 1]  # one fewer call than items: the seed is free


@given(counts)
def test_generate_produces_the_requested_count(n: int) -> None:
    counter = itertools.count()
    assert Stream.generate(lambda: next(counter)).take(n).to_list() == list(range(n))


def test_generate_is_lazy() -> None:
    calls = []

    def produce() -> int:
        calls.append(1)
        return 7

    Stream.generate(produce).take(2).to_list()
    assert len(calls) == 2


def test_generate_runs_nothing_before_a_terminal_op() -> None:
    calls = []
    Stream.generate(lambda: calls.append(1)).take(5)
    assert calls == []


@given(ints, ints)
def test_concat_joins_two_sources(a: list[int], b: list[int]) -> None:
    assert Stream.concat(a, b).to_list() == a + b


@given(ints, ints, ints)
def test_concat_is_associative(a: list[int], b: list[int], c: list[int]) -> None:
    assert Stream.concat(a, b, c).to_list() == Stream.concat(a, Stream.concat(b, c)).to_list()


def test_concat_accepts_mixed_iterable_types() -> None:
    assert Stream.concat([1, 2], (3,), range(4, 6), Stream.of(6)).to_list() == [1, 2, 3, 4, 5, 6]


def test_concat_with_no_arguments_is_empty() -> None:
    assert Stream.concat().to_list() == []


def test_concat_is_lazy() -> None:
    result = Stream.concat([1, 2], itertools.count(3)).take(5).to_list()
    assert result == [1, 2, 3, 4, 5]


def test_concat_does_not_touch_later_sources_early() -> None:
    pulled = []

    def tracked() -> "itertools.chain[int]":
        pulled.append(1)
        return itertools.chain([9])

    assert Stream.concat([1, 2], tracked()).take(2).to_list() == [1, 2]
