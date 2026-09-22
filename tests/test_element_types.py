"""The API across varied element types, not just ints.

Stream[T] is generic, so the type-sensitive ops -- anything relying on
hashing, equality or ordering -- need exercising against the types real
callers actually hold.
"""

import math
from dataclasses import dataclass, field

import pytest
from hypothesis import given
from hypothesis import strategies as st

from pystreamlet import Stream

words = st.lists(st.text())
floats = st.lists(st.floats(allow_nan=False, allow_infinity=False))


@dataclass(frozen=True)
class Person:
    name: str
    age: int


@dataclass
class Mutable:
    """Unhashable by design: dataclasses with eq=True set __hash__ to None."""

    tags: list[str] = field(default_factory=list)


class CaseInsensitive:
    """Custom __eq__/__hash__ so equality is not identity."""

    def __init__(self, value: str) -> None:
        self.value = value

    def __eq__(self, other: object) -> bool:
        return isinstance(other, CaseInsensitive) and self.value.lower() == other.value.lower()

    def __hash__(self) -> int:
        return hash(self.value.lower())

    def __repr__(self) -> str:
        return f"CI({self.value!r})"


# --- strings ------------------------------------------------------------


@given(words)
def test_string_ops_round_trip(items: list[str]) -> None:
    assert Stream(items).map(str.upper).to_list() == [w.upper() for w in items]


def test_strings_flow_through_a_realistic_pipeline() -> None:
    lines = ["INFO boot", "ERROR disk", "INFO ready", "ERROR net"]
    result = (
        Stream(lines)
        .filter(lambda line: line.startswith("ERROR"))
        .map(lambda line: line.split(" ", 1)[1])
        .sorted()
        .join(", ")
    )
    assert result == "disk, net"


def test_flat_map_splits_strings_into_words() -> None:
    assert Stream.of("a b", "c").flat_map(str.split).to_list() == ["a", "b", "c"]


def test_string_grouping_by_first_letter() -> None:
    groups = Stream.of("apple", "avocado", "banana").group_by(lambda w: w[0])
    assert groups == {"a": ["apple", "avocado"], "b": ["banana"]}


# --- floats -------------------------------------------------------------


@given(floats)
def test_float_sum_matches_builtin(items: list[float]) -> None:
    assert Stream(items).sum() == sum(items)


def test_float_and_int_equality_collapses_in_distinct() -> None:
    """1, True and 1.0 are all equal and hash alike, so only the first survives."""
    assert Stream.of(1, True, 1.0).distinct().to_list() == [1]


def test_negative_zero_collapses_with_zero() -> None:
    assert Stream.of(0.0, -0.0).distinct().to_list() == [0.0]


def test_the_same_nan_object_deduplicates() -> None:
    """Sets short-circuit on identity, so one NaN object is still one item."""
    nan = float("nan")
    assert len(Stream.of(nan, nan).distinct().to_list()) == 1


def test_separate_nan_objects_do_not_deduplicate() -> None:
    """NaN != NaN, so distinct NaN objects are distinct items."""
    assert len(Stream.of(float("nan"), float("nan")).distinct().to_list()) == 2


def test_nan_propagates_through_sum() -> None:
    assert math.isnan(Stream.of(1.0, float("nan")).sum())


# --- booleans and None --------------------------------------------------


def test_booleans_sum_as_integers() -> None:
    assert Stream.of(True, True, False).sum() == 2


def test_none_is_a_perfectly_valid_item() -> None:
    assert Stream.of(None, 1, None).distinct().to_list() == [None, 1]


def test_first_cannot_distinguish_empty_from_a_leading_none() -> None:
    """A known limitation: both cases return None."""
    assert Stream.of(None, 1).first() is None
    assert Stream.empty().first() is None


def test_filter_uses_the_predicate_not_truthiness() -> None:
    assert Stream.of(0, 1, 2, 0).filter(lambda n: n == 0).to_list() == [0, 0]


# --- tuples and nested containers ---------------------------------------


def test_tuples_are_hashable_items() -> None:
    assert Stream.of((1, 2), (1, 2), (3,)).distinct().to_list() == [(1, 2), (3,)]


def test_tuples_sort_lexicographically() -> None:
    assert Stream.of((2, 1), (1, 9), (1, 2)).sorted().to_list() == [(1, 2), (1, 9), (2, 1)]


def test_to_dict_from_pairs() -> None:
    assert Stream.of(("a", 1), ("b", 2)).to_dict(lambda p: p[0], lambda p: p[1]) == {"a": 1, "b": 2}


def test_nested_lists_are_unhashable() -> None:
    with pytest.raises(TypeError):
        Stream.of([1], [2]).distinct().to_list()


def test_nested_lists_still_flatten() -> None:
    """flat_map needs no hashing, so unhashable items are fine."""
    assert Stream.of([1, 2], [3]).flat_map(lambda xs: xs).to_list() == [1, 2, 3]


# --- dataclasses --------------------------------------------------------


def test_frozen_dataclasses_group_and_sort() -> None:
    people = [Person("ana", 30), Person("bo", 25), Person("cy", 30)]
    assert Stream(people).group_by(lambda p: p.age) == {
        30: [Person("ana", 30), Person("cy", 30)],
        25: [Person("bo", 25)],
    }
    assert Stream(people).min(key=lambda p: p.age) == Person("bo", 25)
    assert Stream(people).sorted(key=lambda p: p.name).first() == Person("ana", 30)


def test_frozen_dataclasses_deduplicate_by_value() -> None:
    assert Stream.of(Person("ana", 30), Person("ana", 30)).distinct().to_list() == [
        Person("ana", 30)
    ]


def test_mutable_dataclasses_are_unhashable() -> None:
    with pytest.raises(TypeError):
        Stream.of(Mutable(), Mutable()).to_set()


def test_dataclass_pipeline_produces_a_report() -> None:
    people = [Person("ana", 30), Person("bo", 25), Person("cy", 30)]
    report = (
        Stream(people)
        .filter(lambda p: p.age >= 30)
        .map(lambda p: p.name.title())
        .sorted()
        .join(" & ")
    )
    assert report == "Ana & Cy"


# --- custom equality ----------------------------------------------------


def test_custom_eq_and_hash_drive_distinct() -> None:
    result = Stream.of(CaseInsensitive("Hi"), CaseInsensitive("HI")).distinct().to_list()
    assert result == [CaseInsensitive("Hi")]


def test_custom_eq_and_hash_drive_group_by() -> None:
    groups = Stream.of("Hi", "HI", "yo").group_by(CaseInsensitive)
    assert len(groups) == 2
    assert groups[CaseInsensitive("hi")] == ["Hi", "HI"]


# --- type transformation ------------------------------------------------


def test_map_changes_the_element_type_through_the_chain() -> None:
    result = Stream.of(1, 2, 3).map(str).map(lambda s: s * 2).to_list()
    assert result == ["11", "22", "33"]


def test_reduce_can_fold_into_a_different_type() -> None:
    people = [Person("ana", 30), Person("bo", 25)]
    total = Stream(people).reduce(lambda acc, p: acc + p.age, 0)
    assert total == 55


def test_flat_map_can_change_the_element_type() -> None:
    assert Stream.of("ab", "c").flat_map(list).to_list() == ["a", "b", "c"]


def test_heterogeneous_streams_work_when_ops_allow_it() -> None:
    mixed: list[object] = [1, "a", None, (2,)]
    assert Stream(mixed).count() == 4
    assert Stream(mixed).map(type).map(lambda t: t.__name__).to_list() == [
        "int",
        "str",
        "NoneType",
        "tuple",
    ]
