"""Direct proof that nothing executes until a terminal op runs.

The tests elsewhere prove laziness indirectly: an eager implementation would
hang on an infinite source. These prove it numerically -- how many times the
mapper was called, and how many items were pulled from the source.
"""

from collections.abc import Callable, Iterator

from pystreamlet import Stream


class CallSpy:
    """Wraps a function and records how many times it was called."""

    def __init__(self, fn: Callable[[int], int]) -> None:
        self._fn = fn
        self.calls = 0

    def __call__(self, item: int) -> int:
        self.calls += 1
        return self._fn(item)


class CountingSource:
    """An iterable that records how many items have been pulled from it."""

    def __init__(self, size: int) -> None:
        self._size = size
        self.pulled = 0

    def __iter__(self) -> Iterator[int]:
        for item in range(self._size):
            self.pulled += 1
            yield item


def test_construction_pulls_nothing() -> None:
    source = CountingSource(100)
    Stream(source)
    assert source.pulled == 0


def test_building_a_pipeline_runs_nothing() -> None:
    source = CountingSource(100)
    double = CallSpy(lambda n: n * 2)
    keep = CallSpy(lambda n: n % 2)

    Stream(source).map(double).filter(lambda n: bool(keep(n))).take(3)

    assert source.pulled == 0
    assert double.calls == 0
    assert keep.calls == 0


def test_take_pulls_only_what_it_needs() -> None:
    source = CountingSource(100)
    double = CallSpy(lambda n: n * 2)

    result = Stream(source).map(double).take(3).to_list()

    assert result == [0, 2, 4]
    assert source.pulled == 3
    assert double.calls == 3


def test_first_pulls_exactly_one_item() -> None:
    source = CountingSource(100)
    double = CallSpy(lambda n: n * 2)

    assert Stream(source).map(double).first() == 0
    assert source.pulled == 1
    assert double.calls == 1


def test_any_stops_at_the_first_match() -> None:
    source = CountingSource(100)

    assert Stream(source).any(lambda n: n == 4) is True
    assert source.pulled == 5


def test_all_stops_at_the_first_failure() -> None:
    source = CountingSource(100)

    assert Stream(source).all(lambda n: n < 4) is False
    assert source.pulled == 5


def test_terminal_op_pulls_everything() -> None:
    source = CountingSource(10)
    double = CallSpy(lambda n: n * 2)

    Stream(source).map(double).to_list()

    assert source.pulled == 10
    assert double.calls == 10


def test_each_stage_runs_once_per_item() -> None:
    """A three-stage pipeline must not re-walk the source per stage."""
    source = CountingSource(10)
    double = CallSpy(lambda n: n * 2)
    add_one = CallSpy(lambda n: n + 1)

    result = Stream(source).map(double).map(add_one).take(4).to_list()

    assert result == [1, 3, 5, 7]
    assert source.pulled == 4
    assert double.calls == 4
    assert add_one.calls == 4


def test_items_flow_one_at_a_time() -> None:
    """Item N+1 must not be pulled before item N has been emitted."""
    source = CountingSource(100)
    seen_at_pull: list[tuple[int, int]] = []

    stream = Stream(source).map(lambda n: n)
    for index, item in enumerate(stream):
        seen_at_pull.append((item, source.pulled))
        if index == 4:
            break

    assert seen_at_pull == [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)]
