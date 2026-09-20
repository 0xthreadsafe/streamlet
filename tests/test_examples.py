"""The shipped examples must keep working.

An example that no longer runs is worse than no example, so these exercise
the real functions rather than just importing the module.
"""

import sys
from pathlib import Path

import pytest

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"
sys.path.insert(0, str(EXAMPLES))

from concurrent_fan_out import (  # noqa: E402
    CONCURRENCY,
    USER_IDS,
    FakeAPI,
    concurrent,
    failure_cancels_siblings,
    first_match_stops_early,
    report_by_team,
    sequential,
)

FAST = 0.001


async def test_sequential_fetches_every_user() -> None:
    api = FakeAPI(latency=FAST)
    users, _ = await sequential(api)
    assert [user.id for user in users] == USER_IDS
    assert api.peak_concurrency == 1


async def test_concurrent_returns_the_same_users_in_order() -> None:
    api = FakeAPI(latency=FAST)
    users, _ = await concurrent(api)
    assert [user.id for user in users] == USER_IDS


async def test_concurrent_respects_the_limit() -> None:
    api = FakeAPI(latency=0.01)
    await concurrent(api)
    assert 1 < api.peak_concurrency <= CONCURRENCY


async def test_concurrent_is_faster_than_sequential() -> None:
    slow = FakeAPI(latency=0.01)
    _, slow_elapsed = await sequential(slow)
    fast = FakeAPI(latency=0.01)
    _, fast_elapsed = await concurrent(fast)
    assert fast_elapsed < slow_elapsed


async def test_report_groups_active_users_by_team() -> None:
    teams = await report_by_team(FakeAPI(latency=FAST))
    assert set(teams) == {"platform", "growth", "infra"}
    assert all(names == sorted(names) for names in teams.values())
    assert sum(len(names) for names in teams.values()) == len(
        [uid for uid in USER_IDS if uid % 4 != 0]
    )


async def test_first_match_does_not_fetch_everything() -> None:
    api = FakeAPI(latency=FAST)
    match = await first_match_stops_early(api)
    assert match is not None
    assert match.team == "infra"
    assert api.calls < len(USER_IDS)


async def test_a_failure_propagates_as_itself() -> None:
    api = FakeAPI(latency=FAST, failing_ids=frozenset({7}))
    assert await failure_cancels_siblings(api) == "GET /users/7 failed"


async def test_no_requests_are_left_running_after_a_failure() -> None:
    api = FakeAPI(latency=0.05, failing_ids=frozenset({3}))
    await failure_cancels_siblings(api)
    assert api._inflight == 0


async def test_the_example_runs_end_to_end(capsys: pytest.CaptureFixture[str]) -> None:
    """Guards the demo entry point and the printed report."""
    from concurrent_fan_out import main

    await main(latency=FAST)

    output = capsys.readouterr().out
    assert "sequential" in output
    assert "concurrent" in output
    assert "faster" in output
    assert "grouped by team" in output
