"""Concurrent API fan-out with ``AsyncStream``.

Fetching 24 records one at a time takes 24 round trips. ``map_concurrent``
keeps several requests in flight while still pulling lazily from the source,
so a pipeline reads the same way it always does -- it just finishes sooner.

Run it::

    uv run python examples/concurrent_fan_out.py

The "API" here is simulated so the example needs no network, no keys and no
extra dependencies. Swap ``FakeAPI.get_user`` for a real client call (httpx,
aiohttp) and nothing else changes.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from pystreamlet import AsyncStream

USER_IDS = list(range(1, 25))
CONCURRENCY = 8


@dataclass(frozen=True)
class User:
    id: int
    name: str
    team: str
    active: bool


class FakeAPI:
    """Stands in for a real HTTP client, with latency and occasional failures."""

    TEAMS = ("platform", "growth", "infra")

    def __init__(self, *, latency: float = 0.08, failing_ids: frozenset[int] = frozenset()) -> None:
        self.latency = latency
        self.failing_ids = failing_ids
        self.calls = 0
        self.peak_concurrency = 0
        self._inflight = 0

    async def get_user(self, user_id: int) -> User:
        self.calls += 1
        self._inflight += 1
        self.peak_concurrency = max(self.peak_concurrency, self._inflight)
        try:
            await asyncio.sleep(self.latency)
            if user_id in self.failing_ids:
                raise ConnectionError(f"GET /users/{user_id} failed")
            return User(
                id=user_id,
                name=f"user-{user_id:02d}",
                team=self.TEAMS[user_id % len(self.TEAMS)],
                active=user_id % 4 != 0,
            )
        finally:
            self._inflight -= 1


async def sequential(api: FakeAPI) -> tuple[list[User], float]:
    """One request at a time -- the baseline."""
    started = time.perf_counter()
    users = await AsyncStream.from_iterable(USER_IDS).map(api.get_user).to_list()
    return users, time.perf_counter() - started


async def concurrent(api: FakeAPI) -> tuple[list[User], float]:
    """The same pipeline, with several requests in flight."""
    started = time.perf_counter()
    users = await (
        AsyncStream.from_iterable(USER_IDS)
        .map_concurrent(api.get_user, limit=CONCURRENCY)
        .to_list()
    )
    return users, time.perf_counter() - started


async def report_by_team(api: FakeAPI) -> dict[str, list[str]]:
    """Fan out, then reduce -- the shape most real pipelines take."""
    groups = await (
        AsyncStream.from_iterable(USER_IDS)
        .map_concurrent(api.get_user, limit=CONCURRENCY)
        .filter(lambda user: user.active)
        .group_by(lambda user: user.team)
    )
    return {team: sorted(user.name for user in members) for team, members in groups.items()}


async def first_match_stops_early(api: FakeAPI) -> User | None:
    """Laziness still holds: the source is never fully consumed."""
    return await (
        AsyncStream.from_iterable(USER_IDS)
        .map_concurrent(api.get_user, limit=CONCURRENCY)
        .filter(lambda user: user.team == "infra")
        .first()
    )


async def failure_cancels_siblings(api: FakeAPI) -> str:
    """A failed request propagates as itself and stops the rest."""
    try:
        await (
            AsyncStream.from_iterable(USER_IDS)
            .map_concurrent(api.get_user, limit=CONCURRENCY)
            .to_list()
        )
    except ConnectionError as exc:
        return str(exc)
    return "no failure"


async def main(latency: float = 0.08) -> None:
    """Run the whole demo. ``latency`` is lowered by the test suite."""
    slow = FakeAPI(latency=latency)
    _, slow_elapsed = await sequential(slow)
    print(f"sequential : {slow.calls:2d} requests in {slow_elapsed:.2f}s (1 at a time)")

    fast = FakeAPI(latency=latency)
    users, fast_elapsed = await concurrent(fast)
    print(
        f"concurrent : {fast.calls:2d} requests in {fast_elapsed:.2f}s "
        f"(up to {fast.peak_concurrency} at once) "
        f"-- {slow_elapsed / fast_elapsed:.1f}x faster"
    )
    print(f"             results stay in source order: {[u.id for u in users[:6]]} ...")

    print()
    teams = await report_by_team(FakeAPI(latency=latency))
    print("grouped by team (active users only):")
    for team, names in sorted(teams.items()):
        print(
            f"  {team:9} {len(names):2d}  {', '.join(names[:3])}{' ...' if len(names) > 3 else ''}"
        )

    print()
    lazy = FakeAPI(latency=latency)
    match = await first_match_stops_early(lazy)
    assert match is not None
    print(f"first infra user: {match.name} -- fetched {lazy.calls}/{len(USER_IDS)} users, not all")

    print()
    broken = FakeAPI(latency=latency, failing_ids=frozenset({7}))
    message = await failure_cancels_siblings(broken)
    print(f"on failure : {message!r} propagated; in-flight requests cancelled")


if __name__ == "__main__":
    asyncio.run(main())
