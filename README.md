# Streamlet

A fluent, **lazy** stream-processing library for Python, modeled after Java's Streams API.

```python
from streamlet import Stream

(
    Stream.from_iterable(range(100))
    .filter(lambda n: n % 2 == 0)
    .map(lambda n: n**2)
    .take(5)
    .to_list()
)
# [0, 4, 16, 36, 64]
```

The goal is to make `itertools`' power reachable through clean, chainable syntax.

```bash
pip install pystreamlet    # or: uv add pystreamlet
```

You install `pystreamlet` but import `streamlet`, as above — PyPI reserves names confusable with
`streamlit`. Requires **Python 3.11+**, has no dependencies, and ships type information
(`py.typed`).

> **Status: 0.1.0, awaiting its first upload.** Everything below works and is tested; until the
> release lands on PyPI, install from source (see [Development](#development)). Before 1.0.0 the
> API may still change — see [versioning](#versioning).

## Why

`itertools` is powerful but reads inside out — you write the last step first:

```python
# itertools: read from the middle outwards
from itertools import count, islice

list(islice((n**2 for n in count() if n % 2 == 0), 5))

# streamlet: read left to right, in the order it happens
(
    Stream.iterate(0, lambda n: n + 1)
    .filter(lambda n: n % 2 == 0)
    .map(lambda n: n**2)
    .take(5)
    .to_list()
)
```

Both are lazy and both handle infinite sources. The difference is that one describes the
pipeline in the order it runs.

### Side by side

**Take the first few results of an expensive step.** `islice` wraps what it slices, so the
bound you care about ends up furthest from the work it bounds:

```python
from itertools import islice

list(islice(map(fetch, filter(is_recent, urls)), 5))

Stream(urls).filter(is_recent).map(fetch).take(5).to_list()
```

**Group items by a key.** `itertools.groupby` only groups *adjacent* items, so it needs a sort
first and hands back iterators that expire as you advance:

```python
from itertools import groupby
from operator import attrgetter

by_team = {
    team: list(members)
    for team, members in groupby(sorted(users, key=attrgetter("team")), key=attrgetter("team"))
}

by_team = Stream(users).group_by(attrgetter("team"))
```

**Drop repeats but keep order.** The `set` version loses order, and the `dict.fromkeys` trick
reads as a puzzle:

```python
list(dict.fromkeys(names))

Stream(names).distinct().to_list()
```

**Chain a few steps over a file.** The itertools version needs a `with` block, a generator
expression and a slice, in three different directions:

```python
with open("app.log", encoding="utf-8") as handle:
    first_errors = list(islice((line for line in handle if line.startswith("ERROR")), 10))

with Stream.from_file("app.log") as lines:
    first_errors = lines.filter(lambda line: line.startswith("ERROR")).take(10).to_list()
```

**Stop as soon as one item matches.** `next()` with a default over a generator expression says
the same thing, more quietly:

```python
next((user for user in users if user.is_admin), None)

Stream(users).filter(lambda user: user.is_admin).first()
```

None of these are faster than the `itertools` version — they compile to the same generators.
They are shorter to read and, more usefully, they read in the order the work happens.

## Laziness

Nothing executes until a terminal op runs. Intermediate ops just build a pipeline:

```python
stream = Stream.from_iterable(count()).map(expensive)  # expensive() has not been called
stream.take(3).to_list()  # now it runs -- exactly 3 times
```

This is what makes infinite sources safe:

```python
Stream.iterate(1, lambda n: n * 2).take_while(lambda n: n < 100).to_list()
# [1, 2, 4, 8, 16, 32, 64]
```

## Single use

A stream is consumed once, like the iterator underneath it. Reusing one raises rather than
silently yielding nothing:

```python
stream = Stream.of(1, 2, 3)
stream.to_list()  # [1, 2, 3]
stream.to_list()  # StreamConsumedError
```

## API

### Creating a stream

| | |
|---|---|
| `Stream(iterable)` | wrap any iterable |
| `Stream.of(*items)` | from individual arguments |
| `Stream.from_iterable(iterable)` | from an existing iterable |
| `Stream.empty()` | no items |
| `Stream.iterate(seed, fn)` | infinite: `seed`, `fn(seed)`, `fn(fn(seed))`, … |
| `Stream.generate(fn)` | infinite: repeated calls to `fn()` |
| `Stream.concat(*iterables)` | join sources end to end |
| `Stream.from_file(path)` | lines of a file, handle managed |
| `Stream.from_handle(handle, close=True)` | a handle you already have, ownership stated |

### Intermediate ops — lazy, return a new `Stream`

| | |
|---|---|
| `.map(fn)` | apply `fn` to every item |
| `.filter(predicate)` | keep matching items |
| `.flat_map(fn)` | map to iterables and flatten one level |
| `.distinct()` | drop repeats, keeping first-seen order |
| `.take(n)` / `.skip(n)` | first `n` / everything after `n` |
| `.take_while(p)` / `.drop_while(p)` | stop at, or skip until, the first failure |
| `.peek(action)` | run a side effect per item, yielding it unchanged |
| `.sorted(key=None, reverse=False)` | sort — **buffers the whole stream** |
| `.reverse()` | reverse order — **buffers the whole stream** |

### Terminal ops — consume the stream, return a value

| | |
|---|---|
| `.to_list()` `.to_tuple()` `.to_set()` | materialise |
| `.to_dict(key, value=None)` | collect into a dict (last key wins) |
| `.join(separator="")` | concatenate a `Stream[str]` |
| `.group_by(key)` | `dict` of key → members |
| `.reduce(fn, initial)` | fold into a single value |
| `.sum()` `.count()` `.min(key=None)` `.max(key=None)` | aggregate |
| `.first()` | first item, or `None` |
| `.any(p)` `.all(p)` `.none(p)` | matching — all short-circuit |
| `.for_each(action)` | run an action per item |

### Pipe operator

Any function taking a `Stream` can act as a stage, so you can extend the library without
subclassing:

```python
def errors_only(stream):
    return stream.filter(lambda line: line.startswith("ERROR"))


Stream(lines) | errors_only | (lambda s: s.take(10)) | list
```

## Files

`Stream.from_file` streams a file's lines lazily and owns the handle:

```python
with Stream.from_file("app.log") as lines:
    errors = lines.map(str.rstrip).filter(lambda line: line.startswith("ERROR")).to_list()
```

The file is closed on every exit path: exhausting the stream, stopping early with a `break` or
a partial read, an exception escaping iteration, or leaving the `with` block. `with` remains the
clearest way to express the intent, and it closes the file even if you never iterate at all.

Lines keep their trailing newline, matching `open()`. Pass `encoding`, `errors` or `newline`
through as needed.

### Handles you already have

`Stream(handle)` leaves the handle to whoever opened it — the right default, but it leaks if
nobody follows up. `Stream.from_handle` makes the hand-off explicit:

```python
Stream.from_handle(sock.makefile())  # the stream closes it
Stream.from_handle(sys.stdin, close=False)  # you keep it
```

With `close=True` (the default) the resource is closed on the same paths `from_file` covers;
with `close=False` nothing is closed for you. It works for anything iterable with a `close()` —
socket files, `os.popen` pipes, `io.StringIO`, database cursors — and raises `TypeError` for an
iterable that has none. Streamlet never auto-detects handles in `Stream(...)`: silently closing
something you opened would break the whoever-opens-closes convention.

## Async

`AsyncStream[T]` is the same API over an async source, driven with `async for` and `await`:

```python
import asyncio

from streamlet import AsyncStream


async def fetch(url: str) -> str: ...


async def main() -> None:
    titles = await (
        AsyncStream.from_iterable(urls)
        .filter(lambda u: u.startswith("https://"))
        .map(fetch)
        .take(10)
        .to_list()
    )
```

Every op that takes a function accepts **either a plain function or a coroutine function**, so
`.map(str)` and `.map(fetch)` both work and both type correctly.

### Concurrency

`.map(fetch)` awaits one item at a time. `.map_concurrent(fetch)` runs several at once:

```python
results = await AsyncStream.from_iterable(urls).map_concurrent(fetch, limit=8).to_list()
```

At most `limit` calls are in flight, and items are pulled from the source only as slots free up —
so it stays lazy and works on infinite sources. Results come back in source order by default;
pass `ordered=False` to get each one as soon as it is ready.

If the mapper raises, the original exception propagates (not an `ExceptionGroup`) and every
in-flight call is cancelled. Abandoning the stream early — a `take`, a `break` — cancels them too.

A runnable fan-out demo lives in [`examples/concurrent_fan_out.py`](examples/concurrent_fan_out.py):

```bash
uv run python examples/concurrent_fan_out.py
```

```
sequential : 24 requests in 1.95s (1 at a time)
concurrent : 24 requests in 0.24s (up to 8 at once) -- 8.0x faster
first infra user: user-02 -- fetched 8/24 users, not all
```

Sources can be sync or async: `AsyncStream.of(...)`, `.from_iterable(list)`,
`.from_async_iterable(agen)`, `.empty()`, `.iterate(seed, fn)`, `.generate(fn)`, and
`.concat(...)` — which mixes both kinds.

Terminal ops are coroutines, so they need `await`. Everything else matches `Stream`: the same
intermediate ops, the same single-use semantics, the same laziness.

### Cleanup

Each stage closes the one behind it, so a pipeline that stops early finalises its source right
away — a bounded `take`, a short-circuiting `first`/`any`/`all`, or an exception all unwind the
whole chain. That matters when the source holds something: a connection, a cursor, a handle.

One case Python gives no hook for is a bare `break` out of an `async for`, which leaves the
outermost stage suspended. Wrap the iterator when that matters:

```python
from contextlib import aclosing

async with aclosing(stream.map(fetch).__aiter__()) as items:
    async for item in items:
        if done(item):
            break
```

## Typing

`Stream[T]` is generic and ships a `py.typed` marker, so element types flow through a chain:

```python
Stream.of(1, 2, 3)          # Stream[int]
  .map(str)                 # Stream[str]
  .to_list()                # list[str]
```

Some ops are restricted by self-type and fail at type-check time, not runtime:

- `.sum()` only on numeric streams
- `.join()` only on `Stream[str]`
- `.to_set()`, `.distinct()`, `.group_by()` require hashable items

## Development

```bash
uv sync                    # install project + dev tools (needs Python 3.11+)
uv run pytest              # tests
uv run ruff check .        # lint
uv run mypy                # type check (strict)
```

Run everything the way CI does:

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest
```

## Versioning

Streamlet follows [semantic versioning](https://semver.org/spec/v2.0.0.html). Before 1.0.0 a
breaking change bumps the minor version and everything else bumps the patch version. Changes
are recorded in [CHANGELOG.md](CHANGELOG.md).

Releases are cut from a tag: pushing `vX.Y.Z` builds the artifacts and checks the tag against the
version in `pyproject.toml`. Publishing the GitHub release for that tag uploads them to PyPI
through trusted publishing, so no token lives in this repository.

## Roadmap

- [x] Core `Stream[T]`, intermediate + terminal ops, laziness tests
- [x] `__or__` pipe chaining, constructors, `group_by`, edge cases
- [x] `Stream.from_file` — context-managed resource streams
- [x] `AsyncStream` — bounded concurrent `map`
- [x] Docstrings, README examples
- [ ] PyPI release

## License

[MIT](LICENSE)
