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

> **Status: in development.** The API below works and is tested, but Streamlet is not
> published on PyPI yet. Install from source (see [Development](#development)).

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

The file is closed when the stream is exhausted, when an exception escapes iteration, or when
the `with` block exits. If you stop early *without* `with` — a `break`, a `first()` — the handle
stays open until the stream is garbage collected, which is not a moment you control. Use `with`
whenever you might not read to the end.

Lines keep their trailing newline, matching `open()`. Pass `encoding`, `errors` or `newline`
through as needed.

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
uv sync                    # install project + dev tools
uv run pytest              # tests
uv run ruff check .        # lint
uv run mypy                # type check (strict)
```

Run everything the way CI does:

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest
```

## Roadmap

- [x] Core `Stream[T]`, intermediate + terminal ops, laziness tests
- [x] `__or__` pipe chaining, constructors, `group_by`, edge cases
- [x] `Stream.from_file` — context-managed resource streams
- [ ] `AsyncStream` — concurrent `map` over `asyncio.TaskGroup`
- [ ] Docstrings, README examples, PyPI release

## License

Not yet chosen.
