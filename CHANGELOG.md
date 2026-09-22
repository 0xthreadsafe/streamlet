# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and Streamlet uses
[semantic versioning](https://semver.org/spec/v2.0.0.html).

Until 1.0.0 the public API may still change: a breaking change bumps the minor version,
anything else bumps the patch version.

## [Unreleased]

### Fixed

- `AsyncStream` now finalises its source as soon as a pipeline is done with it. Each stage closes
  the one behind it, so a bounded `take`, a short-circuiting terminal op (`first`, `any`, `all`),
  an exception or an explicit `aclose` unwinds the whole chain instead of leaving the source
  suspended until the event loop shut its async generators down. A bare `break` out of an
  `async for` still needs `contextlib.aclosing` — Python offers no hook for it.

## [0.1.0] — 2026-09-22

First release.

### Added

- `Stream[T]` — a lazy, single-use, generic stream over any iterable. Intermediate ops build a
  pipeline and nothing runs until a terminal op asks for items.
  - Sources: `Stream(iterable)`, `of`, `from_iterable`, `empty`, `iterate`, `generate`, `concat`.
  - Lazy ops: `map`, `filter`, `flat_map`, `distinct`, `take`, `skip`, `take_while`,
    `drop_while`, `peek`.
  - Buffering ops: `sorted`, `reverse` — documented as reading the whole stream.
  - Terminal ops: `to_list`, `to_tuple`, `to_set`, `to_dict`, `join`, `group_by`, `reduce`,
    `sum`, `count`, `min`, `max`, `first`, `any`, `all`, `none`, `for_each`.
  - `__iter__`, `__repr__`, and `__or__` for pipe-style chaining.
  - `StreamConsumedError` on a second iteration, rather than silently yielding nothing.
- `Stream.from_file(path)` — lines of a file as a `FileStream`, which closes the handle on every
  exit path: exhaustion, an early `break`, an exception, or leaving its `with` block.
- `Stream.from_handle(handle, close=True)` — the same guarantee for a handle you already have,
  with ownership stated explicitly. Works for any iterable with a `close()`: socket files,
  `os.popen` pipes, `io.StringIO`, database cursors. Backed by a generic `ResourceStream[T]`.
- `AsyncStream[T]` — the same API over an async source, driven with `async for` and `await`.
  Every op that takes a function accepts a plain function or a coroutine function.
- `AsyncStream.map_concurrent(mapper, limit=8, ordered=True)` — bounded concurrency that stays
  lazy: at most `limit` calls in flight, items pulled only as slots free up, so it works on
  infinite sources. A failing mapper propagates the original exception (not an `ExceptionGroup`)
  and cancels its siblings; abandoning the stream cancels them too.
- Typing: generic throughout, self-typed restrictions (`sum` on numbers, `join` on strings,
  `to_set`/`distinct`/`group_by` on hashables), and a `py.typed` marker in the wheel.

[Unreleased]: https://github.com/0xthreadsafe/streamlet/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/0xthreadsafe/streamlet/releases/tag/v0.1.0
