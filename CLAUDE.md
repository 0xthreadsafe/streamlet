# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What Streamlet is

A fluent, **lazy** stream-processing library for Python, modeled after Java's Streams API:

```python
Stream.of(...).filter(...).map(...).take(10).to_list()
```

The goal is to make `itertools`' power reachable through clean, chainable syntax. It is a
**portfolio project** — each feature was chosen to demonstrate a specific Python capability, so
prefer the idiomatic-and-showcase-worthy implementation over the shortest one:

| Capability | Where it shows up |
|---|---|
| Generators / laziness | Intermediate ops (`map`, `filter`, `take`, `skip`, `flat_map`, `distinct`) build a pipeline; **nothing executes until a terminal op runs** |
| Magic methods | `__iter__`, `__repr__`, and `__or__` for pipe-style chaining |
| Context managers | `Stream.from_file(path)` / `from_handle(h)` via `__enter__`/`__exit__`; the handle must close on early `break` or exception |
| Async | `AsyncStream[T]` with `__aiter__`, concurrent map with bounded concurrency |
| Generics | `Stream[T]` via `typing.Generic`, for real IDE autocomplete |
| Terminal ops | `to_list`, `reduce`, `sum`, `count`, `first`, `any`, `all`, `group_by` |

Published on PyPI as **`pystreamlet`**, which is also the import name. PyPI refuses `streamlet`
as confusable with `streamlit`, and TestPyPI's `streamlet` belongs to an unrelated project, so
0.2.0 renamed the module to match the distribution. The GitHub repo is still `streamlet`.

## Project state

M1-M6 are complete bar the upload itself. `src/pystreamlet/stream.py` holds `Stream[T]`;
`file_stream.py` holds `ResourceStream[T]`/`FileStream` (`from_file`, `from_handle`);
`async_stream.py` holds `AsyncStream[T]` with `map_concurrent`. 349 tests pass under
`mypy --strict`, including Hypothesis property tests, direct laziness proofs (call spies and
pull counters, not just infinite-source canaries), closing guarantees on both the sync and
async sides, and async exception propagation. `py.typed` ships in the wheel so consumers get
real types.

0.1.0 and 0.2.0 are published. Releases go out by tagging `vX.Y.Z` and publishing the GitHub
release for that tag; see the publish workflow.

Roadmap milestones, tracked in Linear: **M1** scaffolding/lint/CI · **M2** core `Stream[T]`,
intermediate + terminal ops, laziness tests · **M3** `__or__`, constructors, `group_by`, edge cases
· **M4** `from_file` / `from_handle` resource streams · **M5** `AsyncStream` · **M6** docstrings,
`mypy --strict`, README, PyPI release.

## Linear

Issues live in the **Thread-Safe** workspace (team key `THR`), project *Streamlet*
(`d0777fc2-700b-46ee-abc4-88524793410f`).

The Linear **MCP connector is authenticated to a different workspace (Narmin) and cannot see `THR`
issues** — its tools return "team not found". Query the GraphQL API directly instead, using the key
in `.env`:

```bash
set -a && . ./.env && set +a && curl -s -X POST https://api.linear.app/graphql \
  -H "Content-Type: application/json" -H "Authorization: $LINEAR_API_KEY" \
  -d '{"query":"query { issue(id: \"THR-6\") { title state{name} branchName } }"}'
```

## Commands

Dependencies are managed with **uv** (`uv_build` is the build backend, `uv.lock` is committed).
Dev tools live in `[dependency-groups] dev`, which uv installs by default — no extra flags needed.

```bash
uv sync                    # install project + dev tools into .venv
uv run ruff check .        # lint
uv run ruff format .       # format (add --check to report without rewriting)
uv run mypy                # type check (paths come from [tool.mypy] files)
uv run pytest              # tests
uv run pytest tests/test_smoke.py::test_main_run   # single test
uv run streamlet           # run the console entry point
```

Run all four gates the way CI does before pushing:

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest
```

`pre-commit` runs ruff (with `--fix`) and mypy on every `git commit`. If a hook rewrites a file the
commit aborts by design — `git add -A` and commit again. Install it once per clone with
`uv run pre-commit install`. Hook versions in `.pre-commit-config.yaml` are pinned separately from
`pyproject.toml`, so bump both together (`uv run pre-commit autoupdate`).

CI (`.github/workflows/ci.yml`) runs the same four steps across Python 3.11–3.14 via `uv sync`, so
it honors `uv.lock` rather than resolving fresh. It triggers on every PR and on pushes to `main`.

`.github/workflows/publish.yml` handles releases: pushing a `vX.Y.Z` tag builds, checks the tag
against the version in `pyproject.toml` and uploads to Test PyPI; publishing the GitHub release
for that tag uploads to PyPI. Both use trusted publishing, so there is no token in the repo.

## Python version

`requires-python` is `>=3.11` and CI tests 3.11 through 3.14, while `.python-version` pins local
dev to 3.14. **Code must stay valid on 3.11** — don't reach for newer syntax because the local
interpreter accepts it. Both `ruff` (`target-version = "py311"`) and `mypy`
(`python_version = "3.11"`) are configured to enforce that floor locally, so violations surface
before CI.

## Conventions

- Branches: `aghasabeh/thr-<n>-<milestone>-<kebab-slug>` (e.g. `aghasabeh/thr-6-m1-linttype-check-config-ruff-mypy-pre-commit`).
- Commits and PR titles: `THR-<n>: M<milestone>: <description>`.
- **Branching model:** feature branches PR into `dev`; `main` is release-only. Always open PRs with
  `gh pr create --base dev` — `main` is the GitHub default branch, so the base must be set
  explicitly or the PR targets the wrong place.
- **Docstrings are a lint gate.** ruff's pydocstyle rules (`D`) run over `src/`; anything public
  needs a docstring. `D1` is off for tests and examples.
- **Keep `README.md` and `CHANGELOG.md` current.** Any commit that adds, removes, or changes
  user-facing API or behaviour must update both in the same commit -- the op tables, the
  examples, the roadmap checkboxes, and the Unreleased section. Every code block in the README
  is expected to run; verify before committing.
- `.env` is gitignored and holds `LINEAR_API_KEY`.
