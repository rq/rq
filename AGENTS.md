# Repository Guidelines

## Project Structure & Module Organization
- `rq/` hosts the runtime library (workers, scheduling, CLI); keep new code inside the relevant subpackage.
- `tests/` mirrors runtime modules; isolating slow or Redis-heavy scenarios keeps the default run fast.
- `docs/` powers the public site and release notes, while `examples/` contains runnable snippets referenced by docs.
- Shared tooling lives in `pyproject.toml`, `tox.ini`, and `Makefile`; extend these before adding new scripts.

## Build, Test, and Development Commands
- `python -m pip install -e .[dev]` prepares an editable env with linting and test extras.
- `pytest` runs the suite; narrow scope with `pytest tests/queue/test_queue.py::test_enqueue_at_front`.
- `tox -e py311` executes the managed 3.11 env; plain `tox` runs the full matrix with coverage.
- `make lint` triggers `ruff check rq tests`; `make test` builds `tests/Dockerfile` for CI-parity checks.

## Coding Style & Naming Conventions
- Use four-space indentation and keep lines ≤120 characters; `ruff` enforces `E`, `F`, `W`, and import ordering (`I`).
- Prefer single-quoted strings unless escaping is easier, and run `ruff check --fix` before pushing.
- Modules and functions stay snake_case, classes PascalCase, constants UPPER_SNAKE; CLI entry points belong under `rq/cli`.
- Public APIs should be type hinted (`py.typed` advertises them), and import blocks follow the configured first/third-party ordering.

## Testing Guidelines
- Place pytest files beside related code as `test_<module>.py`; share utilities in the nearest `conftest.py`.
- Use markers from `pyproject.toml` (`slow`, `ssl_test`); enable them with `RUN_SLOW_TESTS_TOO=1` or `RUN_SSL_TESTS=1`.
- Track coverage with `pytest --cov rq --cov-config=.coveragerc --cov-report=term-missing` and update docs/examples when behaviour changes.

## Commit & Pull Request Guidelines
- Write concise, imperative commit subjects in the existing style (`Add ForkingWorker`, `Simplify CronScheduler...`), referencing issues via `(#1234)` when relevant.
- PRs should describe behaviour changes, include test evidence (`pytest`, `tox`, or `make test`), and call out documentation updates.
- Let CI finish before requesting review; note skipped checks and update `CHANGES.md` for user-visible changes.

## Security & Configuration Tips
- Follow `SECURITY.md` for disclosure workflow and report vulnerabilities through Tidelift.
- Keep Redis/Valkey credentials and dumps out of the repo; rely on environment variables for connection strings and do not commit `dump.rdb`.
