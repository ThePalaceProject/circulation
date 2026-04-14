# palace-util

Small, dependency-light utilities shared across [Palace Project](https://thepalaceproject.org)
Python packages. Lives in the `palace.util` namespace.

This package exists so that the handful of cross-cutting helpers that *every* Palace package needs
— a common exception hierarchy, timezone-aware datetime handling, a standardized
`LoggerMixin` — can be depended on without pulling in the heavier `palace-manager` application.
Packages that need only these primitives depend on `palace-util` directly; `palace-manager` is
also just a consumer.

## Scope

`palace-util` sits at the bottom of the Palace dependency graph: **other Palace packages depend
on `palace-util`; `palace-util` depends on no other Palace package.**

If a utility is only useful inside the Palace Manager application, it does not belong here —
keep it in `src/palace/manager/util/`. Candidates for `palace-util` should be:

- **Reusable** outside the manager application (e.g., by other Palace services, CLI tools, or
  by future extracted packages like `palace-opds`).
- **Runtime-dependency-light** — stdlib-only is the ideal, and in the rare case a third-party
  library is justified it must be tiny and widely-used. Consumers of `palace-util` should be
  able to add it to their `pyproject.toml` without materially growing their install footprint.
- **No intra-Palace dependencies**.
- **Stable** enough that the wider ecosystem can rely on it without expecting frequent breaking
  changes.

## Development

This package is a [`uv` workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/) member
of the main [`circulation`](../../README.md) repository. Work on it from the repo root — `uv sync`
picks up all workspace members automatically; `tox -e py312-docker` runs the full test suite.
Tests for this package live at the repository root under `tests/palace_util/` (workspace-member
tests are kept under the root `tests/` tree so they share the repo's pytest fixtures and conftest
plugins).
