# palace-opds

Pydantic models for the OPDS / Readium specifications used across [Palace
Project](https://thepalaceproject.org) Python packages — OPDS 2.0, Readium Web Publication
Manifest, ODL, LCP, Authentication for OPDS, plus Palace-specific extensions. Lives in the
`palace.opds` namespace.

This package exists so that the OPDS schema models — which are inherently shared between the
Palace Manager application, ingestion tools, and any other Palace service that talks OPDS — can be
depended on without pulling in the heavier `palace-manager` application (databases, Flask, Celery,
and friends). Packages that need to parse, validate, or emit OPDS payloads depend on `palace-opds`
directly; `palace-manager` is also just a consumer.

## Scope

`palace-opds` sits near the bottom of the Palace dependency graph alongside `palace-util`: **other
Palace packages depend on `palace-opds`; `palace-opds` depends only on `palace-util` (no other
`palace-*` package).** This is a hard rule — introducing a dependency on `palace-manager` (or any
other downstream Palace package) defeats the whole point of extracting the package.

If a model is only useful inside the Palace Manager application, it does not belong here — keep it
under `src/palace/manager/`. Candidates for `palace-opds` should be:

- **Faithful to a published spec or widely-shared OPDS extension.** Models in this package
  describe formats that are exchanged across system boundaries, so they have an external contract
  and should change carefully.
- **Reusable** outside the manager application (e.g., by other Palace services, ingestion CLIs,
  or future extracted packages).
- **Runtime-dependency-light** — only the validation/serialization libraries that the OPDS models
  genuinely need (Pydantic, pydantic-xml, pycountry, uritemplate). Anything that would drag in
  Flask, SQLAlchemy, boto3, Celery, or similar heavyweights does not belong here.
- **Workspace-internal palace deps only via `palace-util`** — see the hard rule above.
- **Stable** enough that the wider ecosystem can rely on it without expecting frequent breaking
  changes.

## Stability

Versioned alongside the rest of the monorepo via `dunamai` at build time. The API is considered
internal-to-Palace for now: breaking changes are allowed but should be called out in commit
messages and ideally coordinated with consumers.

## Development

This package is a [`uv` workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/) member
of the main [`circulation`](../../README.md) repository. Work on it from the repo root — `uv sync`
picks up all workspace members automatically; `tox -e py312-docker` runs the full test suite.
Tests for this package live at the repository root under `tests/palace_opds/` (workspace-member
tests are kept under the root `tests/` tree so they share the repo's pytest fixtures and conftest
plugins).
