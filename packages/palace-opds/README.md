# palace-opds

Pydantic models for the [OPDS](https://opds.io/) and [Readium](https://readium.org/) family of
specifications used in digital-library publishing — read, validate, and emit feed and license
documents with type-safe Python objects.

## What's covered

- **OPDS 2.0** publication and feed documents, including facets, navigation groups, and
  publication groups.
- **Readium Web Publication Manifest (RWPM)** — link models, presentation hints,
  encryption metadata.
- **OPDS for Distributors / Authentication for OPDS 1.0** — authentication documents and
  link relations.
- **ODL 1.0** (Open Distribution to Libraries) — license metadata, terms, protection,
  loan/checkout status.
- **LCP** (Readium Licensed Content Protection) — license documents and loan-status
  (LSD) documents.
- **W3C accessibility metadata** and **schema.org publication vocabulary** for the slices
  that OPDS feeds reference.
- **Palace-specific extensions** to the publication metadata vocabulary (under
  `palace.opds.palace`) — these can be safely ignored if you don't need them.

## Installation

```bash
pip install palace-opds   # or: uv add palace-opds
```

Requires Python 3.12+.

## Quick example

```python
from palace.opds.opds2 import PublicationFeed

feed = PublicationFeed.model_validate_json(raw_feed_bytes)
for publication in feed.publications:
    print(publication.metadata.title, publication.metadata.identifier)
```

Models live under the `palace.opds` namespace and follow Pydantic v2 conventions, so they
serialize to/from JSON and dictionaries with `.model_dump()` / `.model_validate()`.

## Design

- **Spec-faithful**, not opinionated. Where the OPDS / Readium specs allow flexibility, the
  models accept it; we don't impose application-specific constraints.
- **Lean dependencies.** Only the validation and serialization libraries the models actually
  need (`pydantic`, `pycountry`, `uritemplate`) — no web framework, ORM, or cloud SDK is
  pulled in transitively.
- **Stable, documented breakage.** Breaking changes follow semver and are called out in
  release notes; minor releases are additive.

## Project context

`palace-opds` is developed and maintained as part of the [Palace
Project](https://thepalaceproject.org), where it powers OPDS ingestion and feed generation
for the [Palace Manager](https://github.com/ThePalaceProject/circulation) digital-library
backend. It's published as a standalone package because the OPDS specs are widely shared —
other library systems, ingestion pipelines, or research tools that work with OPDS feeds can
use these models without taking on Palace Manager as a dependency.

Outside contributions and bug reports are welcome via the
[ThePalaceProject/circulation](https://github.com/ThePalaceProject/circulation) repository,
where this package is developed as a [`uv`
workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/) member.

## License

Apache 2.0.
