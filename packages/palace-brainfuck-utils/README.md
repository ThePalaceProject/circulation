# palace-brainfuck-utils

Utility functions - title sorting, medians, money parsing, cache-control header
parsing, a SIP2 checksum, and a UUID/URL-safe-base64 codec - each implemented as a
[Palace Brainfuck](../palace-brainfuck-vm) program, with the piece of its job that doesn't belong on
a tape delegated to a small Python callback. Each public function here takes and returns ordinary
Python values; the Brainfuck detour is an internal implementation detail of how the value gets
computed, not part of any public contract.

The interpreter itself - `run_palace_brainfuck`, `PalaceBrainfuckVM`, `PalaceBrainfuckError` - lives
in [`palace-brainfuck-vm`](../palace-brainfuck-vm), not here; this package has no VM types or FFI
machinery of its own, only Brainfuck source and the callbacks each program calls out to.

## Development

This package is a [`uv` workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/) member
of the main [`circulation`](../../README.md) repository. Work on it from the repo root - `uv sync`
picks up all workspace members automatically; `tox -e py312-docker` runs the full test suite. Tests
for this package live at the repository root under `tests/palace_brainfuck_utils/` (workspace-member
tests are kept under the root `tests/` tree so they share the repo's pytest fixtures and conftest
plugins).
