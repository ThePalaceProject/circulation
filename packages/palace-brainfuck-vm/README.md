# palace-brainfuck-vm

Palace Brainfuck: an in-house Brainfuck dialect with a bidirectional Python FFI (`@name@` calls out
to a registered Python function; that function may itself call back into Brainfuck).

This package owns the whole interpreter. `palace.brainfuck_vm.palace_brainfuck` is the public
surface (`PalaceBrainfuckVM`, `PalaceBrainfuckError`, `run_palace_brainfuck`) and documents the
language; `palace.brainfuck_vm._native` is the compiled Rust extension that actually executes
programs.

The Rust side owns the tape, pointer, and I/O buffers, and runs the instruction loop with the GIL
released so a long-running program does not block the other threads in the process. It re-acquires
the GIL only at `@name@` call sites: it copies its current state into a `PalaceBrainfuckVM`
instance, invokes the registered Python callback with that instance, then reads whatever the
callback mutated back out before resuming. Foreign functions are unmodified Python and never need to
know which interpreter is driving them.

Because state crosses that boundary by value, the VM a callback receives is a snapshot rather than a
window onto live memory: what it has written when it returns is applied, anything it does to the
object afterwards is not, and each call site gets its own instance.

[`palace-brainfuck-utils`](../palace-brainfuck-utils) depends on this package for the interpreter
itself; it has no Brainfuck source or VM types of its own.

To build and install locally for development:

```bash
uv run maturin develop --release -m packages/palace-brainfuck-vm/Cargo.toml
```

Tests for this package live at the repository root under `tests/palace_brainfuck_vm/` (workspace-member
tests are kept under the root `tests/` tree so they share the repo's pytest fixtures and conftest
plugins). The interpreter is covered from Python rather than with `cargo test`, so that the behaviour
under test is the one callers actually get, across the FFI boundary.
