"""Palace Brainfuck: an interpreter for the reference Brainfuck language
(Urban Muller, 1993), extended with a single additional instruction, ``@``,
for calling a registered host-language subroutine.

A Palace Brainfuck program consists of the eight standard instructions
(``><+-.,[]``) plus zero or more ``@name@`` call sites, where ``name`` is a
key into the ``functions`` mapping supplied to :func:`run_palace_brainfuck`.
Any other character is a comment and is ignored, per the reference language.

A called function receives a :class:`PalaceBrainfuckVM` holding the current
tape, pointer, input, and output, and may read or write any of them; it may
also call :func:`run_palace_brainfuck` again itself, typically with a small
self-contained program and a fresh VM, to make full use of the host
language's standard library from within an otherwise deliberately
impoverished one.

The VM is a snapshot rather than a window onto live memory. Whatever the
function has written when it returns is copied back and execution resumes
from there, so mutations made to the object after that point have no effect,
and each call site is handed its own instance.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TypeAlias

from palace.util.exceptions import PalaceValueError

from palace.brainfuck_vm._native import run_palace_brainfuck_native


class PalaceBrainfuckError(PalaceValueError): ...


@dataclass
class PalaceBrainfuckVM:
    """A snapshot of one Palace Brainfuck program's execution state.

    Unlike the program constants elsewhere in this package, this is
    deliberately not immutable: it exists specifically for a foreign
    function to read and write. What it holds when that function returns is
    copied back into the running interpreter.

    :param tape: The cell tape. Cell values wrap modulo 256. Resizing it
        resizes the interpreter's tape; the pointer must land inside the
        result by the time the next instruction reads or writes a cell.
    :param pointer: Index of the currently selected cell.
    :param input_bytes: The bytes available to ``,``. Replacing it replaces
        the interpreter's input; ``input_pos`` still indexes into whatever
        the function leaves here.
    :param input_pos: Index of the next byte ``,`` will read.
    :param output: Bytes written so far by ``.``.
    """

    tape: bytearray
    pointer: int = 0
    input_bytes: bytes = b""
    input_pos: int = 0
    output: bytearray = field(default_factory=bytearray)


PalaceBrainfuckForeignFunction: TypeAlias = Callable[[PalaceBrainfuckVM], None]


def run_palace_brainfuck(
    program: str,
    input_bytes: bytes = b"",
    *,
    tape_size: int = 30000,
    functions: Mapping[str, PalaceBrainfuckForeignFunction] | None = None,
) -> bytes:
    """Run a Palace Brainfuck program to completion and return its output.

    Execution itself happens in ``_native``, a compiled Rust interpreter for
    this same language: see this package's own README for how it stays
    behaviorally identical to this module's documented semantics despite
    owning the tape and I/O buffers natively.

    :param program: Palace Brainfuck source. Characters other than
        ``><+-.,[]@`` are ignored.
    :param input_bytes: Bytes made available to the ``,`` instruction. Once
        exhausted, ``,`` sets the current cell to ``0``.
    :param tape_size: Number of cells on the tape. Must be at least one.
    :param functions: Foreign functions callable via ``@name@``.
    :return: The bytes written by the program's ``.`` instructions.
    :raise PalaceBrainfuckError: If brackets are unbalanced, an ``@`` escape
        is unterminated or names an unregistered function, ``tape_size`` is
        zero, or the pointer moves outside the tape.
    """
    return run_palace_brainfuck_native(program, input_bytes, tape_size, functions or {})
