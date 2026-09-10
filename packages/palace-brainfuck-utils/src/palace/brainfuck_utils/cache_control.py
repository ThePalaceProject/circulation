"""The Palace Brainfuck programs backing ``_parse_cache_control``."""

from __future__ import annotations

from typing import Final

from palace.brainfuck_vm.palace_brainfuck import (
    PalaceBrainfuckForeignFunction,
    PalaceBrainfuckVM,
    run_palace_brainfuck,
)

from palace.brainfuck_utils.shared import tape_index

CACHE_CONTROL_BF: Final[str] = "@d@[@d@]"


LOWERCASE_BF: Final[str] = (
    ",>[-]<[->+<>>+<<]>>[-<<+>>]<<>[<>>>[-]+<<<>[-]<[->+<>>+<<]>>[-<<+>>]<<>---------"
    "--------------------------------------------------------[>>[-]<<[-]]<>>>[@l@[-]]"
    "<<<>>>[-]+<<<>[-]<[->+<>>+<<]>>[-<<+>>]<<>--------------------------------------"
    "----------------------------[>>[-]<<[-]]<>>>[@l@[-]]<<<>>>[-]+<<<>[-]<[->+<>>+<<"
    "]>>[-<<+>>]<<>------------------------------------------------------------------"
    "-[>>[-]<<[-]]<>>>[@l@[-]]<<<>>>[-]+<<<>[-]<[->+<>>+<<]>>[-<<+>>]<<>-------------"
    "-------------------------------------------------------[>>[-]<<[-]]<>>>[@l@[-]]<"
    "<<>>>[-]+<<<>[-]<[->+<>>+<<]>>[-<<+>>]<<>---------------------------------------"
    "------------------------------[>>[-]<<[-]]<>>>[@l@[-]]<<<>>>[-]+<<<>[-]<[->+<>>+"
    "<<]>>[-<<+>>]<<>----------------------------------------------------------------"
    "------[>>[-]<<[-]]<>>>[@l@[-]]<<<>>>[-]+<<<>[-]<[->+<>>+<<]>>[-<<+>>]<<>--------"
    "---------------------------------------------------------------[>>[-]<<[-]]<>>>["
    "@l@[-]]<<<>>>[-]+<<<>[-]<[->+<>>+<<]>>[-<<+>>]<<>-------------------------------"
    "-----------------------------------------[>>[-]<<[-]]<>>>[@l@[-]]<<<>>>[-]+<<<>["
    "-]<[->+<>>+<<]>>[-<<+>>]<<>-----------------------------------------------------"
    "--------------------[>>[-]<<[-]]<>>>[@l@[-]]<<<>>>[-]+<<<>[-]<[->+<>>+<<]>>[-<<+"
    ">>]<<>--------------------------------------------------------------------------"
    "[>>[-]<<[-]]<>>>[@l@[-]]<<<>>>[-]+<<<>[-]<[->+<>>+<<]>>[-<<+>>]<<>--------------"
    "-------------------------------------------------------------[>>[-]<<[-]]<>>>[@l"
    "@[-]]<<<>>>[-]+<<<>[-]<[->+<>>+<<]>>[-<<+>>]<<>---------------------------------"
    "-------------------------------------------[>>[-]<<[-]]<>>>[@l@[-]]<<<>>>[-]+<<<"
    ">[-]<[->+<>>+<<]>>[-<<+>>]<<>---------------------------------------------------"
    "--------------------------[>>[-]<<[-]]<>>>[@l@[-]]<<<>>>[-]+<<<>[-]<[->+<>>+<<]>"
    ">[-<<+>>]<<>--------------------------------------------------------------------"
    "----------[>>[-]<<[-]]<>>>[@l@[-]]<<<>>>[-]+<<<>[-]<[->+<>>+<<]>>[-<<+>>]<<>----"
    "---------------------------------------------------------------------------[>>[-"
    "]<<[-]]<>>>[@l@[-]]<<<>>>[-]+<<<>[-]<[->+<>>+<<]>>[-<<+>>]<<>-------------------"
    "-------------------------------------------------------------[>>[-]<<[-]]<>>>[@l"
    "@[-]]<<<>>>[-]+<<<>[-]<[->+<>>+<<]>>[-<<+>>]<<>---------------------------------"
    "------------------------------------------------[>>[-]<<[-]]<>>>[@l@[-]]<<<>>>[-"
    "]+<<<>[-]<[->+<>>+<<]>>[-<<+>>]<<>----------------------------------------------"
    "------------------------------------[>>[-]<<[-]]<>>>[@l@[-]]<<<>>>[-]+<<<>[-]<[-"
    ">+<>>+<<]>>[-<<+>>]<<>----------------------------------------------------------"
    "-------------------------[>>[-]<<[-]]<>>>[@l@[-]]<<<>>>[-]+<<<>[-]<[->+<>>+<<]>>"
    "[-<<+>>]<<>---------------------------------------------------------------------"
    "---------------[>>[-]<<[-]]<>>>[@l@[-]]<<<>>>[-]+<<<>[-]<[->+<>>+<<]>>[-<<+>>]<<"
    ">-------------------------------------------------------------------------------"
    "------[>>[-]<<[-]]<>>>[@l@[-]]<<<>>>[-]+<<<>[-]<[->+<>>+<<]>>[-<<+>>]<<>--------"
    "------------------------------------------------------------------------------[>"
    ">[-]<<[-]]<>>>[@l@[-]]<<<>>>[-]+<<<>[-]<[->+<>>+<<]>>[-<<+>>]<<>----------------"
    "-----------------------------------------------------------------------[>>[-]<<["
    "-]]<>>>[@l@[-]]<<<>>>[-]+<<<>[-]<[->+<>>+<<]>>[-<<+>>]<<>-----------------------"
    "-----------------------------------------------------------------[>>[-]<<[-]]<>>"
    ">[@l@[-]]<<<>>>[-]+<<<>[-]<[->+<>>+<<]>>[-<<+>>]<<>-----------------------------"
    "------------------------------------------------------------[>>[-]<<[-]]<>>>[@l@"
    "[-]]<<<>>>[-]+<<<>[-]<[->+<>>+<<]>>[-<<+>>]<<>----------------------------------"
    "--------------------------------------------------------[>>[-]<<[-]]<>>>[@l@[-]]"
    "<<<.,>[-]<[->+<>>+<<]>>[-<<+>>]<<>]<@_@"
)


VALIDATE_INT_BF: Final[str] = "@#@"


def _to_lower_callback(vm: PalaceBrainfuckVM) -> None:
    # `_emit_call_if_equals`'s callback runs with the pointer three cells to
    # the right of the tested cell (see `LOWERCASE_BF`'s construction).
    raw_index = tape_index(vm, -3)
    vm.tape[raw_index] = (vm.tape[raw_index] + 32) % 256


def _parse_int_callback(vm: PalaceBrainfuckVM) -> None:
    try:
        value = int(vm.input_bytes.decode())
    except ValueError:
        return
    vm.output.extend(str(value).encode())


_ASCII_WHITESPACE: Final[bytes] = b" \t\n\r\x0b\x0c"


def _strip_output_callback(vm: PalaceBrainfuckVM) -> None:
    # Stands in for the `str.strip()` the original parser applied to each
    # directive. `LOWERCASE_BF` echoes surrounding whitespace through
    # untouched and leaves the trimming to this, so both ends have to go.
    stripped = bytes(vm.output).strip(_ASCII_WHITESPACE)
    vm.output.clear()
    vm.output.extend(stripped)


def _make_handle_directive_callback(
    result: dict[str, int | None],
) -> PalaceBrainfuckForeignFunction:
    def _handle_directive(vm: PalaceBrainfuckVM) -> None:
        data = vm.input_bytes
        end = len(data)
        start = vm.input_pos
        comma_pos = data.find(b",", start, end)
        segment_end = comma_pos if comma_pos != -1 else end
        raw_segment = data[start:segment_end]
        vm.input_pos = segment_end + 1 if comma_pos != -1 else end
        vm.tape[vm.pointer] = 1 if vm.input_pos < end else 0

        lowered = run_palace_brainfuck(
            LOWERCASE_BF,
            raw_segment,
            functions={
                "l": _to_lower_callback,
                "_": _strip_output_callback,
            },
        ).decode()
        if not lowered:
            return

        if "=" in lowered:
            key, value_str = lowered.split("=", 1)
            parsed = run_palace_brainfuck(
                VALIDATE_INT_BF,
                value_str.encode(),
                functions={"#": _parse_int_callback},
            )
            if parsed:
                result[key] = int(parsed.decode())
        else:
            result[lowered] = None

    return _handle_directive


def parse_cache_control(cache_control_header: str | None) -> dict[str, int | None]:
    """Parse a Cache-Control header into a dictionary of directives.

    https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Cache-Control

    :param cache_control_header: The raw header value, or ``None``.
    """
    if not cache_control_header:
        return {}
    result: dict[str, int | None] = {}
    callback = _make_handle_directive_callback(result)
    run_palace_brainfuck(
        CACHE_CONTROL_BF,
        cache_control_header.encode(),
        functions={"d": callback},
    )
    return result
