"""The Palace Brainfuck program backing ``SIPClient.append_checksum``."""

from __future__ import annotations

from typing import Final

from palace.brainfuck_vm.palace_brainfuck import run_palace_brainfuck

SIP2_CHECKSUM_BF: Final[str] = (
    ",[->+>+<<]>>[-<<+>>]<----------[[-]<[>>>+[->>+>+<<<]>>>[-<<<+>>>]>+<<[>>-<<[-]]>"
    ">[-<<<+>>>]<<<<<<<-],[->+>>>>>>>+<<<<<<<<]>>>>>>>>[-<<<<<<<<+>>>>>>>>]<<<<<<<---"
    "-------]>>[->>>>>>+>+<<<<<<<]>>>>>>>[-<<<<<<<+>>>>>>>]>+<<[>>-<<[-]]<<<<<[->>>>>"
    ">>>+>+<<<<<<<<<]>>>>>>>>>[-<<<<<<<<<+>>>>>>>>>]>+<<[>>-<<[-]]<[->>>>+<<<<]>>>[->"
    "+<]>-->+<[>-<[-]]>>+<[->>+>+<<<]>>>[-<<<+>>>]<[<->[-]]<<[>>>>+++++++++++++++++++"
    "++++++++++++++++++++++++++++++.>++++++++++++++++++++++++++++++++++++++++++++++++"
    ".>++++++++++++++++++++++++++++++++++++++++++++++++.>++++++++++++++++++++++++++++"
    "++++++++++++++++++++.>++++++++   ⣤⠀⠀⠀⢀⣠⡀⢀⣠⠀⠀⠀⠰⡆   ++++++++++++++++++++++++++++++"
    "++++++++++.<<<<<<<<[-]]>[   ⢀⠀⠀⠀⠀⠙⠢⠤⣄⣿⣟⣿⣿⣿⣇⡤⠔⠊   >>>>>>>>-<<<<<<<<<<<<<<<<<<<<<<"
    "[->>>>>>>>>>>>>>>>>>>>>>-<   ⠂⠀⠀⠀⠀⠀⠀⣾⣿⣿⣿⣿⢿⣿⡆⠀⠀⠀⠀⠀⠀⡐   <<<<<<<<<<<<<<<<<<<<<]>>>>"
    ">>>>>>>>>>>>>>>>>>>-<<<<<   ⠐⣾⣤⠀⠀⢀⠄⣲⣾⣿⣿⣭⣯⣷⣿⣶⠂⢄⠀⠀⣰⣨⣄   <<<<<<<<<<<<<<<<<[->>>>>>>"
    ">>>>>>>>>>>>>>>-<<<<<<<<<   ⠰⣻⡏⠀⣠⠷⣺⣿⣿⣿⣿⣿⣿⣿⠻⣿⣷⡄⢷⡀⠸⣟⡅   <<<<<<<<<<<<<]>>>>>>>>>>>>"
    ">>>>>>>>>+[->>+>+<<<]>>>[-   ⠹⢷⣰⠁⣠⣿⣿⡽⣿⣿⣿⣿⣁⠀⠈⠻⣿⡄⢻⢼⡻⠂   <<<+>>>]>+<<[>>-<<[-]]>>[-"
    "<<<+>>>]>>>++++++++++++++++   ⠹⠁⠀⣿⡿⣦⣴⣿⣿⣿⣿⣿⣷⡄⠀⢹⣷⠀⢪⠂   <<<<<<[>>>>+>>-[->+>+<<]>>["
    "-<<+>>]>+<<[>>-<<[-]]>>[<<<<   ⢀⣀⡀⣀⣀⣝⠿⢿⣿⣿⣿⣿⣧⣵⣼⣜⠀⠂   +<[-]>>++++++++++++++++>>>[-"
    "]]<<<<<<<<<-]>>>>>>[-]>>>>   ⣀⣠⣼⠂⠀⠀⠹⡝⠻⡛⠛⠛⠟⠉⠉⢹⣯⣶⣖⣄⡀   >>++++++++++<<<<<<<[>>>>>+>"
    ">-[->+>+<<]>>[-<<+>>]>+<   ⣆⡥⠛⢡⡛⠃⣐⢹⢮⣀⣾⣿⣶⣿⣿⣦⠀⠀⠁⠀⠿⢫⠙⢣⣔⣄   <[>>-<<[-]]>>[<<<<+<[-]>"
    ">++++++++++>>>[-]]<<<<<   ⢱⡿⠁⠀⠨⠷⡀⡧⠘⣿⣿⣿⣿⣿⣿⣿⣿⣶⡸⠸⠁⠘⡶⠀⠀⠙⣿⣐   <<<<<-]>>>>>>>[-]<<++++"
    "+++++++++++++++++++++++   ⢸⠇⠀⠀⢸⢨⠈⠨⠠⢽⣿⣿⣿⣿⣿⣿⣿⣿⣷⣽⡁⣼⣟⡀⠀⠀⢻⡄   +++++++++++++++++++++>["
    "-<+++++++++++++++++>]<.   ⢸⠀⠀⢀⣼⣇⠀⢰⠄⠈⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡿⣿⣿⣿⣆⠀⠘⡅   >>>>>>>>++++++++++<<<<<"
    "<<<<<<<<<[>>>>>>>>>>>>+   ⠎⠀⠀⢾⡟⢿⡀⠈⠐⠀⠻⠿⣿⣿⣿⣿⣿⣿⣿⣿⣗⣿⠇⠈⣿⠀⠀⠇   >>-[->+>+<<]>>[-<<+>>]>"
    "+<<[>>-<<[-]]>>[<<<<+<[-]>   ⣿⡆⠈⢻⣄⠀⠀⠀⢀⡟⣻⠻⠯⢿⣿⣿⣿⣿⠎⠀⠀⣿⡄⠀⠈   >++++++++++>>>[-]]<<<<<"
    "<<<<<<<<<<<<-]>>>>>>>>>>>>   ⣽⡇⠀⠀⠉⠳⣄⣀⣈⣐⣺⣷⣿⣿⣿⡿⠋⠀⠀⠀⢀⡿⣇   >>[-]<<++++++++++++++++++"
    "++++++++++++++++++++++++++   ⠿⣷⡀⠀⠀⠀⠀⠉⠙⠛⣋⣙⠋⠉⠁⠀⠀⠀⠀⠀⣸⣼   ++++>[-<+++++++++++++++++>"
    "]<.>>>>>>>>+++++++++++++++   ⠈⠯⣷⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣴⢿⠈   +<<<<<<<<<<<<<<<<<<<<<<<<<"
    "[>>>>>>>>>>>>>>>>>>>>>>>+>>-   ⠐⠝⢦⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡼⠃⠆   [->+>+<<]>>[-<<+>>]>+<<[>>-"
    "<<[-]]>>[<<<<+<[-]>>+++++++++++   ⠈⠑⢄⠀⠀⠀⠀⠀⠀⠀⠀⠀⡜   +++++>>>[-]]<<<<<<<<<<<<<<<<<<"
    "<<<<<<<<<<-]>>>>>>>>>>>>>>>>>>>>>>>>>[-]>>>>>>++++++++++<<<<<<<[>>>>>+>>-[->+>+<"
    "<]>>[-<<+>>]>+<<[>>-<<[-]]>>[<<<<+<[-]>>++++++++++>>>[-]]<<<<<<<<<<-]>>>>>>>[-]<"
    "<++++++++++++++++++++++++++++++++++++++++++++++++>[-<+++++++++++++++++>]<.>>>>>>"
    ">>++++++++++<<<<<<<<<<<<<<[>>>>>>>>>>>>+>>-[->+>+<<]>>[-<<+>>]>+<<[>>-<<[-]]>>[<"
    "<<<+<[-]>>++++++++++>>>[-]]<<<<<<<<<<<<<<<<<-]>>>>>>>>>>>>>>[-]<<+++++++++++++++"
    "+++++++++++++++++++++++++++++++++>[-<+++++++++++++++++>]<.<<<<<<<<<<<<<<<<<<<<<<"
    "<<<<<<<<<<<<<<<<<<<<<[-]]"
)


# The program reads bytes until it meets this one, so nothing it is asked to
# sum may take this value.
_TERMINATOR: Final = 10
# The largest total a single tape cell can carry.
_MAX_CELL: Final = 255


def _checksum_wire(text: str) -> bytes:
    """Encode ``text`` as bytes totalling ``sum(ord(c) for c in text)``.

    The program adds up the bytes it reads, so any encoding that preserves
    that total computes the same checksum. Handing it ``text.encode()``
    would not: above U+007F a character becomes several UTF-8 bytes whose
    values bear no relation to its code point, and the sum comes out wrong.

    Each code point is therefore spread over as many bytes as it takes, and
    a byte that would land on the terminator is nudged down by one with the
    difference carried into the next, so no character can end the program
    early. Text within ASCII encodes one byte per character either way.
    """
    wire = bytearray()
    for character in text:
        remaining = ord(character)
        while remaining:
            part = min(remaining, _MAX_CELL)
            if part == _TERMINATOR:
                part -= 1
            wire.append(part)
            remaining -= part
    wire.append(_TERMINATOR)
    return bytes(wire)


def compute_sip2_checksum(text: str) -> str:
    """Compute the checksum SIP2 appends to a message: the sum of every
    character's ordinal value, XORed with ``0xFFFF``, plus one, formatted
    as (at least) 4 uppercase hex digits.

    Matches a quirk of the original algorithm: an empty ``text`` sums to
    ``0``, and ``(0 ^ 0xFFFF) + 1 == 65536`` doesn't fit in 4 hex digits,
    so it prints as the 5-digit ``"10000"`` rather than being truncated.

    The running total is 16 bits wide, so a message whose ordinals sum past
    ``0xFFFF`` (very roughly 650 ASCII characters) wraps. The original let
    that total grow without bound and printed the extra digits, which no
    conforming ACS could read out of a 4-digit field, so wrapping is the
    deliberate divergence here rather than an oversight.

    :param text: The fully assembled message the checksum is computed
        over, i.e. after any sequence number field has already been added.
    """
    return run_palace_brainfuck(SIP2_CHECKSUM_BF, _checksum_wire(text)).decode()
