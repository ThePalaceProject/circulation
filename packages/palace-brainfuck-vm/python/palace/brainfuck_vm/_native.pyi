from collections.abc import Mapping

from palace.brainfuck_vm.palace_brainfuck import PalaceBrainfuckForeignFunction

__version__: str

def run_palace_brainfuck_native(
    program: str,
    input_bytes: bytes,
    tape_size: int,
    functions: Mapping[str, PalaceBrainfuckForeignFunction],
) -> bytes: ...
