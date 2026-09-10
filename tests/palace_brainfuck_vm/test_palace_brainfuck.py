import pytest
from palace.brainfuck_vm.palace_brainfuck import (
    PalaceBrainfuckError,
    PalaceBrainfuckVM,
    run_palace_brainfuck,
)

HELLO_WORLD_BF = (
    "++++++++[>++++[>++>+++>+++>+<<<<-]>+>+>->>+[<]<-]"
    ">>.>---.+++++++..+++.>>.<-.<.+++.------.--------.>>+.>++."
)


class TestRunPalaceBrainfuck:
    def test_hello_world(self):
        assert run_palace_brainfuck(HELLO_WORLD_BF) == b"Hello World!\n"

    def test_echo_roundtrip(self):
        assert run_palace_brainfuck(",[.,]", b"round trip") == b"round trip"

    def test_unmatched_open_bracket_raises(self):
        with pytest.raises(PalaceBrainfuckError):
            run_palace_brainfuck("[+")

    def test_unmatched_close_bracket_raises(self):
        with pytest.raises(PalaceBrainfuckError):
            run_palace_brainfuck("+]")

    def test_cell_wraps_on_overflow(self):
        assert run_palace_brainfuck("+" * 256 + ".") == b"\x00"

    def test_cell_wraps_on_underflow(self):
        assert run_palace_brainfuck("-.") == b"\xff"

    def test_pointer_underflow_raises(self):
        with pytest.raises(PalaceBrainfuckError):
            run_palace_brainfuck("<")

    def test_pointer_overflow_raises(self):
        with pytest.raises(PalaceBrainfuckError):
            run_palace_brainfuck(">", tape_size=1)

    def test_reading_past_end_of_input_yields_zero(self):
        assert run_palace_brainfuck(",.", b"") == b"\x00"

    def test_reading_past_end_of_input_repeatedly_yields_zero(self):
        # The read position must stop at the end of the input rather than
        # running past it, or a second exhausted `,` would read out of bounds.
        assert run_palace_brainfuck(",>,>,.", b"a") == b"\x00"

    def test_non_command_characters_are_ignored(self):
        assert run_palace_brainfuck("hello+.world") == b"\x01"

    def test_empty_tape_raises(self):
        with pytest.raises(PalaceBrainfuckError, match="at least one cell"):
            run_palace_brainfuck("+.", tape_size=0)

    def test_nested_loops(self):
        # Three nested loops multiplying out to 2 * 3 * 5 = 30.
        assert run_palace_brainfuck("++[->+++[->+++++<]<]>>.") == bytes([30])


class TestPalaceBrainfuckForeignFunctions:
    def test_call_invokes_registered_function_with_the_current_vm_state(self):
        def greet(vm: PalaceBrainfuckVM) -> None:
            vm.output.extend(b"hello from python")

        assert run_palace_brainfuck("@greet@", functions={"greet": greet}) == (
            b"hello from python"
        )

    def test_callback_writing_tape_is_visible_to_later_instructions(self):
        def set_cell_to_65(vm: PalaceBrainfuckVM) -> None:
            vm.tape[vm.pointer] = 65

        assert run_palace_brainfuck("@set@.", functions={"set": set_cell_to_65}) == b"A"

    def test_callback_moving_input_pos_is_respected_by_comma(self):
        def skip_one_byte(vm: PalaceBrainfuckVM) -> None:
            vm.input_pos += 1

        assert (
            run_palace_brainfuck("@skip@,.", b"XY", functions={"skip": skip_one_byte})
            == b"Y"
        )

    def test_callback_replacing_the_input_is_respected_by_comma(self):
        def substitute_input(vm: PalaceBrainfuckVM) -> None:
            vm.input_bytes = b"XY"
            vm.input_pos = 0

        assert (
            run_palace_brainfuck(
                "@sub@,.,.", b"AB", functions={"sub": substitute_input}
            )
            == b"XY"
        )

    def test_callback_moving_pointer_is_respected_by_later_instructions(self):
        def move_right(vm: PalaceBrainfuckVM) -> None:
            vm.pointer += 1
            vm.tape[vm.pointer] = 42

        assert run_palace_brainfuck("@move@.", functions={"move": move_right}) == (
            bytes([42])
        )

    def test_unregistered_function_name_raises(self):
        with pytest.raises(PalaceBrainfuckError):
            run_palace_brainfuck("@nope@")

    def test_unterminated_at_sign_raises(self):
        with pytest.raises(PalaceBrainfuckError):
            run_palace_brainfuck("@nope", functions={"nope": lambda vm: None})

    def test_callback_can_call_run_palace_brainfuck_recursively(self):
        def delegate_to_nested_program(vm: PalaceBrainfuckVM) -> None:
            nested_output = run_palace_brainfuck(",[.,]", b"nested!")
            vm.output.extend(nested_output)

        assert (
            run_palace_brainfuck(
                "@nest@", functions={"nest": delegate_to_nested_program}
            )
            == b"nested!"
        )

    def test_exception_raised_in_callback_propagates(self):
        def explode(vm: PalaceBrainfuckVM) -> None:
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            run_palace_brainfuck("@explode@", functions={"explode": explode})

    def test_callback_shrinking_the_tape_raises_rather_than_reading_past_it(self):
        def truncate(vm: PalaceBrainfuckVM) -> None:
            del vm.tape[3:]

        with pytest.raises(PalaceBrainfuckError, match="out of bounds"):
            run_palace_brainfuck(
                "@cut@>>>>>+", tape_size=100, functions={"cut": truncate}
            )

    def test_callback_shrinking_the_tape_raises_when_a_cell_is_next_touched(self):
        # Nothing moves the pointer here, so the fault surfaces at the `+`
        # rather than at a `>` that walked off the end.
        def truncate(vm: PalaceBrainfuckVM) -> None:
            vm.tape.clear()

        with pytest.raises(PalaceBrainfuckError, match="0 cell tape"):
            run_palace_brainfuck("@cut@+", tape_size=100, functions={"cut": truncate})

    def test_callback_may_leave_the_pointer_off_the_tape_at_the_end_of_a_program(self):
        # The program is over, so nothing is left to dereference and there is
        # no fault to report.
        def wander_off(vm: PalaceBrainfuckVM) -> None:
            vm.pointer = 10**6

        assert run_palace_brainfuck(".@off@", functions={"off": wander_off}) == b"\x00"

    def test_callback_may_leave_the_pointer_off_the_tape_if_a_move_brings_it_back(self):
        def step_left_off_the_tape(vm: PalaceBrainfuckVM) -> None:
            vm.pointer = -1

        assert (
            run_palace_brainfuck(
                "@left@>+.", functions={"left": step_left_off_the_tape}
            )
            == b"\x01"
        )

    def test_callback_growing_the_tape_extends_the_reachable_cells(self):
        def grow(vm: PalaceBrainfuckVM) -> None:
            vm.tape.extend(bytes(10))

        assert (
            run_palace_brainfuck(
                "@grow@" + ">" * 12 + "+.", tape_size=5, functions={"grow": grow}
            )
            == b"\x01"
        )

    def test_callback_identifier_may_contain_instruction_characters(self):
        # Everything between the two `@` is a name, so the `[` here starts no
        # loop and needs no matching `]`.
        def named(vm: PalaceBrainfuckVM) -> None:
            vm.output.extend(b"called")

        assert run_palace_brainfuck("@a[b@", functions={"a[b": named}) == b"called"

    def test_callback_sees_a_snapshot_not_the_running_machine(self):
        # The VM handed to a foreign function is synced back when the call
        # returns, so mutations made after that point are not applied.
        stashed: list[PalaceBrainfuckVM] = []

        def stash_then_mutate_later(vm: PalaceBrainfuckVM) -> None:
            if stashed:
                stashed[0].output.extend(b"too late")
            stashed.append(vm)

        assert (
            run_palace_brainfuck("@f@@f@", functions={"f": stash_then_mutate_later})
            == b""
        )
