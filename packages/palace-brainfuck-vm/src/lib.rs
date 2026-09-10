//! A native-speed interpreter for Palace Brainfuck.
//!
//! Implements the language documented in
//! `palace.brainfuck_vm.palace_brainfuck`: the eight standard Brainfuck
//! commands plus the `@name@` foreign-call escape, with the same
//! wraparound, bounds, and EOF conventions.
//!
//! The tape, pointer, and I/O buffers are owned natively here, and the
//! instruction loop runs with the GIL released so that a long-running
//! program does not block the other threads in the process. The GIL is
//! re-acquired only at `@name@` call sites, where the current state is
//! copied into a `PalaceBrainfuckVM` instance, the registered Python
//! callback is invoked with it, and every field the callback left behind is
//! copied back before execution resumes. Foreign functions are unmodified
//! Python and have no way to tell which interpreter is driving them.
//!
//! Because state crosses the boundary by value, a callback sees a snapshot
//! rather than live memory: the mutations it makes before it returns are
//! applied, and anything it does to the object afterwards is not.

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyByteArray, PyBytes, PyDict, PyType};
use std::collections::HashMap;
use std::fmt;

/// The Python module holding the classes this one needs to reach.
const PUBLIC_MODULE: &str = "palace.brainfuck_vm.palace_brainfuck";

/// Marks a position in the jump table that no `[` or `]` occupies. Doubles as
/// the ceiling on program length, since positions are stored as `u32`.
const NO_JUMP: u32 = u32::MAX;

/// One `@name@` call site: the function to invoke, and the index of the
/// closing `@` so the interpreter knows where to resume.
struct CallSite {
    name: String,
    end: usize,
}

/// A defect in the program text, found before execution starts.
#[derive(Debug)]
enum ScanError {
    UnmatchedOpen(usize),
    UnmatchedClose(usize),
    UnterminatedCall(usize),
    ProgramTooLong(usize),
}

impl fmt::Display for ScanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnmatchedOpen(at) => write!(f, "Unmatched '[' at position {at}"),
            Self::UnmatchedClose(at) => write!(f, "Unmatched ']' at position {at}"),
            Self::UnterminatedCall(at) => write!(f, "Unterminated '@' at position {at}"),
            Self::ProgramTooLong(len) => {
                write!(f, "Program is {len} bytes, over the {NO_JUMP} byte limit")
            }
        }
    }
}

/// A fault raised while executing. Carried as plain data rather than a
/// `PyErr` so that the instruction loop can run with the GIL released and
/// build the Python exception once it is back.
enum Fault {
    PointerMovedOutOfBounds(i64),
    PointerOffTape { pointer: i64, tape_len: usize },
    MissingJumpTarget(usize),
}

impl fmt::Display for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PointerMovedOutOfBounds(at) => write!(f, "Pointer moved out of bounds: {at}"),
            Self::PointerOffTape { pointer, tape_len } => write!(
                f,
                "Foreign function left the pointer at {pointer}, \
                 outside its {tape_len} cell tape"
            ),
            Self::MissingJumpTarget(at) => {
                write!(f, "No jump target recorded for bracket at position {at}")
            }
        }
    }
}

/// Why the instruction loop stopped.
enum Halt {
    /// Ran off the end of the program.
    Done,
    /// Reached the `@` at this index. Only the caller can service it, since
    /// invoking the callback needs the GIL.
    Call(usize),
}

/// What [`scan`] works out about a program before it runs: a jump table
/// indexed by program position, holding the partner of the `[` or `]` there
/// and [`NO_JUMP`] everywhere else, plus every call site keyed by the
/// position of its opening `@`.
///
/// The jump table is a flat vector rather than a map because the instruction
/// loop consults it on every iteration of every loop in the program, which
/// makes it the hottest lookup in the interpreter.
type ProgramMap = (Vec<u32>, HashMap<usize, CallSite>);

/// Matches `[`/`]` pairs and locates `@name@` call sites in one pass.
///
/// Palace Brainfuck instructions and call identifiers are always ASCII, and
/// no UTF-8 multi-byte sequence can contain an ASCII byte, so scanning bytes
/// finds exactly the same instructions Python's character-based indexing
/// would. Only the positions quoted in error messages can differ, and then
/// only for programs whose comments contain non-ASCII text.
fn scan(program: &[u8]) -> Result<ProgramMap, ScanError> {
    if program.len() >= NO_JUMP as usize {
        return Err(ScanError::ProgramTooLong(program.len()));
    }

    let mut jumps: Vec<u32> = vec![NO_JUMP; program.len()];
    let mut calls: HashMap<usize, CallSite> = HashMap::new();
    let mut open_stack: Vec<usize> = Vec::new();
    let mut i = 0usize;

    while i < program.len() {
        match program[i] {
            b'[' => open_stack.push(i),
            b']' => {
                let open_index = open_stack.pop().ok_or(ScanError::UnmatchedClose(i))?;
                jumps[open_index] = i as u32;
                jumps[i] = open_index as u32;
            }
            b'@' => {
                let end = program[i + 1..]
                    .iter()
                    .position(|&b| b == b'@')
                    .map(|offset| i + 1 + offset)
                    .ok_or(ScanError::UnterminatedCall(i))?;
                let name = String::from_utf8_lossy(&program[i + 1..end]).into_owned();
                calls.insert(i, CallSite { name, end });
                i = end;
            }
            _ => {}
        }
        i += 1;
    }

    if let Some(&position) = open_stack.first() {
        return Err(ScanError::UnmatchedOpen(position));
    }

    Ok((jumps, calls))
}

/// Looks up the partner of the bracket at `at`.
///
/// [`scan`] fills in both halves of every pair, so a miss means the table and
/// the program have come apart. That is reported as a [`Fault`] rather than
/// left to panic because the instruction loop runs with the GIL released,
/// where a panic would surface as an opaque `PanicException`.
fn jump_target(jumps: &[u32], at: usize) -> Result<usize, Fault> {
    match jumps.get(at).copied() {
        Some(target) if target != NO_JUMP => Ok(target as usize),
        _ => Err(Fault::MissingJumpTarget(at)),
    }
}

/// The execution state of one program: everything a `PalaceBrainfuckVM`
/// exposes to a foreign function, held natively between call sites.
struct Machine {
    tape: Vec<u8>,
    pointer: i64,
    input: Vec<u8>,
    input_pos: usize,
    output: Vec<u8>,
}

impl Machine {
    /// The tape index the pointer selects, or `None` if it has left the tape.
    ///
    /// This deliberately measures the tape's current length rather than the
    /// size it was created with: a foreign function is free to resize
    /// `vm.tape`, and a tape shortened out from under the pointer has to
    /// report a miss here instead of being indexed past its end.
    fn selected(&self) -> Option<usize> {
        let index = usize::try_from(self.pointer).ok()?;
        (index < self.tape.len()).then_some(index)
    }

    /// Checks that `>` or `<` has left the pointer on the tape.
    fn check_move(&self) -> Result<(), Fault> {
        match self.selected() {
            Some(_) => Ok(()),
            None => Err(Fault::PointerMovedOutOfBounds(self.pointer)),
        }
    }

    /// Resolves the cell a dereferencing instruction is about to act on.
    ///
    /// `>` and `<` check themselves as they move, so the only way this can
    /// fail is a foreign function that moved the pointer or shortened the
    /// tape while the interpreter was away.
    fn cell(&self) -> Result<usize, Fault> {
        self.selected().ok_or(Fault::PointerOffTape {
            pointer: self.pointer,
            tape_len: self.tape.len(),
        })
    }

    /// Runs instructions from `start` until the program ends or reaches a
    /// call site. Holds no Python state, so the caller can run it with the
    /// GIL released.
    ///
    /// A foreign function may have moved the pointer or resized the tape
    /// while we were away, so nothing here assumes the pointer is on the tape
    /// on entry. Every instruction that touches a cell resolves it for
    /// itself, which leaves a move, a call, a comment, and a program with no
    /// instructions left at all free to run whatever the callback left behind.
    ///
    /// `jumps` must come from [`scan`] over this same `program`, which
    /// guarantees an entry for every `[` and `]` the loop can reach.
    fn run(&mut self, program: &[u8], jumps: &[u32], start: usize) -> Result<Halt, Fault> {
        let mut i = start;
        while i < program.len() {
            match program[i] {
                b'>' => {
                    self.pointer += 1;
                    self.check_move()?;
                }
                b'<' => {
                    self.pointer -= 1;
                    self.check_move()?;
                }
                b'+' => {
                    let cell = self.cell()?;
                    self.tape[cell] = self.tape[cell].wrapping_add(1);
                }
                b'-' => {
                    let cell = self.cell()?;
                    self.tape[cell] = self.tape[cell].wrapping_sub(1);
                }
                b'.' => {
                    let cell = self.cell()?;
                    self.output.push(self.tape[cell]);
                }
                b',' => {
                    let cell = self.cell()?;
                    self.tape[cell] = match self.input.get(self.input_pos) {
                        Some(&byte) => {
                            self.input_pos += 1;
                            byte
                        }
                        None => 0,
                    }
                }
                b'[' => {
                    if self.tape[self.cell()?] == 0 {
                        i = jump_target(jumps, i)?;
                    }
                }
                b']' => {
                    if self.tape[self.cell()?] != 0 {
                        i = jump_target(jumps, i)?;
                    }
                }
                b'@' => return Ok(Halt::Call(i)),
                _ => {}
            }
            i += 1;
        }

        Ok(Halt::Done)
    }
}

/// Builds a `PalaceBrainfuckError`, falling back to whatever error stopped us
/// from building one.
fn brainfuck_error(py: Python<'_>, message: &str) -> PyErr {
    static ERROR_CLASS: PyOnceLock<Py<PyType>> = PyOnceLock::new();
    let build = || -> PyResult<PyErr> {
        let cls = ERROR_CLASS.import(py, PUBLIC_MODULE, "PalaceBrainfuckError")?;
        Ok(PyErr::from_value(cls.call1((message,))?))
    };
    build().unwrap_or_else(|e| e)
}

/// Copies the machine's state into a fresh `PalaceBrainfuckVM`, invokes
/// `callback` with it, then copies back whatever the callback changed.
///
/// Every field the VM exposes makes the trip in both directions, so a foreign
/// function can rewrite the input as freely as it can the tape or the output.
/// Both legs of that trip name their fields — construction by keyword here,
/// `getattr` on the way back — so that reordering `PalaceBrainfuckVM`'s
/// fields cannot silently pair a value with the wrong one.
///
/// Each call site gets its own instance so that a callback which holds on to
/// one never sees it change underneath it on a later crossing.
fn cross_into_python(
    py: Python<'_>,
    machine: &mut Machine,
    callback: &Bound<'_, PyAny>,
) -> PyResult<()> {
    static VM_CLASS: PyOnceLock<Py<PyType>> = PyOnceLock::new();
    let fields = PyDict::new(py);
    fields.set_item("tape", PyByteArray::new(py, &machine.tape))?;
    fields.set_item("pointer", machine.pointer)?;
    fields.set_item("input_bytes", PyBytes::new(py, &machine.input))?;
    fields.set_item("input_pos", machine.input_pos)?;
    fields.set_item("output", PyByteArray::new(py, &machine.output))?;
    let vm = VM_CLASS
        .import(py, PUBLIC_MODULE, "PalaceBrainfuckVM")?
        .call((), Some(&fields))?;

    callback.call1((&vm,))?;

    machine.tape = vm.getattr("tape")?.extract()?;
    machine.pointer = vm.getattr("pointer")?.extract()?;
    machine.input = vm.getattr("input_bytes")?.extract()?;
    machine.input_pos = vm.getattr("input_pos")?.extract()?;
    machine.output = vm.getattr("output")?.extract()?;
    Ok(())
}

#[pyfunction]
fn run_palace_brainfuck_native<'py>(
    py: Python<'py>,
    program: &str,
    input_bytes: Vec<u8>,
    tape_size: usize,
    functions: &Bound<'py, PyAny>,
) -> PyResult<Py<PyBytes>> {
    if tape_size == 0 {
        return Err(brainfuck_error(py, "Tape size must be at least one cell"));
    }

    let program_bytes = program.as_bytes();
    let (jumps, calls) = scan(program_bytes).map_err(|e| brainfuck_error(py, &e.to_string()))?;
    for call in calls.values() {
        if !functions.contains(call.name.as_str())? {
            return Err(brainfuck_error(
                py,
                &format!("Call to unregistered function '{}'", call.name),
            ));
        }
    }

    let mut machine = Machine {
        tape: vec![0u8; tape_size],
        pointer: 0,
        input: input_bytes,
        input_pos: 0,
        output: Vec::new(),
    };

    let mut resume_at = 0usize;
    loop {
        // The interpreter touches no Python state, so other threads are free
        // to run for as long as it stays between call sites.
        let halt = py
            .detach(|| machine.run(program_bytes, &jumps, resume_at))
            .map_err(|fault| brainfuck_error(py, &fault.to_string()))?;

        match halt {
            Halt::Done => break,
            Halt::Call(at) => {
                let call = calls.get(&at).ok_or_else(|| {
                    brainfuck_error(py, &format!("No call site recorded at position {at}"))
                })?;
                let callback = functions.get_item(&call.name)?;
                cross_into_python(py, &mut machine, &callback)?;
                resume_at = call.end + 1;
            }
        }
    }

    Ok(PyBytes::new(py, &machine.output).into())
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_function(wrap_pyfunction!(run_palace_brainfuck_native, m)?)?;
    Ok(())
}
