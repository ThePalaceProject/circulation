import string
from datetime import datetime
from pathlib import Path

import pytest
from pyinstrument import Profiler


@pytest.fixture(scope="session")
def pyinstrument_root_dir() -> Path:
    return Path.cwd() / "tests"


@pytest.fixture(scope="session")
def pyinstrument_profile_dir(pyinstrument_root_dir: Path) -> Path:
    """
    This fixture is used to set up the pyinstrument profiler for the entire test session.
    It will create a directory for storing the profiling results.
    """
    profile_root = (
        pyinstrument_root_dir
        / "profiles"
        / datetime.now().isoformat(timespec="minutes")
    )
    profile_root.mkdir(exist_ok=True, parents=True)
    return profile_root


VALID_FILENAME_CHARS = frozenset("-_.(): " + string.ascii_letters + string.digits)


def sanitize_filename(filename: str) -> str:
    """
    Sanitize a filename by replacing invalid characters with spaces.
    """
    return "".join(c if c in VALID_FILENAME_CHARS else "_" for c in filename)


@pytest.fixture()
def pyinstrument_test(
    pyinstrument_root_dir: Path,
    pyinstrument_profile_dir: Path,
    request: pytest.FixtureRequest,
    worker_id: str,
):
    """
    Fixture to profile a test using pyinstrument.

    In order to profile a test, just use this fixture in the test function, and it will be profiled automatically.

    If you are profiling a number of tests, you can also modify the parameters of this fixture, by setting
    auto_use=True and different test scopes, you automatically get profiling of all our test cases.

    For example something like @pytest.fixture(auto_use=True, scope="module") will profile provide
    module level traces for all the tests that are run.
    """
    # Turn profiling on
    profiler = Profiler()
    profiler.start()

    yield  # Run test

    profiler.stop()

    if "::" in request.node.nodeid:
        path_str, filename = request.node.nodeid.split("::", maxsplit=1)
        path = Path(path_str)
        filename = sanitize_filename(filename)
    elif request.node.nodeid == "":
        path = Path()
        filename = f"{worker_id}-results"
    else:
        nodeid_path = Path(request.node.nodeid)
        path = nodeid_path.parent
        filename = nodeid_path.name

    results_dir = pyinstrument_profile_dir / path
    results_dir.mkdir(exist_ok=True, parents=True)
    results_file = (results_dir / filename).with_suffix(".html")
    profiler.write_html(results_file)
