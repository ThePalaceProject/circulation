import pytest

from tests.fixtures.files import APIFilesFixture


class AxisFilesFixture(APIFilesFixture):
    """A fixture providing access to Axis files."""

    def __init__(self):
        super().__init__("axis")


@pytest.fixture()
def api_axis_files_fixture() -> AxisFilesFixture:
    """A fixture providing access to Axis files."""
    return AxisFilesFixture()
