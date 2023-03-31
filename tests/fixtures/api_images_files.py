import pytest

from tests.fixtures.files import APIFilesFixture


class ImageFilesFixture(APIFilesFixture):
    """A fixture providing access to image files."""

    def __init__(self):
        super().__init__("images")


@pytest.fixture()
def api_image_files_fixture() -> ImageFilesFixture:
    """A fixture providing access to image files."""
    return ImageFilesFixture()
