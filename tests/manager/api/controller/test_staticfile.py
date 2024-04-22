from __future__ import annotations

import pytest
from werkzeug.exceptions import NotFound

from palace.manager.api.controller.static_file import StaticFileController
from tests.fixtures.files import FilesFixture
from tests.fixtures.flask import FlaskAppFixture


class ImageFilesFixture(FilesFixture):
    """A fixture providing access to image files."""

    def __init__(self):
        super().__init__("images")


@pytest.fixture()
def image_files_fixture() -> ImageFilesFixture:
    """A fixture providing access to image files."""
    return ImageFilesFixture()


class TestStaticFileController:
    def test_static_file(
        self,
        image_files_fixture: ImageFilesFixture,
        flask_app_fixture: FlaskAppFixture,
    ):
        files = image_files_fixture
        expected_content = files.sample_data("blue.jpg")
        with flask_app_fixture.test_request_context():
            response = StaticFileController.static_file(files.directory, "blue.jpg")

        assert response.status_code == 200
        assert response.headers.get("Cache-Control") == "no-cache"
        assert response.response.file.read() == expected_content

        with flask_app_fixture.test_request_context():
            pytest.raises(
                NotFound,
                StaticFileController.static_file,
                files.directory,
                "missing.png",
            )
