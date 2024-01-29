from __future__ import annotations

import pytest
from werkzeug.exceptions import NotFound

from api.controller.static_file import StaticFileController
from tests.fixtures.api_images_files import ImageFilesFixture
from tests.fixtures.flask import FlaskAppFixture


class TestStaticFileController:
    def test_static_file(
        self,
        api_image_files_fixture: ImageFilesFixture,
        flask_app_fixture: FlaskAppFixture,
    ):
        files = api_image_files_fixture
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
