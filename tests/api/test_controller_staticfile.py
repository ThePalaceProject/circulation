import pytest
from werkzeug.exceptions import NotFound

from api.config import Configuration
from core.model import ConfigurationSetting
from tests.fixtures.api_controller import CirculationControllerFixture
from tests.fixtures.api_images_files import ImageFilesFixture


class TestStaticFileController:
    def test_static_file(
        self,
        circulation_fixture: CirculationControllerFixture,
        api_image_files_fixture: ImageFilesFixture,
    ):
        files = api_image_files_fixture
        cache_timeout = ConfigurationSetting.sitewide(
            circulation_fixture.db.session, Configuration.STATIC_FILE_CACHE_TIME
        )
        cache_timeout.value = 10

        expected_content = files.sample_data("blue.jpg")
        with circulation_fixture.app.test_request_context("/"):
            response = circulation_fixture.app.manager.static_files.static_file(
                files.directory, "blue.jpg"
            )

        assert 200 == response.status_code
        assert "public, max-age=10" == response.headers.get("Cache-Control")
        assert expected_content == response.response.file.read()

        with circulation_fixture.app.test_request_context("/"):
            pytest.raises(
                NotFound,
                circulation_fixture.app.manager.static_files.static_file,
                files.directory,
                "missing.png",
            )

    def test_image(
        self,
        circulation_fixture: CirculationControllerFixture,
        resources_files_fixture: ImageFilesFixture,
    ):
        files = resources_files_fixture

        filename = "FirstBookLoginButton280.png"
        expected_content = files.sample_data(filename)

        with circulation_fixture.app.test_request_context("/"):
            response = circulation_fixture.app.manager.static_files.image(filename)

        assert 200 == response.status_code
        assert expected_content == response.response.file.read()
