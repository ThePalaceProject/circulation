from __future__ import annotations

import os
import random
import string
from io import BytesIO
from unittest.mock import MagicMock

import pytest

from palace.manager.service.google_drive.configuration import GoogleDriveConfiguration
from tests.fixtures.services import ServicesFixture


class GoogleDriveTestFixture:
    def __init__(self, services_fixture: ServicesFixture) -> None:
        self.services_fixture = services_fixture
        self.container = services_fixture.services.google_drive()

        test_credentials = os.getenv(
            "PALACE_TEST_GOOGLE_DRIVE_SERVICE_ACCOUNT_INFO_JSON"
        )
        if test_credentials:
            # Load configuration with our test credentials, when we do this
            # we will make actual requests out to google drive in our tests.
            self.mock_api_client = None
            self.container.config.from_dict(
                GoogleDriveConfiguration(
                    service_account_info_json=test_credentials,
                ).model_dump()
            )
        else:
            # Otherwise we mock the Google Drive api client, and just test
            # that we make the correct calls to the mock.
            self.mock_api_client = MagicMock()
            self.container.api_client.override(self.mock_api_client)
        self.service = self.container.service()

    def random_string(self, length: int = 10) -> str:
        return "".join(
            random.choice(string.ascii_letters + string.digits) for _ in range(length)
        )

    def files_result(
        self, file: dict[str, str] | None = None
    ) -> dict[str, list[dict[str, str]]]:
        files = []
        if file is not None:
            files.append(file)
        return {"files": files}

    def file_data(
        self, name: str | None = None, id: str | None = None
    ) -> dict[str, str]:
        return {
            "id": id or self.random_string(),
            "name": name or self.random_string(),
        }


@pytest.fixture
def google_drive_service_fixture(
    services_fixture: ServicesFixture,
) -> GoogleDriveTestFixture:
    return GoogleDriveTestFixture(services_fixture)


class TestGoogleDriveService:
    def test_create_nested_folders(
        self, google_drive_service_fixture: GoogleDriveTestFixture
    ) -> None:
        fixture = google_drive_service_fixture
        service = fixture.service
        parent_name = fixture.random_string()
        mock_api_client = fixture.mock_api_client

        if mock_api_client:
            mock_api_client.files.return_value.list.return_value.execute.side_effect = [
                fixture.files_result(),
                fixture.files_result(file=fixture.file_data()),
                fixture.files_result(file=fixture.file_data()),
                fixture.files_result(file=fixture.file_data()),
                fixture.files_result(file=fixture.file_data()),
                fixture.files_result(),
                fixture.files_result(),
            ]

        # start by sanity checking the non-existence of the root folder
        folder_info = service.get_file(name=parent_name)
        assert folder_info is None

        # create a parent and child folder
        folders = [parent_name, "child_dir"]
        folder_results = service.create_nested_folders_if_not_exist(folders=folders)
        assert len(folder_results) == 2

        parent_folder = folder_results[0]
        parent_folder_id = parent_folder["id"]

        # verify that now exist
        assert service.get_file(name=parent_name)
        assert service.get_file(name="child_dir", parent_folder_id=parent_folder_id)

        # delete the directories that were created.
        service.delete_file(file_id=folder_results[1]["id"])
        # confirm that the child folder is gone
        folder_info = service.get_file(
            name="child_dir", parent_folder_id=parent_folder_id
        )
        assert folder_info is None

        service.delete_file(file_id=parent_folder_id)

        # confirm that the parent folder is gone
        folder_info = service.get_file(parent_name)
        assert folder_info is None

    def test_create_file(
        self, google_drive_service_fixture: GoogleDriveTestFixture
    ) -> None:
        fixture = google_drive_service_fixture
        service = fixture.service
        file_name = fixture.random_string() + ".txt"
        mock_api_client = fixture.mock_api_client

        expected_data = "Hello world 🌎".encode()

        if mock_api_client:
            mock_api_client.files.return_value.list.return_value.execute.side_effect = [
                fixture.files_result(),
                fixture.files_result(file=fixture.file_data(file_name, "123")),
            ]
            mock_api_client.files.return_value.create.return_value.execute.return_value = fixture.file_data(
                file_name, "123"
            )
            mock_api_client.files.return_value.get_media.return_value.execute.return_value = (
                expected_data
            )

        stream = BytesIO()
        stream.write(expected_data)
        stored_file = service.create_file(
            file_name=file_name,
            stream=stream,
            content_type="text/plain",
        )
        assert stored_file
        assert stored_file["id"]
        assert stored_file["name"] == file_name

        # Confirm the contents of the uploaded file
        assert (
            service.api_client.files().get_media(fileId=stored_file["id"]).execute()
            == expected_data
        )

        retrieved_info = service.get_file(name=file_name)
        assert retrieved_info
        assert stored_file["id"] == retrieved_info["id"]
        assert stored_file["name"] == retrieved_info["name"]
        # clean up
        service.delete_file(file_id=retrieved_info["id"])

    def test_create_existing_file_fails(
        self, google_drive_service_fixture: GoogleDriveTestFixture
    ):
        fixture = google_drive_service_fixture
        service = fixture.service
        file_name = fixture.random_string() + ".txt"
        mock_api_client = fixture.mock_api_client

        if mock_api_client:
            mock_api_client.files.return_value.list.return_value.execute.side_effect = [
                fixture.files_result(),
                fixture.files_result(file=fixture.file_data(file_name)),
            ]
            mock_api_client.files.return_value.create.return_value.execute.return_value = fixture.file_data(
                file_name
            )
        stored_file = service.create_file(
            file_name=file_name,
            stream=BytesIO(b"Hello world"),
            content_type="text/plain",
        )
        assert stored_file

        with pytest.raises(FileExistsError):
            service.create_file(
                file_name=file_name,
                stream=BytesIO(b"Hello world"),
                content_type="text/plain",
            )
