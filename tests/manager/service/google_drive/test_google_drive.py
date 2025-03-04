from __future__ import annotations

import os
import random
import string
from io import BytesIO

import pytest
from pydantic_settings import SettingsConfigDict

from palace.manager.service.google_drive.configuration import GoogleDriveConfiguration
from palace.manager.service.google_drive.google_drive import GoogleDriveService

skip = os.getenv("PALACE_TEST_GOOGLE_DRIVE_SERVICE_ACCOUNT_KEY_FILE_PATH") is None
pytestmark = pytest.mark.skipif(skip, reason="Google Drive tests not configured.")


class GoogleDriveTestConfiguration(GoogleDriveConfiguration):
    service_account_key_file_path: str
    model_config = SettingsConfigDict(
        env_prefix="PALACE_TEST_GOOGLE_DRIVE_",
        extra="ignore",
    )


class GoogleDriveTestFixture:
    def __init__(self):
        config = GoogleDriveTestConfiguration()
        self.google_drive_service = GoogleDriveService(
            config.service_account_key_file_path
        )


@pytest.fixture
def google_drive_service_fixture() -> GoogleDriveTestFixture:
    fixture = GoogleDriveTestFixture()
    return fixture


def generate_random_strings(length):
    return "".join(
        random.choice(string.ascii_letters + string.digits) for _ in range(length)
    )


class TestGoogleDriveService:

    def test_factory(self):
        config = GoogleDriveTestConfiguration()
        google_drive_service = GoogleDriveService.factory(
            config.service_account_key_file_path
        )
        assert google_drive_service is not None

    def test_create_nested_folders(
        self, google_drive_service_fixture: GoogleDriveTestFixture
    ):
        service = google_drive_service_fixture.google_drive_service
        parent_name = generate_random_strings(10)

        # start by sanity checking the non-existence of the root folder
        folder_info = service.get_file(name=parent_name)
        # assert folder_info is None

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

    def test_store_stream(self, google_drive_service_fixture: GoogleDriveTestFixture):
        service = google_drive_service_fixture.google_drive_service
        file_name = generate_random_strings(10) + ".txt"

        stored_file = service.create_file(
            file_name=file_name,
            stream=BytesIO(b"Hello world"),
            content_type="text/plain",
        )
        assert stored_file
        assert stored_file["id"]
        assert stored_file["name"] == file_name

        retrieved_info = service.get_file(name=file_name)

        assert stored_file["id"] == retrieved_info["id"]
        assert stored_file["name"] == retrieved_info["name"]
        # clean up
        service.delete_file(file_id=retrieved_info["id"])

    def test_create_existing_file_fails(
        self, google_drive_service_fixture: GoogleDriveTestFixture
    ):
        service = google_drive_service_fixture.google_drive_service
        file_name = generate_random_strings(10) + ".txt"

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

    def test_share_document(self, google_drive_service_fixture: GoogleDriveTestFixture):
        service = google_drive_service_fixture.google_drive_service
        file_name = "test_folder_" + generate_random_strings(10)
        folder = service.create_nested_folders_if_not_exist([file_name])[0]
        test_email_address = "test@example.com"
        permissions = service.share(
            file_or_folder_id=folder["id"], email_addresses=[test_email_address]
        )
        assert len(permissions) == 1
        permission = permissions[0]
        stored_file = service.create_file(
            file_name="test_file.txt",
            stream=BytesIO(b"Hello world"),
            content_type="text/plain",
            parent_folder_id=folder["id"],
        )

        # verify test_email address has permission  to read test_file.txt
        permissions = (
            service.service.permissions()
            .list(fileId=stored_file["id"], fields="*")
            .execute()["permissions"]
        )
        test_perm = {
            "type": "user",
            "emailAddress": test_email_address,
            "role": "reader",
        }

        matches = False
        for p in permissions:
            if test_perm == {key: p.get(key, None) for key in test_perm}:
                matches = True
                break

        assert matches

        service.delete_file(file_id=stored_file["id"])
        service.unshare(file_id=folder["id"], permission_id=permission["id"])
        service.delete_file(file_id=folder["id"])
