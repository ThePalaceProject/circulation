from __future__ import annotations

import json
import sys
from io import IOBase
from typing import TYPE_CHECKING, Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import HttpMockSequence, MediaIoBaseUpload

from palace.manager.util.log import LoggerMixin

if sys.version_info >= (3, 11):
    pass
else:
    pass

if TYPE_CHECKING:
    from googleapiclient._apis.drive.v3 import File


class GoogleDriveService(LoggerMixin):
    def __init__(
        self, credentials: Any | None, http: HttpMockSequence | None = None
    ) -> None:

        if credentials:
            assert not http
        if http:
            assert not credentials

        self.service = build(
            "drive", "v3", credentials=credentials, http=http  # type:ignore[arg-type]
        )

    @classmethod
    def factory(cls, service_account_info_json: str = "{}") -> GoogleDriveService:
        scopes = ["https://www.googleapis.com/auth/drive"]
        credentials = service_account.Credentials.from_service_account_info(
            info=json.loads(service_account_info_json), scopes=scopes
        )
        return GoogleDriveService(credentials=credentials)

    def get_file(self, name: str, parent_folder_id: str | None = None) -> File | None:

        query = f"name = '{name}'"

        if parent_folder_id:
            query += f" and '{parent_folder_id}' in parents"

        results = (
            self.service.files()  # type: ignore[attr-defined]
            .list(
                q=query,
                pageSize=10,
                fields="nextPageToken, files(*)",
            )
            .execute()
        )

        files: list[File] = results["files"]
        if files:
            return files[0]
        else:
            return None

    def create_file(
        self,
        file_name: str,
        stream: IOBase,
        content_type: str,
        parent_folder_id: str | None = None,
    ) -> File:

        try:
            # check that the file doesn't already exist since google drive will create multiple files (with different
            # ids) in the same directory which we don't ever want.
            file = self.get_file(name=file_name, parent_folder_id=parent_folder_id)
            if file:
                raise FileExistsError(
                    f'A file named "{file_name}" already exists in folder(id={parent_folder_id}'
                )

            media = MediaIoBaseUpload(stream, mimetype=content_type)
            parents = [parent_folder_id] if parent_folder_id else []
            file_metadata = dict(name=file_name, parents=parents)
            file = (
                self.service.files()  # type: ignore[attr-defined]
                .create(body=file_metadata, media_body=media, fields="*")
                .execute()
            )
        finally:
            stream.close()

        assert file
        self.log.info(f"Stored '{file_name}' in parent_folder[{parent_folder_id}].")
        return file

    def create_nested_folders_if_not_exist(
        self, folders: list[str] = [], parent_folder_id: str | None = None
    ) -> list[File]:
        """
        Creates a hierarchy of nested folders based on the list of folder names.
        Any of the folders already exist, they will be returned with the results.
        """
        results: list[File] = []
        parent_id = parent_folder_id
        for folder_name in folders:
            body: dict[str, Any] = {
                "name": folder_name,
                "mimeType": "application/vnd.google-apps.folder",
            }
            if parent_id:
                body["parents"] = [parent_id]

            folder = self.get_file(name=folder_name, parent_folder_id=parent_id)

            if not folder:
                folder = (
                    self.service.files()  # type: ignore[attr-defined]
                    .create(body=body)
                    .execute()
                )
            assert folder
            results.append(folder)
            parent_id = folder["id"]

        return results
