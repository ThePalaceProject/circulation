from __future__ import annotations

from io import IOBase
from tempfile import _TemporaryFileWrapper
from typing import TYPE_CHECKING

from googleapiclient.http import MediaIoBaseUpload

from palace.manager.util.log import LoggerMixin

if TYPE_CHECKING:
    from googleapiclient._apis.drive.v3 import DriveResource, File


class GoogleDriveService(LoggerMixin):

    def __init__(self, api_client: DriveResource) -> None:
        self.api_client = api_client

    def get_file(self, name: str, parent_folder_id: str | None = None) -> File | None:

        query = f"name = '{name}'"

        if parent_folder_id:
            query += f" and '{parent_folder_id}' in parents"

        results = (
            self.api_client.files()
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
        stream: IOBase | _TemporaryFileWrapper[bytes],
        content_type: str,
        parent_folder_id: str | None = None,
    ) -> File:

        # check that the file doesn't already exist since google drive will create multiple files (with different
        # ids) in the same directory which we don't ever want.
        file = self.get_file(name=file_name, parent_folder_id=parent_folder_id)
        if file:
            raise FileExistsError(
                f'A file named "{file_name}" already exists in folder(id={parent_folder_id}'
            )

        media = MediaIoBaseUpload(stream, mimetype=content_type)
        parents = [parent_folder_id] if parent_folder_id else []
        file_metadata: File = {"name": file_name, "parents": parents}
        file = (
            self.api_client.files()
            .create(body=file_metadata, media_body=media, fields="*")
            .execute()
        )

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
            body: File = {
                "name": folder_name,
                "mimeType": "application/vnd.google-apps.folder",
            }
            if parent_id:
                body["parents"] = [parent_id]

            folder = self.get_file(name=folder_name, parent_folder_id=parent_id)

            if not folder:
                folder = self.api_client.files().create(body=body).execute()

            results.append(folder)
            parent_id = folder["id"]

        return results
