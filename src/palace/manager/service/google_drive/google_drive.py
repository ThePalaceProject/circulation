from __future__ import annotations

import sys
from io import IOBase
from typing import TYPE_CHECKING, Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

from palace.manager.util.log import LoggerMixin

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

if TYPE_CHECKING:
    from googleapiclient._apis.drive.v3 import File, Permission


class GoogleDriveService(LoggerMixin):
    def __init__(
        self,
        service_account_key_file_path: str,
    ) -> None:
        credentials = service_account.Credentials.from_service_account_file(
            service_account_key_file_path,
            scopes=["https://www.googleapis.com/auth/drive"],
        )

        self.service = build("drive", "v3", credentials=credentials)

    @classmethod
    def factory(
        cls,
        service_account_key_file_path: str,
    ) -> Self:
        return cls(service_account_key_file_path)

    def get_file(self, name: str, parent_folder_id: str | None = None) -> File | None:

        query = f"name = '{name}'"

        if parent_folder_id:
            query += f" and '{parent_folder_id}' in parents"

        results = (
            self.service.files()
            .list(
                q=query,
                pageSize=10,
                fields="nextPageToken, files(*)",
            )
            .execute()
        )

        files = results["files"]
        if files:
            return files[0]
        else:
            return None

    def delete_file(self, file_id: str) -> None:
        self.service.files().delete(fileId=file_id).execute()

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
                self.service.files()
                .create(body=file_metadata, media_body=media, fields="*")  # type: ignore[arg-type]
                .execute()
            )
        finally:
            stream.close()

        self.log.info(f"Stored '{file_name}' in parent_folder[{parent_folder_id}].")
        return file

    def create_nested_folders_if_not_exist(self, folders: list[str] = []) -> list[File]:
        """
        Creates a hierarchy of nested folders based on the list of folder names.
        Any of the folders already exist, they will be returned with the results.
        """
        results: list[File] = []
        parent_id = None
        for folder_name in folders:
            body: dict[str, Any] = {
                "name": folder_name,
                "mimeType": "application/vnd.google-apps.folder",
            }
            if parent_id:
                body["parents"] = [parent_id]

            folder = self.get_file(name=folder_name, parent_folder_id=parent_id)

            if not folder:
                folder = self.service.files().create(body=body).execute()  # type: ignore[arg-type]
            results.append(folder)
            parent_id = folder["id"]

        return results

    def share(
        self,
        file_or_folder_id: str,
        email_addresses: list[str],
        role: str = "reader",
    ) -> list[Permission]:
        """
        Share this folder or file with the specified email addresses.
        If the folder or file has already been shared with the email address in the same
        role, the existing permission will be returned rather than creating a new one.
        """
        new_permission: dict[str, Any] = {
            "type": "user",
            "role": role,
        }

        results: list[Permission] = []
        for email_address in email_addresses:
            result = None
            new_permission["emailAddress"] = email_address

            permission_list = (
                self.service.permissions()
                .list(fileId=file_or_folder_id, fields="*")
                .execute()
            )
            for permission_info in permission_list["permissions"]:
                # create a new dictionary with the keys from the permission to be created
                # and the values from the existing permission and compare them
                perm_fields_to_compare = {
                    key: permission_info.get(key, None) for key in new_permission
                }
                # if permission already exists return it with the results
                if new_permission == perm_fields_to_compare:
                    result = permission_info
                    break

            if result is None:
                result = (
                    self.service.permissions()
                    .create(body=new_permission, fileId=file_or_folder_id, fields="*")  # type: ignore[arg-type]
                    .execute()
                )
            results.append(result)
        return results

    def unshare(self, file_id: str, permission_id: str) -> None:
        self.service.permissions().delete(fileId=file_id, permissionId=permission_id)
