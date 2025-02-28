from __future__ import annotations

import sys
from typing import Any, BinaryIO

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

from palace.manager.util.log import LoggerMixin

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


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
    ) -> Self | None:
        return cls(service_account_key_file_path)

    def store_stream(
        self,
        file_name: str,
        stream: BinaryIO,
        parent_folder_id: str = None,
        content_type: str | None = None,
    ) -> dict[str, Any] | None:
        try:
            file_metadata = {
                "name": file_name,
            }

            if parent_folder_id:
                file_metadata["parents"] = [parent_folder_id]

            media = MediaIoBaseUpload(stream, mimetype=content_type)

            files = self.service.files().get(body=file_metadata).execute()

            if not files:
                file = (
                    self.service.files()
                    .create(body=file_metadata, media_body=media)
                    .execute()
                )
            else:
                file_metadata[id] = files[0]["id"]
                file = (
                    self.service.files()
                    .update(body=file_metadata, media_body=media)
                    .execute()
                )
        except Exception as e:
            self.log.exception(f"Error uploading {file_name}: {str(e)}")
            return None
        finally:
            stream.close()

        self.log.info(f"Stored '{file_name}' in parent_folder[{parent_folder_id}].")
        return file

    def create_nested_folders_if_exist(self, folders: list[str] = []) -> list[dict]:
        results: list[dict] = []
        parent_id = None
        for folder_name in folders:
            body = {
                "name": folder_name,
                "mimeType": "application/vnd.google-apps.folder",
            }
            if parent_id:
                body["parents"] = [parent_id]

            folders = self.service.files().get(body=body).execute()

            if not folders:
                folder = self.service.files().create(body=body).execute()
            results.append(folders[0])
            parent_id = folder[0]["id"]

        return results

    def share(
        self,
        file_or_folder_id: str,
        role: str = "reader",
        permission_type: str = "anyone",
        email: str = None,
    ) -> list[dict]:
        user_permission = {
            "type": permission_type,
            "role": role,
        }
        if email and role == "user":
            user_permission["emailAddress"] = email

        permission = (
            self.service.permssions()
            .create(body=user_permission, fileId=file_or_folder_id)
            .execute()
        )
        return permission
