from __future__ import annotations

from io import IOBase
from typing import IO, TYPE_CHECKING, Protocol, TypeVar, cast

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

from palace.util.exceptions import BasePalaceException
from palace.util.log import LoggerMixin

if TYPE_CHECKING:
    from googleapiclient._apis.drive.v3 import DriveResource, File


# Number of times to retry a Drive API call that fails with a retryable error.
# googleapiclient only retries when num_retries is passed explicitly (it defaults
# to 0); when set, it applies exponential backoff with jitter to 5xx responses and
# socket errors. The Drive upload endpoint intermittently returns a bare
# "HttpError 500 ... Unknown Error" that Google documents as retryable, and
# without this a single blip aborts the whole calling task.
API_NUM_RETRIES = 5

# HTTP statuses that indicate a transient Drive-side problem rather than a bad
# request. 5xx is Google's documented retryable set; 429 is rate limiting, where
# backing off is likewise the correct response. Anything else (401/403/404/400)
# reflects configuration or a genuinely bad request and must surface immediately
# instead of being retried for a quarter of an hour.
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})

T_co = TypeVar("T_co", covariant=True)


class GoogleDriveUnavailable(BasePalaceException):
    """The Drive API failed in a way that is worth retrying later.

    This is raised only after googleapiclient's own in-request retry budget
    (:data:`API_NUM_RETRIES`) has been exhausted, so it means the problem
    outlasted the client-level backoff. Callers running as Celery tasks should
    list it in ``autoretry_for`` so the work is re-attempted on a longer delay.
    """


class _ApiRequest(Protocol[T_co]):
    """The subset of googleapiclient's generated request objects that we call.

    The generated stubs give each Drive method its own request class (e.g.
    ``FileHttpRequest``) rather than a generic one, so a structural type is what
    lets :meth:`GoogleDriveService._execute` stay typed across all of them.
    """

    def execute(self, *, num_retries: int = ...) -> T_co: ...


class GoogleDriveService(LoggerMixin):

    def __init__(self, api_client: DriveResource) -> None:
        self.api_client = api_client

    def _execute(self, request: _ApiRequest[T_co]) -> T_co:
        """Run a Drive API request, retrying and classifying transient failures.

        ``num_retries`` turns on googleapiclient's exponential backoff with
        jitter for 5xx responses and socket errors, which covers a brief blip
        within the single call. A failure that survives that budget is
        re-raised as :class:`GoogleDriveUnavailable` so the caller can retry on
        a much longer schedule; non-retryable statuses propagate unchanged.
        """
        try:
            return request.execute(num_retries=API_NUM_RETRIES)
        except HttpError as e:
            if e.resp.status in RETRYABLE_STATUS_CODES:
                raise GoogleDriveUnavailable(
                    f"Google Drive returned a retryable error "
                    f"(status={e.resp.status}): {e}"
                ) from e
            raise

    def get_file(self, name: str, parent_folder_id: str | None = None) -> File | None:
        """
        Return the oldest non-trashed file or folder with the given name, or ``None``.

        Excluding trashed items prevents a folder moved to the Drive trash from
        being mistaken for a live folder (which would cause uploads to land in a
        trashed, invisible directory).

        Ordering by ``createdTime`` (ascending) ensures that when concurrent
        workers race to create the same folder, every worker converges on the
        same canonical (oldest) result regardless of which worker's ``create``
        call won the race.

        :param name: The exact name to search for.
        :param parent_folder_id: If provided, restrict the search to items
            whose parent is this folder/drive ID.
        :return: The oldest matching, non-trashed ``File`` object, or ``None``
            if no match is found.
        """
        query = f"name = '{name}' and trashed = false"

        if parent_folder_id:
            query += f" and '{parent_folder_id}' in parents"

        results = self._execute(
            self.api_client.files().list(
                q=query,
                pageSize=10,
                fields="nextPageToken, files(*)",
                orderBy="createdTime",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
        )

        files: list[File] = results["files"]
        if files:
            return files[0]
        else:
            return None

    def create_file(
        self,
        file_name: str,
        stream: IO[bytes],
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

        # We cast this to IOBase, because the type hints for MediaIoBaseUpload
        # specify this as a IOBase, but it will accept the more generic IO[bytes]
        media = MediaIoBaseUpload(cast(IOBase, stream), mimetype=content_type)
        parents = [parent_folder_id] if parent_folder_id else []
        file_metadata: File = {"name": file_name, "parents": parents}
        file = self._execute(
            self.api_client.files().create(
                body=file_metadata,
                media_body=media,
                fields="*",
                supportsAllDrives=True,
            )
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
                folder = self._execute(
                    self.api_client.files().create(
                        body=body,
                        supportsAllDrives=True,
                    )
                )

            results.append(folder)
            parent_id = folder["id"]

        return results
