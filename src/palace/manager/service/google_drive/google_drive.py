from __future__ import annotations

import ssl
from io import IOBase
from typing import IO, TYPE_CHECKING, Any, Protocol, TypeVar, cast

from google.auth.exceptions import TransportError
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload
from httplib2 import ServerNotFoundError

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
# backing off is likewise the correct response. Anything else (401/404/400)
# reflects configuration or a genuinely bad request and must surface immediately
# instead of being retried for a quarter of an hour.
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})

# Drive reports quota exhaustion as a 403, using the same status as a genuine
# permission failure and distinguishing the two only by the reason attached to
# the error. Google documents these reasons as retryable, so a 403 is classified
# by its reason rather than by its status alone -- otherwise a quota blip would
# fail the run outright while a real permission error got retried for a quarter
# of an hour.
RETRYABLE_403_REASONS = frozenset(
    {
        "rateLimitExceeded",
        "userRateLimitExceeded",
        "sharingRateLimitExceeded",
    }
)

# Network-level failures that mean Drive could not be reached, as opposed to
# Drive answering with an error. googleapiclient retries these itself and
# re-raises the last one once ``num_retries`` is spent, so seeing one here means
# the outage outlasted the in-request budget -- the same situation a retryable
# 5xx leaves us in, and it must be classified the same way or the caller's
# longer retry never runs.
#
# This mirrors googleapiclient's own retryable set exactly. It catches
# ``ssl.SSLError`` and ``ServerNotFoundError`` directly; ``socket.timeout`` is an
# alias of ``TimeoutError``; and the raw-``OSError`` branch it guards by errno
# (ETIMEDOUT, EPIPE, ECONNABORTED, ECONNREFUSED, ECONNRESET) needs no separate
# entry because ``OSError`` maps each of those to a ``ConnectionError`` or
# ``TimeoutError`` subclass on construction. Listing ``OSError`` itself would be
# wrong: googleapiclient re-raises every other errno without retrying.
#
# ``TransportError`` is the exception to the mirror. googleapiclient does not
# retry it at all, so a token refresh that fails mid-outage escapes on the first
# attempt with no backoff; it is still a transient network failure and belongs on
# the longer schedule. Bad credentials raise ``RefreshError``, which is a sibling
# rather than a subclass, so that keeps failing fast.
UNREACHABLE_ERRORS: tuple[type[Exception], ...] = (
    ConnectionError,
    TimeoutError,
    ssl.SSLError,
    ServerNotFoundError,
    TransportError,
)

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


def _is_retryable(error: HttpError) -> bool:
    """Decide whether a Drive error is worth retrying on a longer schedule."""
    status = error.resp.status
    if status in RETRYABLE_STATUS_CODES:
        return True
    if status == 403:
        # error_details holds Drive's per-error dicts when the payload has the
        # expected shape, but googleapiclient leaves it as a string (or unset)
        # for other shapes, so only dicts are inspected.
        details: Any = error.error_details or []
        return any(
            isinstance(detail, dict) and detail.get("reason") in RETRYABLE_403_REASONS
            for detail in details
        )
    return False


class GoogleDriveService(LoggerMixin):

    def __init__(self, api_client: DriveResource) -> None:
        self.api_client = api_client

    def _execute(self, request: _ApiRequest[T_co], operation: str) -> T_co:
        """Run a Drive API request, retrying and classifying transient failures.

        ``num_retries`` turns on googleapiclient's exponential backoff with
        jitter for 5xx responses and socket errors, which covers a brief blip
        within the single call. A failure that survives that budget is
        re-raised as :class:`GoogleDriveUnavailable` so the caller can retry on
        a much longer schedule -- whether Drive answered with a retryable status
        or could not be reached at all. Non-retryable statuses, and errors that
        mean the request itself was bad, propagate unchanged.

        :param request: The built Drive request to run.
        :param operation: What the request was doing, named in any raised
            error. A caller that retries may surface nothing but this message,
            so it has to say which call failed on its own.
        """
        try:
            return request.execute(num_retries=API_NUM_RETRIES)
        except HttpError as e:
            if _is_retryable(e):
                raise GoogleDriveUnavailable(
                    f"Google Drive returned a retryable error while {operation} "
                    f"(status={e.resp.status}): {e}"
                ) from e
            raise
        except UNREACHABLE_ERRORS as e:
            raise GoogleDriveUnavailable(
                f"Could not reach Google Drive while {operation}: {e!r}"
            ) from e

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
            ),
            operation=f"looking up {name!r}",
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
            ),
            operation=f"uploading {file_name!r}",
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
                    ),
                    operation=f"creating folder {folder_name!r}",
                )

            results.append(folder)
            parent_id = folder["id"]

        return results
