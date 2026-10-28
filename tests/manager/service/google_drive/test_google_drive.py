from __future__ import annotations

import errno
import json
import ssl
from io import BytesIO
from typing import Any
from urllib.parse import unquote_plus

import pytest
from google.auth.exceptions import RefreshError, TransportError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import HttpMockSequence
from httplib2 import ServerNotFoundError

from palace.manager.service.google_drive.google_drive import (
    API_NUM_RETRIES,
    GoogleDriveService,
    GoogleDriveUnavailable,
)


class UnreachableHttp(HttpMockSequence):
    """A transport that answers once, then fails at the socket level.

    ``HttpMockSequence`` can only produce HTTP responses, so an outage where
    Drive cannot be reached at all needs a transport that raises instead. The
    one canned response satisfies ``create_file``'s duplicate check, putting the
    failure on the upload itself -- the call that failed in production.
    """

    def __init__(self, error: Exception, canned_responses: int = 1) -> None:
        canned = [({"status": "200"}, '{"files": []}')] * canned_responses
        super().__init__(canned)
        self.error = error
        self.canned_remaining = len(canned)
        self.upload_attempts = 0

    def request(self, *args: Any, **kwargs: Any) -> Any:
        if self.canned_remaining:
            self.canned_remaining -= 1
            return super().request(*args, **kwargs)
        self.upload_attempts += 1
        raise self.error


def drive_service(http: HttpMockSequence) -> GoogleDriveService:
    api_client = build(
        "drive", "v3", credentials=None, http=http
    )  #  type: ignore[call-overload]
    return GoogleDriveService(api_client=api_client)


class TestGoogleDriveService:

    def test_create_nested_folders(
        self,
    ):
        root_folder_id = "parent_folder_id"
        parent_id = "parent-id"
        parent_name = "parent-folder"
        child_id = "child-id"
        child_name = "child-folder"
        http_mock_sequence = HttpMockSequence(
            [
                ({"status": "200"}, '{"files": []}'),
                (
                    {"status": "200"},
                    '{"kind": "drive#file", "id": "parent-id", "name": "parent-folder","mimeType": "application/vnd.google-apps.folder"}',
                ),
                ({"status": "200"}, '{"files": []}'),
                (
                    {"status": "200"},
                    '{"kind": "drive#file", "id": "child-id", "name": "child-folder","mimeType": "application/vnd.google-apps.folder"}',
                ),
            ]
        )
        service = drive_service(http=http_mock_sequence)

        # create a parent and child folder
        folders = [parent_name, child_name]

        folder_results = service.create_nested_folders_if_not_exist(
            folders=folders, parent_folder_id=root_folder_id
        )
        assert len(folder_results) == 2

        parent_folder = folder_results[0]
        child_folder = folder_results[1]
        assert parent_id == parent_folder["id"]
        assert parent_name == parent_folder["name"]
        assert child_id == child_folder["id"]
        assert child_name == child_folder["name"]

        request_seq = http_mock_sequence.request_sequence
        assert json.loads(request_seq[1][2]) == {
            "name": parent_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [root_folder_id],
        }
        assert json.loads(request_seq[3][2]) == {
            "name": child_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        }

    def test_create_file(self):
        file_name = "file.txt"
        file_id = "file-id"
        http_mock_sequence = HttpMockSequence(
            [
                ({"status": "200"}, '{"files": []}'),
                (
                    {"status": "200"},
                    json.dumps(
                        {
                            "kind": "drive#file",
                            "id": file_id,
                            "name": file_name,
                            "mimeType": "text/plain",
                        }
                    ),
                ),
            ]
        )
        service = drive_service(http=http_mock_sequence)

        mime_type = "text/plain"

        stored_file = service.create_file(
            file_name=file_name,
            stream=BytesIO(b"Hello world"),
            content_type=mime_type,
        )

        assert stored_file
        assert stored_file["id"]
        assert stored_file["name"] == file_name

        request_seq = http_mock_sequence.request_sequence

        body = str(request_seq[1][2])
        assert '{"name": "' + file_name + '", "parents": []}' in body
        assert "Hello world" in body
        assert f"Content-Type: {mime_type}" in body

    def test_get_file_excludes_trashed_items(self):
        """get_file must include 'trashed = false' in its Drive query."""
        file_name = "some-folder"
        file_id = "folder-id"
        http_mock_sequence = HttpMockSequence(
            [
                (
                    {"status": "200"},
                    json.dumps(
                        {
                            "files": [
                                {
                                    "kind": "drive#file",
                                    "id": file_id,
                                    "name": file_name,
                                    "mimeType": "application/vnd.google-apps.folder",
                                }
                            ]
                        }
                    ),
                ),
            ]
        )
        service = drive_service(http=http_mock_sequence)

        result = service.get_file(name=file_name, parent_folder_id="parent-id")

        assert result is not None
        assert result["id"] == file_id

        # Verify the query sent to the Drive API contains the trashed filter.
        request_uri = unquote_plus(http_mock_sequence.request_sequence[0][0])
        assert "trashed = false" in request_uri

    def test_create_existing_file_fails(self):
        file_name = "file.txt"
        file_id = "file-id"
        http_mock_sequence = HttpMockSequence(
            [
                (
                    {"status": "200"},
                    json.dumps(
                        {
                            "files": [
                                {
                                    "kind": "drive#file",
                                    "id": file_id,
                                    "name": file_name,
                                    "mimeType": "text/plain",
                                },
                            ]
                        }
                    ),
                ),
            ]
        )
        service = drive_service(http=http_mock_sequence)

        with pytest.raises(FileExistsError):
            service.create_file(
                file_name=file_name,
                stream=BytesIO(b"Hello world"),
                content_type="text/plain",
            )

    def test_get_file_orders_by_created_time(self):
        """get_file always requests results ordered by createdTime so that the
        oldest (canonical) folder is returned when duplicates exist."""
        folder_name = "my-folder"
        older_id = "older-id"
        newer_id = "newer-id"
        http_mock_sequence = HttpMockSequence(
            [
                (
                    {"status": "200"},
                    json.dumps(
                        {
                            "files": [
                                {
                                    "kind": "drive#file",
                                    "id": older_id,
                                    "name": folder_name,
                                    "mimeType": "application/vnd.google-apps.folder",
                                },
                                {
                                    "kind": "drive#file",
                                    "id": newer_id,
                                    "name": folder_name,
                                    "mimeType": "application/vnd.google-apps.folder",
                                },
                            ]
                        }
                    ),
                ),
            ]
        )
        service = drive_service(http=http_mock_sequence)

        result = service.get_file(name=folder_name)

        # The first result (oldest, per orderBy=createdTime) is returned.
        assert result is not None
        assert result["id"] == older_id

        # Verify orderBy=createdTime is present in the request URI.
        request_uri = unquote_plus(http_mock_sequence.request_sequence[0][0])
        assert "orderBy=createdTime" in request_uri

    @pytest.fixture
    def no_retry_sleep(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Remove googleapiclient's exponential backoff delay between retries.

        ``HttpRequest`` captures ``time.sleep`` at construction time, so patching
        the attribute on the ``time`` module before the request is built is enough.
        """
        monkeypatch.setattr("time.sleep", lambda _seconds: None)

    def test_create_file_retries_server_error(self, no_retry_sleep: None):
        """A retryable 5xx from the upload endpoint is retried, not propagated.

        Drive intermittently answers uploads with a bare ``HttpError 500 ...
        Unknown Error``; without ``num_retries`` a single one of these aborts the
        whole calling task.
        """
        file_name = "file.txt"
        file_id = "file-id"
        success = json.dumps(
            {
                "kind": "drive#file",
                "id": file_id,
                "name": file_name,
                "mimeType": "text/plain",
            }
        )
        http_mock_sequence = HttpMockSequence(
            [
                ({"status": "200"}, '{"files": []}'),
                ({"status": "500"}, '{"error": {}}'),
                ({"status": "200"}, success),
            ]
        )
        service = drive_service(http=http_mock_sequence)

        stored_file = service.create_file(
            file_name=file_name,
            stream=BytesIO(b"Hello world"),
            content_type="text/plain",
        )

        assert stored_file["id"] == file_id
        # The upload was attempted twice: the 500, then the successful retry.
        assert len(http_mock_sequence.request_sequence) == 3
        # The retry resends the full multipart body, not an exhausted stream.
        assert "Hello world" in str(http_mock_sequence.request_sequence[2][2])

    @pytest.mark.parametrize(
        "status,reason,retryable,upload_attempts",
        [
            # googleapiclient retries 5xx, 429 and two of the rate-limit reasons
            # itself, so for those the whole budget is spent before our
            # classifier ever sees the error.
            pytest.param("500", None, True, API_NUM_RETRIES + 1, id="server-error"),
            pytest.param(
                "429", None, True, API_NUM_RETRIES + 1, id="too-many-requests"
            ),
            pytest.param(
                "403", "rateLimitExceeded", True, API_NUM_RETRIES + 1, id="rate-limit"
            ),
            pytest.param(
                "403",
                "userRateLimitExceeded",
                True,
                API_NUM_RETRIES + 1,
                id="user-rate-limit",
            ),
            # googleapiclient does not recognise this one, so it arrives after a
            # single attempt -- our classifier is what makes it retryable.
            pytest.param(
                "403", "sharingRateLimitExceeded", True, 1, id="sharing-rate-limit"
            ),
            # A 403 that is a real permission problem, not a quota one.
            pytest.param(
                "403", "insufficientFilePermissions", False, 1, id="permission-denied"
            ),
            pytest.param("400", None, False, 1, id="bad-request"),
            pytest.param("401", None, False, 1, id="unauthorized"),
            pytest.param("404", None, False, 1, id="not-found"),
        ],
    )
    def test_create_file_error_classification(
        self,
        status: str,
        reason: str | None,
        retryable: bool,
        upload_attempts: int,
        no_retry_sleep: None,
    ):
        """Only errors worth waiting out become GoogleDriveUnavailable.

        Callers key their own, much longer, retry schedule off that exception,
        so a misconfiguration must keep its original error and surface at once
        rather than being retried for a quarter of an hour. Drive muddies this
        by reporting quota exhaustion with the same 403 as a real permission
        failure, distinguished only by the reason.

        ``upload_attempts`` records which layer caught the error: more than one
        means googleapiclient retried within the request before giving up.
        """
        # googleapiclient reads error.message before it collects error.errors,
        # so a body without a message never yields parsed error details.
        body = (
            json.dumps(
                {
                    "error": {
                        "code": int(status),
                        "message": reason,
                        "errors": [{"reason": reason}],
                    }
                }
            )
            if reason
            else '{"error": {}}'
        )
        # Always offer a full budget of failures; a non-retryable error simply
        # leaves the surplus unused.
        http_mock_sequence = HttpMockSequence(
            [({"status": "200"}, '{"files": []}')]
            + [({"status": status}, body)] * (API_NUM_RETRIES + 1)
        )
        service = drive_service(http=http_mock_sequence)

        with pytest.raises(HttpError if not retryable else GoogleDriveUnavailable) as e:
            service.create_file(
                file_name="file.txt",
                stream=BytesIO(b"Hello world"),
                content_type="text/plain",
            )

        assert isinstance(e.value, GoogleDriveUnavailable) == retryable
        # One request for the duplicate check, then the upload attempts.
        assert len(http_mock_sequence.request_sequence) == upload_attempts + 1

    @pytest.mark.parametrize(
        "error,upload_attempts",
        [
            pytest.param(
                ConnectionResetError(errno.ECONNRESET, "Connection reset by peer"),
                API_NUM_RETRIES + 1,
                id="connection-reset",
            ),
            pytest.param(
                TimeoutError(errno.ETIMEDOUT, "Connection timed out"),
                API_NUM_RETRIES + 1,
                id="timeout",
            ),
            pytest.param(
                BrokenPipeError(errno.EPIPE, "Broken pipe"),
                API_NUM_RETRIES + 1,
                id="broken-pipe",
            ),
            pytest.param(
                ssl.SSLError("record layer failure"), API_NUM_RETRIES + 1, id="ssl"
            ),
            pytest.param(
                ServerNotFoundError("Unable to find the server"),
                API_NUM_RETRIES + 1,
                id="dns",
            ),
            pytest.param(
                TransportError("token refresh failed"), 1, id="auth-transport"
            ),
        ],
    )
    def test_create_file_wraps_unreachable_errors(
        self,
        error: Exception,
        upload_attempts: int,
        no_retry_sleep: None,
    ):
        """An outage that hides Drive entirely is retryable, like a 5xx is.

        A 500 means Drive answered; a reset socket or a failed DNS lookup means
        it never did. Both leave the report unwritten for the same transient
        reason, so both have to reach the caller as GoogleDriveUnavailable --
        otherwise the longer, task-level retry never runs and a network blip
        costs the month's report, exactly as a 500 used to.

        ``upload_attempts`` records whether googleapiclient retried before
        giving up. It does for socket-level errors; a token refresh failing
        mid-outage raises TransportError, which it does not retry at all, so
        that one arrives here after a single attempt.
        """
        http = UnreachableHttp(error)
        service = drive_service(http=http)

        with pytest.raises(GoogleDriveUnavailable) as raised:
            service.create_file(
                file_name="file.txt",
                stream=BytesIO(b"Hello world"),
                content_type="text/plain",
            )

        assert raised.value.__cause__ is error
        assert http.upload_attempts == upload_attempts

    @pytest.mark.parametrize(
        "error",
        [
            pytest.param(
                PermissionError(errno.EACCES, "Permission denied"),
                id="unrelated-oserror",
            ),
            pytest.param(
                RefreshError("invalid_grant: account disabled"), id="bad-credentials"
            ),
        ],
    )
    def test_create_file_does_not_wrap_permanent_errors(
        self, error: Exception, no_retry_sleep: None
    ):
        """Failures that waiting cannot fix keep their own type.

        Neither of these is a transient outage: a local OSError that is not one
        of googleapiclient's retryable errnos is a genuine fault, and a
        RefreshError means the credentials themselves are rejected. Wrapping
        either would hide a permanent problem behind a quarter of an hour of
        pointless retries. RefreshError is a sibling of TransportError rather
        than a subclass, which is what keeps them on opposite sides of this line.
        """
        http = UnreachableHttp(error)
        service = drive_service(http=http)

        with pytest.raises(type(error)) as raised:
            service.create_file(
                file_name="file.txt",
                stream=BytesIO(b"Hello world"),
                content_type="text/plain",
            )

        assert raised.value is error
        # Given up on immediately, with no retrying of any kind.
        assert http.upload_attempts == 1

    @pytest.mark.parametrize(
        "call,canned_responses,operation",
        [
            pytest.param(
                lambda service: service.get_file(name="report.csv"),
                0,
                "looking up 'report.csv'",
                id="lookup",
            ),
            pytest.param(
                lambda service: service.create_file(
                    file_name="report.csv",
                    stream=BytesIO(b"Hello world"),
                    content_type="text/csv",
                ),
                1,
                "uploading 'report.csv'",
                id="upload",
            ),
            pytest.param(
                lambda service: service.create_nested_folders_if_not_exist(
                    folders=["Usage Reports"]
                ),
                1,
                "creating folder 'Usage Reports'",
                id="folder-creation",
            ),
        ],
    )
    def test_unavailable_error_names_the_failed_call(
        self,
        call: Any,
        canned_responses: int,
        operation: str,
        no_retry_sleep: None,
    ):
        """Every Drive call says what it was doing when it failed.

        A caller that retries may surface nothing but this message, and the
        three calls fail for the same reasons, so naming the one that failed is
        what distinguishes a report that could not be uploaded from one whose
        folder could not be created.
        """
        service = drive_service(
            http=UnreachableHttp(
                ConnectionResetError(errno.ECONNRESET, "reset"), canned_responses
            )
        )

        with pytest.raises(GoogleDriveUnavailable) as raised:
            call(service)

        assert operation in str(raised.value)
