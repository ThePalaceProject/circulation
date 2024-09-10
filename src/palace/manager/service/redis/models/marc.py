from __future__ import annotations

import json
import sys
from collections.abc import Callable, Generator, Mapping, Sequence
from contextlib import contextmanager
from enum import auto
from functools import cached_property
from typing import Any

from pydantic import BaseModel
from redis import ResponseError, WatchError

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.service.redis.models.lock import LockError, RedisJsonLock
from palace.manager.service.redis.redis import Pipeline, Redis
from palace.manager.service.storage.s3 import MultipartS3UploadPart
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.util.log import LoggerMixin

# TODO: Remove this when we drop support for Python 3.10
if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from backports.strenum import StrEnum


class MarcFileUploadSessionError(LockError):
    pass


class MarcFileUpload(BaseModel):
    buffer: str = ""
    upload_id: str | None = None
    parts: list[MultipartS3UploadPart] = []


class MarcFileUploadState(StrEnum):
    INITIAL = auto()
    QUEUED = auto()
    UPLOADING = auto()


class PathEscapeMixin:
    """
    Mixin to provide methods for escaping and unescaping paths for use in redis.

    This is necessary because it seems like there is a bug in the AWS elasticache implementation
    of JSONPATH where slashes or tilde character within a string literal used as a key cause issues.
    This bug is not present in the open source redis implementation, which does the sane thing, not
    requiring any special escaping.

    Hopefully at some point AWS will fix these issues, and we can drop this mixin, so I tried to
    encapsulate the logic for this here.

    In AWS when a tilde is used in a key, the key is never updated, despite returning a success. And
    when a slash is used in a key, the key is interpreted as a nested path, nesting a new key for every
    slash in the path. This is not the behavior we want, so we need to escape these characters.

    We can test if this is fixed in the future by running the test suite against AWS elasticache with
    this mixin removed. If the tests pass, then it can be removed.

    Characters are escaped by prefixing them with a backtick character, followed by a single character
    from _MAPPING that represents the escaped character. The backtick character itself is escaped by
    prefixing it with another backtick character.
    """

    _ESCAPE_CHAR = "`"

    _MAPPING = {
        "/": "s",
        "~": "t",
    }

    @cached_property
    def _FORWARD_MAPPING(self) -> dict[str, str]:
        mapping = {k: "".join((self._ESCAPE_CHAR, v)) for k, v in self._MAPPING.items()}
        mapping[self._ESCAPE_CHAR] = "".join((self._ESCAPE_CHAR, self._ESCAPE_CHAR))
        return mapping

    @cached_property
    def _REVERSE_MAPPING(self) -> dict[str, str]:
        mapping = {v: k for k, v in self._MAPPING.items()}
        mapping[self._ESCAPE_CHAR] = self._ESCAPE_CHAR
        return mapping

    def _escape_path(self, path: str) -> str:
        escaped = json.dumps("".join([self._FORWARD_MAPPING.get(c, c) for c in path]))
        return escaped[1:-1]

    def _unescape_path(self, path: str) -> str:
        # Normal redis paths are always double-quoted, so we can use json.loads to unescape them.
        # This does not happen in the AWS elasticache implementation, so we need to handle it manually,
        # so that we can support both implementations.
        try:
            path = json.loads(f'"{path}"')
        except json.JSONDecodeError:
            pass

        in_escape = False
        unescaped = []
        for char in path:
            if in_escape:
                if char not in self._REVERSE_MAPPING:
                    raise PalaceValueError(
                        f"Invalid escape sequence '{self._ESCAPE_CHAR}{char}'"
                    )
                unescaped.append(self._REVERSE_MAPPING[char])
                in_escape = False
            else:
                if char == self._ESCAPE_CHAR:
                    in_escape = True
                else:
                    unescaped.append(char)

        if in_escape:
            raise PalaceValueError("Unterminated escape sequence.")

        return "".join(unescaped)


class MarcFileUploadSession(RedisJsonLock, PathEscapeMixin, LoggerMixin):
    """
    This class is used as a lock for the Celery MARC export task, to ensure that only one
    task can upload MARC files for a given collection at a time. It increments an update
    number each time an update is made, to guard against corruption if a task gets run
    twice.

    It stores the  intermediate results of the MARC file generation process, so that the task
    can complete in multiple steps, saving the progress between steps to redis, and flushing
    them to S3 when the buffer is full.

    This object is focused on the redis part of this operation, the actual s3 upload orchestration
    is handled by the `MarcUploadManager` class.
    """

    def __init__(
        self,
        redis_client: Redis,
        collection_id: int,
        update_number: int = 0,
    ):
        super().__init__(redis_client)
        self._collection_id = collection_id
        self._update_number = update_number

    @cached_property
    def key(self) -> str:
        return self._redis_client.get_key(
            self.__class__.__name__,
            Collection.redis_key_from_id(self._collection_id),
        )

    @property
    def _lock_timeout_ms(self) -> int:
        return 20 * 60 * 1000  # 20 minutes

    @property
    def update_number(self) -> int:
        return self._update_number

    @property
    def _initial_value(self) -> str:
        """
        The initial value to use for the locks JSON object.
        """
        return json.dumps(
            {"uploads": {}, "update_number": 0, "state": MarcFileUploadState.INITIAL}
        )

    @property
    def _update_number_json_key(self) -> str:
        return "$.update_number"

    @property
    def _uploads_json_key(self) -> str:
        return "$.uploads"

    @property
    def _state_json_key(self) -> str:
        return "$.state"

    @staticmethod
    def _upload_initial_value(buffer_data: str) -> dict[str, Any]:
        return MarcFileUpload(buffer=buffer_data).dict(exclude_none=True)

    def _upload_path(self, upload_key: str) -> str:
        upload_key = self._escape_path(upload_key)
        return f'{self._uploads_json_key}["{upload_key}"]'

    def _buffer_path(self, upload_key: str) -> str:
        upload_path = self._upload_path(upload_key)
        return f"{upload_path}.buffer"

    def _upload_id_path(self, upload_key: str) -> str:
        upload_path = self._upload_path(upload_key)
        return f"{upload_path}.upload_id"

    def _parts_path(self, upload_key: str) -> str:
        upload_path = self._upload_path(upload_key)
        return f"{upload_path}.parts"

    @contextmanager
    def _pipeline(
        self, begin_transaction: bool = True
    ) -> Generator[Pipeline, None, None]:
        with self._redis_client.pipeline() as pipe:
            pipe.watch(self.key)
            fetched_data = self._parse_multi(
                pipe.json().get(
                    self.key, self._lock_json_key, self._update_number_json_key
                )
            )
            # Check that we hold the lock
            if (
                remote_random := fetched_data.get(self._lock_json_key)
            ) != self._random_value:
                raise MarcFileUploadSessionError(
                    f"Must hold lock to update upload session. "
                    f"Expected: {self._random_value}, got: {remote_random}"
                )
            # Check that the update number is correct
            if (
                remote_update_number := fetched_data.get(self._update_number_json_key)
            ) != self._update_number:
                raise MarcFileUploadSessionError(
                    f"Update number mismatch. "
                    f"Expected: {self._update_number}, got: {remote_update_number}"
                )
            if begin_transaction:
                pipe.multi()
            yield pipe

    def _execute_pipeline(
        self,
        pipe: Pipeline,
        updates: int,
        *,
        state: MarcFileUploadState = MarcFileUploadState.UPLOADING,
    ) -> list[Any]:
        if not pipe.explicit_transaction:
            raise MarcFileUploadSessionError(
                "Pipeline should be in explicit transaction mode before executing."
            )
        pipe.json().set(self.key, path=self._state_json_key, obj=state)
        pipe.json().numincrby(self.key, self._update_number_json_key, updates)
        pipe.pexpire(self.key, self._lock_timeout_ms)
        try:
            pipe_results = pipe.execute(raise_on_error=False)
        except WatchError as e:
            raise MarcFileUploadSessionError(
                "Failed to update buffers. Another process is modifying the buffers."
            ) from e
        self._update_number = self._parse_value_or_raise(pipe_results[-2])

        return pipe_results[:-3]

    @staticmethod
    def _validate_results(results: list[Any]) -> bool:
        """
        This function validates that all the results of the pipeline are successful,
        and not a ResponseError.

        NOTE: The AWS elasticache implementation returns slightly different results then redis.
        In redis, unsuccessful results when a key is not found are `None`, but in AWS they are
        returned as a `ResponseError`, which is why we are checking for both in this function.
        """
        return all(r and not isinstance(r, ResponseError) for r in results)

    def append_buffers(self, data: Mapping[str, str]) -> dict[str, int]:
        if not data:
            return {}

        set_results = {}
        with self._pipeline(begin_transaction=False) as pipe:
            existing_uploads: list[str] = self._parse_value_or_raise(
                pipe.json().objkeys(self.key, self._uploads_json_key)
            )
            existing_uploads = [self._unescape_path(p) for p in existing_uploads]
            pipe.multi()
            for key, value in data.items():
                if value == "":
                    continue
                if key in existing_uploads:
                    pipe.json().strappend(
                        self.key, path=self._buffer_path(key), value=value
                    )
                else:
                    pipe.json().set(
                        self.key,
                        path=(self._upload_path(key)),
                        obj=self._upload_initial_value(value),
                    )
                    set_results[key] = len(value)

            pipe_results = self._execute_pipeline(pipe, len(data))

        if not self._validate_results(pipe_results):
            raise MarcFileUploadSessionError("Failed to append buffers.")

        return {
            k: set_results[k] if v is True else self._parse_value_or_raise(v)
            for k, v in zip(data.keys(), pipe_results)
        }

    def add_part_and_clear_buffer(self, key: str, part: MultipartS3UploadPart) -> None:
        with self._pipeline() as pipe:
            pipe.json().arrappend(
                self.key,
                self._parts_path(key),
                part.dict(),
            )
            pipe.json().set(
                self.key,
                path=self._buffer_path(key),
                obj="",
            )
            pipe_results = self._execute_pipeline(pipe, 1)

        if not self._validate_results(pipe_results):
            raise MarcFileUploadSessionError("Failed to add part and clear buffer.")

    def set_upload_id(self, key: str, upload_id: str) -> None:
        with self._pipeline() as pipe:
            pipe.json().set(
                self.key,
                path=self._upload_id_path(key),
                obj=upload_id,
                nx=True,
            )
            pipe_results = self._execute_pipeline(pipe, 1)

        if not self._validate_results(pipe_results):
            raise MarcFileUploadSessionError("Failed to set upload ID.")

    def clear_uploads(self) -> None:
        with self._pipeline() as pipe:
            pipe.json().clear(self.key, self._uploads_json_key)
            pipe_results = self._execute_pipeline(pipe, 1)

        if not self._validate_results(pipe_results):
            raise MarcFileUploadSessionError("Failed to clear uploads.")

    def _get_specific(
        self,
        keys: str | Sequence[str],
        get_path: Callable[[str], str],
    ) -> dict[str, Any]:
        if isinstance(keys, str):
            keys = [keys]
        paths = {get_path(k): k for k in keys}
        results = self._redis_client.json().get(self.key, *paths.keys())
        if len(keys) == 1:
            return {keys[0]: self._parse_value(results)}
        else:
            return {paths[k]: v for k, v in self._parse_multi(results).items()}

    def _get_all(self, key: str) -> dict[str, Any]:
        get_results = self._redis_client.json().get(self.key, key)
        results: dict[str, Any] | None = self._parse_value(get_results)

        if results is None:
            return {}

        return {self._unescape_path(k): v for k, v in results.items()}

    def get(self, keys: str | Sequence[str] | None = None) -> dict[str, MarcFileUpload]:
        if keys is None:
            uploads = self._get_all(self._uploads_json_key)
        else:
            uploads = self._get_specific(keys, self._upload_path)

        return {
            k: MarcFileUpload.parse_obj(v) for k, v in uploads.items() if v is not None
        }

    def get_upload_ids(self, keys: str | Sequence[str]) -> dict[str, str]:
        return self._get_specific(keys, self._upload_id_path)

    def get_part_num_and_buffer(self, key: str) -> tuple[int, str]:
        with self._redis_client.pipeline() as pipe:
            pipe.json().get(self.key, self._buffer_path(key))
            pipe.json().arrlen(self.key, self._parts_path(key))
            results = pipe.execute(raise_on_error=False)
        if not self._validate_results(results):
            raise MarcFileUploadSessionError(
                "Failed to get part number and buffer data."
            )

        buffer_data: str = self._parse_value_or_raise(results[0])
        part_number: int = self._parse_value_or_raise(results[1])

        return part_number, buffer_data

    def state(self) -> MarcFileUploadState | None:
        get_results = self._redis_client.json().get(self.key, self._state_json_key)
        state: str | None = self._parse_value(get_results)
        if state is None:
            return None
        return MarcFileUploadState(state)

    def set_state(self, state: MarcFileUploadState) -> None:
        with self._pipeline() as pipe:
            self._execute_pipeline(pipe, 0, state=state)
