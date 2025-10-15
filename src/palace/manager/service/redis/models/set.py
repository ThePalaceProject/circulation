from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Generator, Sequence
from datetime import timedelta
from functools import partial
from typing import Any, TypedDict, cast
from uuid import uuid4

from pydantic import BaseModel

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.service.redis.key import RedisKeyType
from palace.manager.service.redis.redis import Redis
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.util.log import LoggerMixin


class RedisSetKwargs(TypedDict):
    key: Sequence[RedisKeyType]
    expire_time: int


class RedisSet[T: BaseModel](LoggerMixin):
    """
    A set of Pydantic models stored in Redis.

    This class provides methods to add, remove, and retrieve models from the set.
    Sets expire after a specified time (default is 12 hours).
    """

    def __init__(
        self,
        redis_client: Redis,
        model_cls: type[T],
        key: str | Sequence[RedisKeyType] | None = None,
        expire_time: timedelta | int = timedelta(hours=12),
    ):
        self._model_cls = model_cls
        self._redis_client = redis_client
        if key is None:
            key = [str(uuid4())]
        elif isinstance(key, str):
            key = [key]
        self._supplied_key = key
        self._key = self._redis_client.get_key(
            self.__class__.__name__, *self._supplied_key
        )
        if isinstance(expire_time, int):
            expire_time = timedelta(seconds=expire_time)
        self.expire_time = expire_time

    def _str_from_model(self, model: T, /) -> str:
        return model.model_dump_json(exclude_defaults=True)

    def _strs_from_models(self, *models: T) -> list[str]:
        return [self._str_from_model(model) for model in models]

    def _model_from_str(self, _str: str, /) -> T:
        return self._model_cls.model_validate_json(_str)

    def _models_from_strs(
        self,
        *strings: str | bytes | float | int,
    ) -> set[T]:
        return {self._model_from_str(str(str_)) for str_ in strings}

    def add(self, *models: T) -> int:
        """
        Add models to the set. This method will also set an expiration time for the set,
        resetting the expiration time if the set already exists.
        """
        if not models:
            # Extend the expiration time, even if no models are added. In the case
            # where the set doesn't exist, this is a no-op.
            self._redis_client.expire(self._key, self.expire_time)
            return 0

        with self._redis_client.pipeline() as pipe:
            pipe.sadd(self._key, *self._strs_from_models(*models))
            pipe.expire(self._key, self.expire_time)
            sadd_result, _ = pipe.execute()

        return cast(int, sadd_result)

    def remove(self, *models: T) -> int:
        """
        Remove models from the set.
        """
        return self._redis_client.srem(self._key, *self._strs_from_models(*models))

    def get(self) -> set[T]:
        """
        Returns a set containing all the models in the redis set.
        """
        return self._models_from_strs(*self._redis_client.smembers(self._key))

    def len(self) -> int:
        """
        Get the number of models in the set.
        """
        return self._redis_client.scard(self._key)

    def pop(self, size: int) -> set[T]:
        """
        Pop a specified number of models from the set. This method will remove the models
        from the set.
        """
        return self._models_from_strs(*self._redis_client.spop(self._key, size))

    def delete(self) -> bool:
        """
        Delete the set. This method will remove the set from Redis and return True if the set was
        deleted, or False if the set did not exist.
        """
        return self._redis_client.delete(self._key) > 0

    def exists(self) -> bool:
        """
        Check if the set exists in Redis.
        """
        return self._redis_client.exists(self._key) > 0

    def __json__(self) -> RedisSetKwargs:
        """
        Serialize the RedisSet object to a JSON-compatible dictionary, so we can
        use it in the Celery task queue.

        The serialized dict contains the same arguments as the constructor, except
        the redis_client, which is not serializable. This makes it easy to recreate
        the object from the serialized dict:
            RedisSet(client, **redis_set.__json__())
        """
        return RedisSetKwargs(
            key=self._supplied_key,
            expire_time=int(self.expire_time.total_seconds()),
        )

    def __len__(self) -> int:
        """
        Just an alias for len().
        """
        return self.len()

    def __iter__(self) -> Generator[T]:
        """
        Iterate over the models in the set.

        Note: Redis guarantees that all the elements in the set will be returned, but it
        does not guarantee that the elements will only be returned once. So this iterator
        may return the same element multiple times if the set is modified while iterating.

        This should not be a problem, as we are working with a set anyway, but it's important
        for the consumer to be aware of this.
        """
        cursor = None
        sscan = partial(
            self._redis_client.sscan,
            self._key,
        )

        while cursor != 0:
            cursor, identifiers = sscan() if cursor is None else sscan(cursor=cursor)
            for identifier in identifiers:
                yield self._model_from_str(identifier)

    def __contains__(self, model: T) -> bool:
        """
        Check if the set contains the given model.
        """
        return (
            self._redis_client.sismember(
                self._key,
                self._str_from_model(model),
            )
            == 1
        )

    def __repr__(self) -> str:
        """
        Representation of the RedisSet object.
        """
        return f"{self.__class__.__name__}({self.get()!r})"

    def __sub__(self, other: Any) -> set[T]:
        """
        Set difference operation for RedisSet.
        """
        if not isinstance(other, (self.__class__, set)):
            return NotImplemented

        if isinstance(other, set):
            return self.get() - other

        if self._redis_client is not other._redis_client:
            raise PalaceValueError(
                f"Cannot subtract {self.__class__.__name__}s from different Redis clients."
            )

        return self._models_from_strs(*self._redis_client.sdiff(self._key, other._key))

    def __rsub__(self, other: Any) -> set[T]:
        """
        Set difference operation for RedisSet when the other operand is a set.
        """

        if not isinstance(other, set):
            return NotImplemented

        return other - self.get()


class TypeConversionRedisSet[T: BaseModel, U](ABC, RedisSet[T]):
    @abstractmethod
    def _convert(
        self,
        data: T | U,
    ) -> T: ...

    def add(self, *models: T | U) -> int:
        return super().add(*[self._convert(model) for model in models])

    def remove(self, *models: T | U) -> int:
        return super().remove(*[self._convert(model) for model in models])

    def __contains__(self, model: T | U) -> bool:
        return super().__contains__(self._convert(model))


class IdentifierSet(TypeConversionRedisSet[IdentifierData, Identifier]):
    """
    A set of identifiers stored in Redis.

    Identifiers can be supplied as either IdentifierData or Identifier objects.
    """

    def __init__(
        self,
        redis_client: Redis,
        key: str | Sequence[RedisKeyType] | None = None,
        expire_time: timedelta | int = timedelta(hours=12),
    ):
        super().__init__(redis_client, IdentifierData, key, expire_time)

    def _convert(
        self,
        data: IdentifierData | Identifier,
    ) -> IdentifierData:
        return (
            data
            if isinstance(data, IdentifierData)
            else IdentifierData.from_identifier(data)
        )
