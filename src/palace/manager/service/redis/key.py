from typing import Protocol, runtime_checkable

from sqlalchemy.orm import Mapped

from palace.manager.service.redis.exception import RedisKeyError


class RedisKeyMixin:
    id: Mapped[int]

    def redis_key(self) -> str:
        return self.redis_key_from_id(self.id)

    @classmethod
    def redis_key_from_id(cls, id_: int | None) -> str:
        cls_name = cls.__name__

        if id_ is None:
            raise RedisKeyError(f"{cls_name} must have an id to generate a redis key.")

        return f"{cls_name}{RedisKeyGenerator.SEPERATOR}{id_}"


@runtime_checkable
class SupportsRedisKey(Protocol):
    def redis_key(self) -> str:
        ...


class RedisKeyGenerator:
    SEPERATOR = "::"

    def __init__(self, prefix: str):
        self.prefix = prefix

    def _stringify(self, key: SupportsRedisKey | str | int) -> str:
        if isinstance(key, SupportsRedisKey):
            return key.redis_key()
        elif isinstance(key, str):
            return key
        elif isinstance(key, int):
            return str(key)
        else:
            raise RedisKeyError(
                f"Unsupported key type: {key} ({key.__class__.__name__})"
            )

    def __call__(self, *args: SupportsRedisKey | str | int) -> str:
        key_strings = [self._stringify(k) for k in args]
        return self.SEPERATOR.join([self.prefix, *key_strings])
