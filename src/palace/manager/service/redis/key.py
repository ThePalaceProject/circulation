from typing import Protocol, runtime_checkable

from palace.manager.service.redis.exception import RedisKeyError


@runtime_checkable
class SupportsRedisKey(Protocol):
    def __redis_key__(self) -> str:
        ...


class RedisKeyGenerator:
    SEPERATOR = "::"

    def __init__(self, prefix: str):
        self.prefix = prefix

    def _stringify(self, key: SupportsRedisKey | str | int) -> str:
        if isinstance(key, SupportsRedisKey):
            return key.__redis_key__()
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
