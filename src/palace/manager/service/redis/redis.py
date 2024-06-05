from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import redis

from palace.manager.service.redis.exception import RedisValueError
from palace.manager.service.redis.key import RedisKeyGenerator

# We do this right now because we are using types-redis, which defines Redis as a generic type,
# even though it is not actually generic. Redis 5 now support type hints natively, so we we
# should be able to drop types-redis in the future.
# Currently, the build in type hints are incomplete at best, so types-redis does a better job.
# This GitHub issue is tracking the progress of the biggest blocker we have for using the
# built-in type hints:
# https://github.com/redis/redis-py/issues/2399
if TYPE_CHECKING:
    RedisClient = redis.Redis[str]
else:
    RedisClient = redis.Redis


class RedisCommandArgsBase(ABC):
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def key_args(self, args: list[Any]) -> Sequence[str]:
        ...


class RedisCommandArgs(RedisCommandArgsBase):
    def __init__(self, name: str, *, args_start: int = 0, args_end: int | None = 1):
        super().__init__(name)
        self.args_start = args_start
        self.args_end = args_end

    def key_args(self, args: list[Any]) -> Sequence[str]:
        return [str(arg) for arg in args[self.args_start : self.args_end]]


class RedisVariableCommandArgs(RedisCommandArgsBase):
    def __init__(self, name: str, *, key_index: int = 0):
        super().__init__(name)
        self.key_index = key_index

    def key_args(self, args: list[Any]) -> Sequence[str]:
        keys = int(args[self.key_index])
        args_start = self.key_index + 1
        return [str(arg) for arg in args[args_start : args_start + keys]]


class RedisCommandNoArgs(RedisCommandArgsBase):
    def key_args(self, args: list[Any]) -> Sequence[str]:
        return []


class Redis(RedisClient):
    """
    A subclass of redis.Redis that adds the ability to check that keys are prefixed correctly.

    Some inspiration for this was taken from Kombu's Redis class. See:
    https://github.com/celery/kombu/pull/1349
    """

    PREFIXED_COMMANDS = [
        RedisCommandNoArgs("SCRIPT LOAD"),
        RedisCommandArgs("KEYS"),
        RedisCommandArgs("GET"),
        RedisCommandArgs("SET"),
        RedisCommandArgs("TTL"),
        RedisCommandArgs("DEL", args_end=None),
        RedisCommandArgs("EXPIRETIME"),
        RedisVariableCommandArgs("EVALSHA", key_index=1),
    ]

    def __init__(self, *args: Any, key_generator: RedisKeyGenerator, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.get_key: RedisKeyGenerator = key_generator
        self.prefixed_lookup = {cmd.name: cmd for cmd in self.PREFIXED_COMMANDS}
        self.auto_close_connection_pool = True

    def _check_prefix(self, *args: Any) -> None:
        arg_list = list(args)
        command = arg_list.pop(0)
        prefix = self.get_key()
        cmd_args = self.prefixed_lookup.get(command)
        if cmd_args is not None:
            for key in cmd_args.key_args(arg_list):
                if not key.startswith(prefix):
                    raise RedisValueError(
                        f"Key {key} does not start with prefix {prefix}. Command {command} args: {arg_list}"
                    )
        else:
            raise RedisValueError(
                f"Command {command} is not checked for prefix. Args: {arg_list}"
            )

    def execute_command(self, *args: Any, **options: Any) -> Any:
        self._check_prefix(*args)
        return super().execute_command(*args, **options)
