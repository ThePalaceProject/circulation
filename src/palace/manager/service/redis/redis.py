from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

import redis

from palace.manager.service.redis.exception import RedisValueError
from palace.manager.service.redis.key import RedisKeyGenerator

# We do this right now because we are using types-redis, which defines Redis as a generic type,
# even though it is not actually generic. Redis 5 now support type hints natively, so we
# should be able to drop types-redis in the future.
# Currently, the built-in type hints are incomplete at best, so types-redis does a better job.
# The types-redis package description on mypy has some more information and is a good place to
# check in the future for updates: https://pypi.org/project/types-redis/.
# This GitHub issue is tracking the progress of the biggest blocker we have for using the
# built-in type hints: https://github.com/redis/redis-py/issues/2399
if TYPE_CHECKING:
    RedisClient = redis.Redis[str]
    RedisPipeline = redis.client.Pipeline[str]
else:
    RedisClient = redis.Redis
    RedisPipeline = redis.client.Pipeline


class RedisPrefixCheckMixin(ABC):
    """
    A mixin with functions to check that the keys used in a redis command have the expected
    prefix. This is useful for ensuring that keys are namespaced correctly in a multi-tenant
    environment.

    We use this mixin in our Redis and Pipeline classes.

    Some inspiration for this was taken from Kombu's Redis class. See:
        https://github.com/celery/kombu/pull/1349
    """

    class RedisCommandArgsBase(ABC):
        def __init__(self, name: str):
            self.name = name

        @abstractmethod
        def key_args(self, args: list[Any]) -> Sequence[str]:
            """
            Takes a list of arguments and returns a sequence of keys that should be checked for the
            correct prefix.
            """
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

    _PREFIXED_COMMANDS = {
        cmd.name: cmd
        for cmd in [
            RedisCommandNoArgs("SCRIPT LOAD"),
            RedisCommandNoArgs("INFO"),
            RedisCommandArgs("KEYS"),
            RedisCommandArgs("GET"),
            RedisCommandArgs("EXPIRE"),
            RedisCommandArgs("PEXPIRE"),
            RedisCommandArgs("GETRANGE"),
            RedisCommandArgs("SET"),
            RedisCommandArgs("TTL"),
            RedisCommandArgs("PTTL"),
            RedisCommandArgs("PTTL"),
            RedisCommandArgs("SADD"),
            RedisCommandArgs("SPOP"),
            RedisCommandArgs("SCARD"),
            RedisCommandArgs("WATCH"),
            RedisCommandArgs("SRANDMEMBER"),
            RedisCommandArgs("SREM"),
            RedisCommandArgs("DEL", args_end=None),
            RedisCommandArgs("MGET", args_end=None),
            RedisCommandArgs("EXISTS", args_end=None),
            RedisCommandArgs("EXPIRETIME"),
            RedisVariableCommandArgs("EVALSHA", key_index=1),
        ]
    }

    def _check_prefix(self, *args: Any) -> None:
        arg_list = list(args)
        command = arg_list.pop(0).upper()
        cmd_args = self._PREFIXED_COMMANDS.get(command)
        if cmd_args is None:
            raise RedisValueError(
                f"Command {command} is not checked for prefix. Args: {arg_list}"
            )

        for key in cmd_args.key_args(arg_list):
            if not key.startswith(self._prefix):
                raise RedisValueError(
                    f"Key {key} does not start with prefix {self._prefix}. Command {command} args: {arg_list}"
                )

    @property
    @abstractmethod
    def _prefix(self) -> str: ...


class Redis(RedisClient, RedisPrefixCheckMixin):
    """
    A subclass of redis.Redis that adds the ability to check that keys are prefixed correctly.
    """

    def __init__(self, *args: Any, key_generator: RedisKeyGenerator, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.get_key: RedisKeyGenerator = key_generator
        self.auto_close_connection_pool = True

    @property
    def _prefix(self) -> str:
        return self.get_key()

    def execute_command(self, *args: Any, **options: Any) -> Any:
        self._check_prefix(*args)
        return super().execute_command(*args, **options)

    def pipeline(self, transaction: bool = True, shard_hint: Any = None) -> Pipeline:
        return Pipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint,
            key_generator=self.get_key,
        )

    @cached_property
    def elasticache(self) -> bool:
        """
        Check if this Redis instances is actually connected to AWS ElastiCache rather than Redis.

        AWS ElastiCache is supposed to be API compatible with Redis, but there are some differences
        that can cause issues. This property can be used to detect if we are connected to ElastiCache
        and handle those differences.
        """
        return self.info().get("os") == "Amazon ElastiCache"


class Pipeline(RedisPipeline, RedisPrefixCheckMixin):
    """
    A subclass of redis.client.Pipeline that adds the ability to check that keys are prefixed correctly.
    """

    def __init__(self, *args: Any, key_generator: RedisKeyGenerator, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.get_key: RedisKeyGenerator = key_generator

    @property
    def _prefix(self) -> str:
        return self.get_key()

    def execute_command(self, *args: Any, **options: Any) -> Any:
        self._check_prefix(*args)
        return super().execute_command(*args, **options)

    def __enter__(self) -> Pipeline:
        return self
