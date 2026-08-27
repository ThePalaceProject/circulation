from kombu.exceptions import OperationalError as BrokerOperationalError

# The ignore is for OutOfMemoryError: redis-py raises it, but types-redis has no
# stub for it. See the note in service.redis.redis about why we still prefer
# types-redis over the incomplete built-in hints.
from redis.exceptions import (  # type: ignore[attr-defined]
    ConnectionError as RedisConnectionError,
    OutOfMemoryError as RedisOutOfMemoryError,
    TimeoutError as RedisTimeoutError,
)

from palace.util.exceptions import BasePalaceException


class RedisKeyError(BasePalaceException, TypeError): ...


class RedisValueError(BasePalaceException, ValueError): ...


# Transient errors that mean Redis is unusable right now rather than that
# something is wrong in our code: a dropped connection to the application Redis
# client (ConnectionError / TimeoutError), a Redis at maxmemory with nothing
# evictable left rejecting every write (OutOfMemoryError), or a failure reaching
# the Celery broker while publishing a task. The Celery broker is Redis-backed,
# and kombu wraps the underlying redis connection error as its own
# OperationalError, so we catch that here too. Best-effort callers swallow
# these; the web error handler (see core.app_server.ErrorHandler) maps them to a
# 503 "try again later".
TRANSIENT_REDIS_ERRORS: tuple[type[Exception], ...] = (
    BrokerOperationalError,
    RedisConnectionError,
    RedisOutOfMemoryError,
    RedisTimeoutError,
)
