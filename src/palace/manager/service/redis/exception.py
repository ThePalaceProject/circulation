from kombu.exceptions import OperationalError as BrokerOperationalError
from redis import exceptions as redis_exceptions
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    TimeoutError as RedisTimeoutError,
)

from palace.util.exceptions import BasePalaceException


class RedisKeyError(BasePalaceException, TypeError): ...


class RedisValueError(BasePalaceException, ValueError): ...


# types-redis has no stub for OutOfMemoryError, though redis-py raises it. Binding
# it through the module keeps the ignore scoped to this one symbol, so a future
# stub bump that drops or renames another name is still reported. See the note in
# service.redis.redis about why we prefer types-redis to the built-in hints.
RedisOutOfMemoryError: type[Exception] = redis_exceptions.OutOfMemoryError  # type: ignore[attr-defined]

# Transient errors that mean Redis is unusable right now rather than that
# something is wrong in our code: a dropped connection to the application Redis
# client (ConnectionError / TimeoutError), a Redis at maxmemory with nothing
# evictable left rejecting every write (OutOfMemoryError), or a failure reaching
# the Celery broker while publishing a task. The Celery broker is Redis-backed,
# and kombu wraps the underlying redis connection error as its own
# OperationalError, so we catch that here too. Best-effort callers swallow
# these; the web error handler (see core.app_server.ErrorHandler) maps them to a
# 503 "try again later".
#
# Note that redis's AuthenticationError is a ConnectionError subclass and so is a
# member of this tuple, but it signals a persistent credential/ACL problem rather
# than a blip. Callers that swallow these errors should re-raise it; see
# core.app_server.ErrorHandler and Work.queue_indexing.
TRANSIENT_REDIS_ERRORS: tuple[type[Exception], ...] = (
    BrokerOperationalError,
    RedisConnectionError,
    RedisOutOfMemoryError,
    RedisTimeoutError,
)
