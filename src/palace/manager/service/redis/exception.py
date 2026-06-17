from kombu.exceptions import OperationalError as BrokerOperationalError
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    TimeoutError as RedisTimeoutError,
)

from palace.util.exceptions import BasePalaceException


class RedisKeyError(BasePalaceException, TypeError): ...


class RedisValueError(BasePalaceException, ValueError): ...


# Transient errors that mean Redis is briefly unreachable rather than that
# something is wrong in our code: a dropped connection to the application Redis
# client (ConnectionError / TimeoutError), or to the Celery broker while
# publishing a task. The Celery broker is Redis-backed, and kombu wraps the
# underlying redis connection error as its own OperationalError, so we catch
# that here too. Best-effort callers swallow these; the web error handler (see
# core.app_server.ErrorHandler) maps them to a 503 "try again later".
TRANSIENT_REDIS_ERRORS: tuple[type[Exception], ...] = (
    BrokerOperationalError,
    RedisConnectionError,
    RedisTimeoutError,
)
