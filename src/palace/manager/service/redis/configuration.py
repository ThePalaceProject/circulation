from pydantic_settings import SettingsConfigDict

from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)
from palace.manager.util.pydantic import RedisDsn


class RedisConfiguration(ServiceConfiguration):
    url: RedisDsn
    key_prefix: str = "palace"
    model_config = SettingsConfigDict(env_prefix="PALACE_REDIS_")

    socket_timeout: float | None = 15.0
    socket_connect_timeout: float | None = 5.0

    # Connection resilience. health_check_interval is what actually bounds
    # detection: redis-py PINGs an idle connection before reuse, and if the PING
    # fails (e.g. the connection was left stale by a Redis reboot) it disconnects
    # and re-establishes it transparently, so the pool stops handing out dead
    # sockets within ~30s instead of blocking on them. socket_keepalive just
    # turns on TCP keepalive so the OS can eventually reclaim a connection whose
    # peer vanished without a clean close -- with the default OS probe timing
    # that is a slow (hours) backstop, not fast detection. We deliberately do not
    # enable command retries here: re-running a command that reached the server
    # but whose response was lost would double-execute non-idempotent operations
    # like SPOP and the lock Lua scripts.
    socket_keepalive: bool = True
    health_check_interval: int = 30
