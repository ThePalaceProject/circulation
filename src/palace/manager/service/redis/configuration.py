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

    # Connection resilience settings
    # socket_keepalive turns on TCP keepalive so the OS can eventually reclaim 
    # a connection whose peer vanished without a clean close.
    socket_keepalive: bool = True
    # health_check_interval caused redis-py to PING an idle connection before
    # reuse, and if the PING fails (e.g. the connection was left stale by a Redis
    # reboot) it disconnects and re-establishes it transparently. 
    health_check_interval: int = 30
