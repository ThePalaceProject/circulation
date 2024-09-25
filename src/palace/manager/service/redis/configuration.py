from pydantic_settings import SettingsConfigDict

from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)
from palace.manager.util.pydantic import RedisDsn


class RedisConfiguration(ServiceConfiguration):
    url: RedisDsn
    key_prefix: str = "palace"
    model_config = SettingsConfigDict(env_prefix="PALACE_REDIS_")
