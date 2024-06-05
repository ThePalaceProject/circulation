from pydantic import RedisDsn

from palace.manager.service.configuration import ServiceConfiguration


class RedisConfiguration(ServiceConfiguration):
    url: RedisDsn
    key_prefix: str = "palace"

    class Config:
        env_prefix = "PALACE_REDIS_"
