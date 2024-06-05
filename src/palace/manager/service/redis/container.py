import redis
from dependency_injector import providers
from dependency_injector.containers import DeclarativeContainer

from palace.manager.service.redis.key import RedisKeyGenerator
from palace.manager.service.redis.redis import Redis


class RedisContainer(DeclarativeContainer):
    config = providers.Configuration()

    connection_pool: providers.Provider[redis.ConnectionPool] = providers.Singleton(
        redis.ConnectionPool.from_url, url=config.url, decode_responses=True
    )

    key_generator: providers.Provider[RedisKeyGenerator] = providers.Singleton(
        RedisKeyGenerator, prefix=config.key_prefix
    )

    client: providers.Provider[Redis] = providers.Singleton(
        Redis, connection_pool=connection_pool, key_generator=key_generator
    )
