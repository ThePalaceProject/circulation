from palace.manager.service.redis.configuration import RedisConfiguration
from palace.manager.service.redis.container import RedisContainer


class TestRedisConfiguration:
    def test_connection_resilience_defaults(self) -> None:
        # The connection-resilience knobs default on, with a 30s health-check
        # interval. These defaults are what keep the connection pool from getting
        # stuck handing out stale sockets after a Redis restart.
        config = RedisConfiguration(url="redis://localhost:6379/0")
        assert config.socket_keepalive is True
        assert config.health_check_interval == 30


class TestRedisContainer:
    def test_settings_are_threaded_into_the_connection_pool(self) -> None:
        # The container must pass the resilience settings through to the
        # underlying redis connection pool. Building the pool does not open a
        # connection, so we can assert on its connection_kwargs directly.
        container = RedisContainer()
        container.config.from_dict(
            RedisConfiguration(url="redis://localhost:6379/0").model_dump()
        )
        pool = container.connection_pool()
        assert pool.connection_kwargs["socket_keepalive"] is True
        assert pool.connection_kwargs["health_check_interval"] == 30
