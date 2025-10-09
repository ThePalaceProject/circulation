from collections.abc import Generator
from contextlib import contextmanager
from typing import Self

import pytest
from pydantic_settings import SettingsConfigDict

from palace.manager.service.redis.container import RedisContainer
from palace.manager.service.redis.redis import Redis
from palace.manager.util.pydantic import RedisDsn
from tests.fixtures.config import FixtureTestUrlConfiguration
from tests.fixtures.database import TestIdFixture
from tests.fixtures.services import ServicesFixture


class RedisTestConfiguration(FixtureTestUrlConfiguration):
    url: RedisDsn
    model_config = SettingsConfigDict(env_prefix="PALACE_TEST_REDIS_")


class RedisFixture:
    def __init__(self, test_id: TestIdFixture, services_fixture: ServicesFixture):
        self.test_id = test_id
        self.config = RedisTestConfiguration.from_env()
        self.key_prefix = f"test::{self.test_id.id}"

        self.container = RedisContainer()
        self.container.config.from_dict(
            {
                "url": self.config.url,
                "key_prefix": self.key_prefix,
            }
        )
        services_fixture.services.redis.override(self.container)
        self.client: Redis = self.container.client()

    def keys(self) -> list[str]:
        """
        Get all keys in the Redis database created by the test.
        """
        return self.client.keys(f"{self.key_prefix}*")

    def close(self):
        keys = self.keys()
        if keys:
            self.client.delete(*keys)

    @classmethod
    @contextmanager
    def fixture(
        cls, test_id: TestIdFixture, services_fixture: ServicesFixture
    ) -> Generator[Self, None, None]:
        fixture = cls(test_id, services_fixture)
        try:
            yield fixture
        finally:
            fixture.close()


@pytest.fixture(scope="function")
def redis_fixture(
    function_test_id: TestIdFixture, services_fixture: ServicesFixture
) -> Generator[RedisFixture, None, None]:
    with RedisFixture.fixture(function_test_id, services_fixture) as fixture:
        yield fixture
