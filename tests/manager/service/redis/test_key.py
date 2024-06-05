import pytest

from palace.manager.service.redis.exception import RedisKeyError
from tests.fixtures.redis import RedisFixture


class MockSupportsRedisKey:
    def __init__(self, key: str = "test"):
        self.key = key

    def redis_key(self) -> str:
        return self.key


class TestRedisKeyGenerator:
    def test___call__(self, redis_fixture: RedisFixture):
        key_prefix = redis_fixture.key_prefix
        key_generator = redis_fixture.client.get_key
        sep = key_generator.SEPERATOR

        # No args returns just the key prefix
        key = key_generator()
        assert key == key_prefix

        # Simple string key
        test_key = "test"
        key = key_generator(test_key)

        # Key always includes the key prefix and is separated by the RedisKeyGenerator.SEPERATOR
        assert key == f"{key_prefix}{sep}{test_key}"

        # Multiple args are all included and separated by the RedisKeyGenerator.SEPERATOR
        key = key_generator("test", "key", "generator")
        assert key == f"{key_prefix}{sep}test{sep}key{sep}generator"

        # ints are also supported and are converted to strings
        key = key_generator(1, 2, 3)
        assert key == f"{key_prefix}{sep}1{sep}2{sep}3"

        # SupportsRedisKey objects are supported, and their __redis_key__ method is called to get the key
        key = key_generator(MockSupportsRedisKey("test"), MockSupportsRedisKey("key"))
        assert key == f"{key_prefix}{sep}test{sep}key"

        # Unsupported types raise a RedisKeyError
        with pytest.raises(RedisKeyError) as exc_info:
            key_generator([1, 2, 3])  # type: ignore[arg-type]
        assert "Unsupported key type: [1, 2, 3] (list)" in str(exc_info.value)
