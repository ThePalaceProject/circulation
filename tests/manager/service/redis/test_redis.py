import pytest

from palace.manager.service.redis.exception import RedisValueError
from tests.fixtures.redis import RedisFixture


class TestRedis:
    def test_client(self, redis_fixture: RedisFixture):
        # Do a bit of basic testing to make sure the client is working
        client = redis_fixture.client
        key = client.get_key("test")
        redis_fixture.client.set(key, "value")
        assert redis_fixture.client.get(key) == "value"

    def test_prefix_check(self, redis_fixture: RedisFixture):
        # Our version of the redis client checks that keys are prefixed correctly
        client = redis_fixture.client

        key = "test"
        with pytest.raises(RedisValueError) as exc_info:
            client.set(key, "value")
        assert (
            f"Key {key} does not start with prefix {redis_fixture.key_prefix}"
            in str(exc_info.value)
        )

        # We also handle commands with multiple keys
        key1 = client.get_key("test1")
        key2 = client.get_key("test2")
        key3 = "unprefixed"

        with pytest.raises(RedisValueError) as exc_info:
            client.delete(key1, key2, key3)
        assert (
            f"Key {key3} does not start with prefix {redis_fixture.key_prefix}"
            in str(exc_info.value)
        )

        # If we pass a command that isn't checked for a prefix, we raise an error
        with pytest.raises(RedisValueError) as exc_info:
            client.execute_command("UNKNOWN", key1)
        assert "Command UNKNOWN is not checked for prefix" in str(exc_info.value)

    def test_pipeline(self, redis_fixture: RedisFixture):
        # We can also use pipelines
        client = redis_fixture.client
        key = client.get_key("test")
        with client.pipeline() as pipe:
            pipe.set(key, "value")
            pipe.get(key)
            result = pipe.execute()
        assert result == [True, "value"]

        # Any pipeline commands are also checked for prefixes
        with pytest.raises(RedisValueError) as exc_info:
            with client.pipeline() as pipe:
                pipe.set(key, "value")
                pipe.get(key)
                pipe.set("unprefixed", "value")
                pipe.get("unprefixed")
                pipe.execute()
        assert (
            f"Key unprefixed does not start with prefix {redis_fixture.key_prefix}"
            in str(exc_info.value)
        )
