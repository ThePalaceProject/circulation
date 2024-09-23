import pytest
from pydantic import TypeAdapter

from palace.manager.util.pydantic import HttpUrl, RedisDsn


class TestStrUrlTypes:
    def test_redis_dsn(self) -> None:
        ta = TypeAdapter(RedisDsn)
        validate = ta.validate_python

        assert validate("redis://localhost:6379") == "redis://localhost:6379/0"
        assert validate("redis://localhost:6379/") == "redis://localhost:6379/0"
        assert validate("redis://localhost:6379/1") == "redis://localhost:6379/1"

        with pytest.raises(
            ValueError, match="URL scheme should be 'redis' or 'rediss'"
        ):
            validate("foo://localhost")

    def test_http_url(self) -> None:
        ta = TypeAdapter(HttpUrl)
        validate = ta.validate_python

        assert validate("http://localhost:6379") == "http://localhost:6379"
        assert validate("http://localhost:6379/") == "http://localhost:6379"
        assert validate("http://10.0.0.1/foo") == "http://10.0.0.1/foo"
        assert validate("http://10.0.0.1/foo/") == "http://10.0.0.1/foo"
