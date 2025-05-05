import pytest
from frozendict import deepfreeze, frozendict
from pydantic import BaseModel, TypeAdapter

from palace.manager.util.pydantic import FrozenDict, HttpUrl, RedisDsn


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


class TestPydanticFrozenDict:
    def test_main(self) -> None:
        x = deepfreeze(frozendict({0: 0, 0.10: None, 100: [1, 2, 3]}))

        class Example(BaseModel):
            mapping: FrozenDict[int | float, float | None | tuple[int, ...]]

        obj = Example(mapping=x)
        assert isinstance(obj.mapping, frozendict)
        assert obj.mapping == x
        assert obj.model_dump() == {"mapping": x}
        json = obj.model_dump_json()
        loaded = Example.model_validate_json(json)
        assert isinstance(loaded.mapping, frozendict)
        assert loaded.mapping == x
