import json

import pytest
from pydantic import TypeAdapter, ValidationError

from palace.manager.opds.base import BaseOpdsModel
from palace.manager.opds.util import (
    StrModelOrTuple,
    StrOrModel,
    StrOrTuple,
    obj_or_tuple_to_tuple,
)


def test_obj_or_sequence_to_sequence():
    assert obj_or_tuple_to_tuple(None) == tuple()
    assert obj_or_tuple_to_tuple("foo") == ("foo",)
    assert obj_or_tuple_to_tuple(b"bar") == (b"bar",)
    assert obj_or_tuple_to_tuple(["foo"]) == ("foo",)
    assert obj_or_tuple_to_tuple(["foo", "bar"]) == ("foo", "bar")
    assert obj_or_tuple_to_tuple(("foo", "bar")) == ("foo", "bar")

    original_sequence = ("foo", "bar")
    new_sequence = obj_or_tuple_to_tuple(original_sequence)
    assert new_sequence is original_sequence


class TestStrOrTuple:
    def test_deserialize(self):
        # The type ignores here are due to a mypy bug see:
        # https://github.com/python/mypy/issues/13337
        ta = TypeAdapter(StrOrTuple[str])  # type: ignore[misc]

        # Test normal case
        assert ta.validate_python("foo") == "foo"
        assert ta.validate_python(b"foo") == "foo"
        assert ta.validate_python(bytearray(b"foo")) == "foo"
        assert ta.validate_json(json.dumps("foo")) == "foo"
        assert ta.validate_python(["foo", "bar"]) == ("foo", "bar")
        assert ta.validate_python(("foo", "bar")) == ("foo", "bar")
        assert ta.validate_json(json.dumps(["foo", "bar"])) == ("foo", "bar")

        # Test failures - We should only see a single failure, instead of
        # one for each item in the type union.
        with pytest.raises(ValidationError) as exc_info:
            ta.validate_python(1)
        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["msg"] == "Input should be a valid string or list of strings"

        with pytest.raises(ValidationError) as exc_info:
            ta.validate_python(["foo", 2])
        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["msg"] == "Input should be a valid string"


class MockModel(BaseOpdsModel):
    value: str


class TestStrOrModel:
    def test_serialize(self):
        ta = TypeAdapter(StrOrModel[MockModel])  # type: ignore[misc]

        assert json.loads(ta.dump_json("foo")) == "foo"
        assert json.loads(ta.dump_json(MockModel(value="foo"))) == {"value": "foo"}

    def test_deserialize(self):
        ta = TypeAdapter(StrOrModel[MockModel])  # type: ignore[misc]

        assert ta.validate_python("foo") == "foo"
        assert ta.validate_json(json.dumps("foo")) == "foo"
        assert ta.validate_python(MockModel(value="foo")) == MockModel(value="foo")
        assert ta.validate_json(json.dumps({"value": "foo"})) == MockModel(value="foo")

        # Test failures - We should only see a single failure
        with pytest.raises(ValidationError) as exc_info:
            ta.validate_python(1)
        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["msg"] == "Input should be a valid string or OPDS object"

        with pytest.raises(ValidationError) as exc_info:
            ta.validate_python({})
        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["msg"] == "Field required"


class TestStrModelOrTuple:
    # Pydantic emits spurious serialization warnings when serializing a discriminated
    # union, because it tries each union branch before finding the correct one.
    @pytest.mark.filterwarnings("ignore:Pydantic serializer warnings:UserWarning")
    def test_serialize(self):
        ta = TypeAdapter(StrModelOrTuple[MockModel])  # type: ignore[misc]

        assert json.loads(ta.dump_json("foo")) == "foo"
        assert json.loads(ta.dump_json(MockModel(value="foo"))) == {"value": "foo"}
        assert json.loads(ta.dump_json((MockModel(value="foo"), "test"))) == [
            {"value": "foo"},
            "test",
        ]
        assert json.loads(
            ta.dump_json([MockModel(value="bar"), b"test", bytearray(b"foo")])
        ) == [{"value": "bar"}, "test", "foo"]

    def test_deserialize(self):
        ta = TypeAdapter(StrModelOrTuple[MockModel])  # type: ignore[misc]

        assert ta.validate_python("foo") == "foo"
        assert ta.validate_python(b"foo") == "foo"
        assert ta.validate_python(bytearray(b"foo")) == "foo"
        assert ta.validate_python({"value": "foo"}) == MockModel(value="foo")
        assert ta.validate_python(["foo", b"bar", {"value": "baz"}]) == (
            "foo",
            "bar",
            MockModel(value="baz"),
        )

        # Test failures - We should only see a single failure
        with pytest.raises(ValidationError) as exc_info:
            ta.validate_python(1)
        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert (
            errors[0]["msg"] == "Input should be a valid string, OPDS object, or list"
        )

        with pytest.raises(ValidationError) as exc_info:
            ta.validate_python({})
        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["msg"] == "Field required"

        with pytest.raises(ValidationError) as exc_info:
            ta.validate_python([{}, "boo", 1121])
        errors = exc_info.value.errors()
        assert len(errors) == 2
        assert errors[0]["msg"] == "Field required"
        assert errors[1]["msg"] == "Input should be a valid string or OPDS object"
