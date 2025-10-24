from typing import Annotated, Any, get_args

from frozendict import frozendict
from pydantic import (
    AfterValidator,
    BeforeValidator,
    GetCoreSchemaHandler,
    HttpUrl as HttpUrlPydantic,
    RedisDsn as RedisDsnPydantic,
    UrlConstraints,
)
from pydantic_core import CoreSchema, Url, core_schema


# In Pydantic v2, network types (like URL) no longer inherit from str, which caused issues in our codebase.
# Migration documentation:
#   https://docs.pydantic.dev/latest/migration/#url-and-dsn-types-in-pydanticnetworks-no-longer-inherit-from-str
# GitHub issue:
#   https://github.com/pydantic/pydantic/issues/7186
# The following code was adapted from a comment on the issue:
#   https://github.com/pydantic/pydantic/issues/7186#issuecomment-1690235887
#
# This code validates the specified URL type, converts the validated URL to a string,
# and removes the trailing slash that Pydantic adds to the end of the URL.
class Chain:
    def __init__(self, validations: list[Any]) -> None:
        self.validations = validations

    def __get_pydantic_core_schema__(
        self, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.chain_schema(
            [
                *(handler.generate_schema(v) for v in self.validations),
                handler(source_type),
            ]
        )


def strip_slash(value: str) -> str:
    return value.rstrip("/")


RedisDsn = Annotated[
    str, AfterValidator(strip_slash), BeforeValidator(str), Chain([RedisDsnPydantic])
]

HttpUrl = Annotated[
    str, AfterValidator(strip_slash), BeforeValidator(str), Chain([HttpUrlPydantic])
]

PostgresDsnCustom = Annotated[
    Url,
    UrlConstraints(
        host_required=True,
        allowed_schemes=[
            "postgresql",
        ],
    ),
]
PostgresDsn = Annotated[
    str, AfterValidator(strip_slash), BeforeValidator(str), Chain([PostgresDsnCustom])
]


# This was taken from this pydantic discussion:
# https://github.com/pydantic/pydantic/discussions/8721
class _PydanticFrozenDictAnnotation:
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        def validate_from_dict[KeyT, ValT](
            d: dict[KeyT, ValT] | frozendict[KeyT, ValT],
        ) -> frozendict[KeyT, ValT]:
            return frozendict(d)

        args = get_args(source_type)
        if args:
            # replace the type and rely on Pydantic to generate the right schema for `dict`
            dict_schema = handler.generate_schema(dict[args[0], args[1]])  # type: ignore[valid-type]
        else:
            dict_schema = handler.generate_schema(dict)

        frozendict_schema = core_schema.chain_schema(
            [
                dict_schema,
                core_schema.no_info_plain_validator_function(validate_from_dict),
                core_schema.is_instance_schema(frozendict),
            ]
        )
        return core_schema.json_or_python_schema(
            json_schema=frozendict_schema,
            python_schema=frozendict_schema,
            serialization=core_schema.plain_serializer_function_ser_schema(dict),
        )


type FrozenDict[K, V] = Annotated[frozendict[K, V], _PydanticFrozenDictAnnotation]
"""
A type annotation for a frozendict that Pydantic can validate and serialize.
"""
