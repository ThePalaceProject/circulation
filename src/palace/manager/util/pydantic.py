from typing import Annotated, Any

from pydantic import AfterValidator, BeforeValidator, GetCoreSchemaHandler
from pydantic import HttpUrl as HttpUrlPydantic
from pydantic import RedisDsn as RedisDsnPydantic
from pydantic import UrlConstraints
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
