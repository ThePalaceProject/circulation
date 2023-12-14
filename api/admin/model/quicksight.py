from uuid import UUID

from pydantic import Field, validator

from core.util.flask_util import CustomBaseModel, str_comma_list_validator


class QuicksightGenerateUrlRequest(CustomBaseModel):
    library_uuids: list[UUID] = Field(
        description="The list of libraries to include in the dataset, an empty list is equivalent to all the libraries the user is allowed to access."
    )

    @validator("library_uuids", pre=True)
    def parse_library_uuids(cls, value) -> list[str]:
        return str_comma_list_validator(value)


class QuicksightGenerateUrlResponse(CustomBaseModel):
    embed_url: str = Field(description="The dashboard embed url.")


class QuicksightDashboardNamesResponse(CustomBaseModel):
    names: list[str] = Field(description="The named quicksight dashboard ids")
