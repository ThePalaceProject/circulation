from typing import List
from uuid import UUID

from pydantic import Field, validator

from core.util.flask_util import CustomBaseModel, str_comma_list_validator


class QuicksightGenerateUrlRequest(CustomBaseModel):
    library_uuids: List[UUID] = Field(
        description="The list of libraries to include in the dataset, an empty list is equivalent to all the libraries the user is allowed to access."
    )

    @validator("library_uuids", pre=True)
    def parse_library_uuids(cls, value) -> List[UUID]:
        uuid_list = str_comma_list_validator(value)
        # verify that all strings in the list are UUIDs
        return list(map(UUID, uuid_list))


class QuicksightGenerateUrlResponse(CustomBaseModel):
    embed_url: str = Field(description="The dashboard embed url.")


class QuicksightDashboardNamesResponse(CustomBaseModel):
    names: List[str] = Field(description="The named quicksight dashboard ids")
