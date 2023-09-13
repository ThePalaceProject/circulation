from pydantic import Field

from core.util.flask_util import CustomBaseModel, StrCommaList


class QuicksightGenerateUrlRequest(CustomBaseModel):
    library_ids: StrCommaList[int] = Field(
        description="The list of libraries to include in the dataset, an empty list is equivalent to all the libraries the user is allowed to access."
    )


class QuicksightGenerateUrlResponse(CustomBaseModel):
    embed_url: str = Field(description="The dashboard embed url.")
