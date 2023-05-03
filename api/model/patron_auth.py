from pydantic import Field

from core.util.flask_util import CustomBaseModel


class PatronAuthAccessToken(CustomBaseModel):
    access_token: str = Field(description="A JWE encrypted access token")
