from pydantic import Field

from core.util.flask_util import CustomBaseModel


class PatronAuthAccessToken(CustomBaseModel):
    access_token: str = Field(description="A JWE encrypted access token")
    token_type: str = Field(
        description="The authorization token type", default="Bearer"
    )
    expires_in: int = Field(description="Seconds after which the token will expire")
