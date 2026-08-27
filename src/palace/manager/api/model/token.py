from __future__ import annotations

from datetime import datetime, timedelta
from typing import Annotated, Any, Literal

from pydantic import BaseModel, BeforeValidator, ConfigDict, PositiveInt

from palace.util.datetime_helpers import utc_now


def _normalize_token_type(value: Any) -> Any:
    # RFC 6749 §5.1 defines the token_type value as case insensitive, so
    # providers may send any casing (e.g. "bearer"). Normalize to "Bearer".
    if isinstance(value, str):
        return value.title()
    return value


class OAuthTokenResponse(BaseModel):
    """
    A RFC8693 OAuth 2.0 Token Response model.

    This model represents the response from an OAuth 2.0 token endpoint.

    It includes some common helper methods to easily check the token's
    expiration status and retrieve the expiration time.
    """

    model_config = ConfigDict(
        frozen=True,
    )

    access_token: str
    expires_in: PositiveInt
    token_type: Annotated[Literal["Bearer"], BeforeValidator(_normalize_token_type)]
    scope: str | None = None

    _expires_at: datetime

    def model_post_init(self, context: Any, /) -> None:
        # We set the expiration time to 95% of the expires_in value
        # to account for any potential delays in processing, so we
        # will get a new token before the current one expires.
        self._expires_at = utc_now() + timedelta(seconds=self.expires_in * 0.95)

    @property
    def expired(self) -> bool:
        """
        Returns True if the token is expired.
        """
        return utc_now() >= self._expires_at

    @property
    def expires(self) -> datetime:
        """
        Returns the expiration time of the token.
        """
        return self._expires_at
