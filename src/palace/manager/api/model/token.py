from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, PositiveInt

from palace.manager.util.datetime_helpers import utc_now


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
    token_type: Literal["Bearer"]

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
