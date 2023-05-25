from __future__ import annotations

import re
import time
from typing import Optional, Pattern, Union

import jwt
import requests
from flask_babel import lazy_gettext as _
from pydantic import HttpUrl

from core.integration.settings import ConfigurationFormItem, FormField
from core.model import Patron

from .authentication.base import PatronData
from .authentication.basic import (
    BasicAuthenticationProvider,
    BasicAuthProviderLibrarySettings,
    BasicAuthProviderSettings,
)
from .circulation_exceptions import RemoteInitiatedServerError


class FirstBookAuthSettings(BasicAuthProviderSettings):
    url: HttpUrl = FormField(
        "https://ebooksprod.firstbook.org/api/",
        form=ConfigurationFormItem(
            label=_("URL"),
            description=_("The URL for the First Book authentication service."),
            required=True,
        ),
    )
    password: str = FormField(
        ...,
        form=ConfigurationFormItem(
            label=_("Key"),
            description=_("The key for the First Book authentication service."),
        ),
    )
    # Server-side validation happens before the identifier
    # is converted to uppercase, which means lowercase characters
    # are valid.
    identifier_regular_expression: Pattern = FormField(
        re.compile(r"^[A-Za-z0-9@]+$"),
        form=ConfigurationFormItem(
            label="Identifier Regular Expression",
            description="A patron's identifier will be immediately rejected if it doesn't match this "
            "regular expression.",
            weight=10,
        ),
    )
    password_regular_expression: Optional[Pattern] = FormField(
        re.compile(r"^[0-9]+$"),
        form=ConfigurationFormItem(
            label="Password Regular Expression",
            description="A patron's password will be immediately rejected if it doesn't match this "
            "regular expression.",
            weight=10,
        ),
    )


class FirstBookAuthenticationAPI(BasicAuthenticationProvider):
    @classmethod
    def label(cls) -> str:
        return "First Book"

    @classmethod
    def description(cls) -> str:
        return (
            "An authentication service for Open eBooks that authenticates using access codes and "
            "PINs. (This is the new version.)"
        )

    @classmethod
    def settings_class(cls) -> type[FirstBookAuthSettings]:
        return FirstBookAuthSettings

    @property
    def login_button_image(self) -> str | None:
        return "FirstBookLoginButton280.png"

    # The algorithm used to sign JWTs.
    ALGORITHM = "HS256"

    # If FirstBook sends this message it means they accepted the
    # patron's credentials.
    SUCCESS_MESSAGE = "Valid Code Pin Pair"

    def __init__(
        self,
        library_id: int,
        integration_id: int,
        settings: FirstBookAuthSettings,
        library_settings: BasicAuthProviderLibrarySettings,
        analytics=None,
    ):
        super().__init__(
            library_id, integration_id, settings, library_settings, analytics
        )
        self.root = settings.url
        self.secret = settings.password

    def remote_authenticate(
        self, username: Optional[str], password: Optional[str]
    ) -> Optional[PatronData]:
        # All FirstBook credentials are in upper-case.
        if username is None or username == "":
            return None

        username = username.upper()

        # If they fail a PIN test, there is no authenticated patron.
        if not self.remote_pin_test(username, password):
            return None

        # FirstBook keeps track of absolutely no information
        # about the patron other than the permanent ID,
        # which is also the authorization identifier.
        return PatronData(
            permanent_id=username,
            authorization_identifier=username,
        )

    def remote_patron_lookup(
        self, patron_or_patrondata: Union[PatronData, Patron]
    ) -> Optional[PatronData]:
        if isinstance(patron_or_patrondata, PatronData):
            return patron_or_patrondata

        return None

    # End implementation of BasicAuthenticationProvider abstract methods.

    def remote_pin_test(self, barcode, pin):
        jwt = self.jwt(barcode, pin)
        url = self.root + jwt
        try:
            response = self.request(url)
        except requests.exceptions.ConnectionError as e:
            raise RemoteInitiatedServerError(str(e), self.__class__.__name__)
        content = response.content.decode("utf8")
        if response.status_code != 200:
            msg = "Got unexpected response code %d. Content: %s" % (
                response.status_code,
                content,
            )
            raise RemoteInitiatedServerError(msg, self.__class__.__name__)
        if self.SUCCESS_MESSAGE in content:
            return True
        return False

    def jwt(self, barcode, pin):
        """Create and sign a JWT with the payload expected by the
        First Book API.
        """
        now = str(int(time.time()))
        payload = dict(
            barcode=barcode,
            pin=pin,
            iat=now,
        )
        return jwt.encode(payload, self.secret, algorithm=self.ALGORITHM)

    def request(self, url):
        """Make an HTTP request.

        Defined solely so it can be overridden in the mock.
        """
        return requests.get(url)
