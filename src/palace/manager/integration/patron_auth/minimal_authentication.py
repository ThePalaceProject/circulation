from __future__ import annotations

from palace.manager.api.authentication.base import (
    PatronData,
)
from palace.manager.api.authentication.basic import (
    BasicAuthenticationProvider,
    BasicAuthProviderLibrarySettings,
    BasicAuthProviderSettings,
)
from palace.manager.sqlalchemy.model.patron import Patron


class MinimalAuthenticationProvider(
    BasicAuthenticationProvider[
        BasicAuthProviderSettings, BasicAuthProviderLibrarySettings
    ]
):
    @classmethod
    def label(cls) -> str:
        return "Minimal Authentication Provider"

    @classmethod
    def description(cls) -> str:
        return (
            "An internal authentication service that provides a no op remote authentication. "
            "As long as the pre auth tests succeeds this auth provider will provide a successful authentication. "
            "It is useful for libraries that only want to perform only simple validation checks such as user barcodes "
            "matching a list of prefixes and do not require passwords and/or remote authentication checks."
        )

    @classmethod
    def settings_class(cls) -> type[BasicAuthProviderSettings]:
        return BasicAuthProviderSettings

    @classmethod
    def library_settings_class(cls) -> type[BasicAuthProviderLibrarySettings]:
        return BasicAuthProviderLibrarySettings

    def remote_authenticate(
        self, username: str | None, password: str | None
    ) -> PatronData | None:
        """No Auth authentication: Allow everyone through as long as they have a username."""
        if not username:
            return None

        return self.generate_patrondata(username)

    @classmethod
    def generate_patrondata(cls, authorization_identifier: str) -> PatronData:
        if authorization_identifier.endswith("_username"):
            username = authorization_identifier
            identifier = authorization_identifier[:-9]
        else:
            identifier = authorization_identifier
            username = authorization_identifier + "_username"

        personal_name = "PersonalName" + identifier

        patrondata = PatronData(
            authorization_identifier=identifier,
            permanent_id=identifier + "_id",
            username=username,
            personal_name=personal_name,
            authorization_expires=None,
            fines=None,
        )
        return patrondata

    def remote_patron_lookup(
        self, patron_or_patrondata: Patron | PatronData
    ) -> PatronData | None:
        if not patron_or_patrondata.authorization_identifier:
            return None

        return self.generate_patrondata(patron_or_patrondata.authorization_identifier)
