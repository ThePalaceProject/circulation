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
        return """
        An internal authentication service that provides a no op remote authentication.
        As long as the pre-auth tests succeed, this auth provider will provide a successful authentication.
        It is useful for libraries that only want to perform only simple validation checks such as user barcodes
        matching a list of prefixes and do not require passwords and/or remote authentication checks.
        Here are a few examples of pre-auth tests that you might use this to perform:
    <pre>
    Configurable at the individual library level:
      * Library Identifier Restriction based on a list of barcode prefixes
      * Library Identifier Restriction based on a list of exact barcode matches
      * Library Identifier Restriction based on a regex matches
    Configurable across all libraries:
      * Password match by regular expression
      * Barcode match by regular expression</pre>"""

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
        patrondata = PatronData(
            authorization_identifier=authorization_identifier,
            permanent_id=f"id:{authorization_identifier}",
            username=authorization_identifier,
            personal_name=f"Unavailable: {authorization_identifier}",
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
