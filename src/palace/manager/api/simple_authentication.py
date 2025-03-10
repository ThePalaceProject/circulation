from palace.manager.api.authentication.base import PatronData
from palace.manager.api.authentication.basic import (
    BasicAuthenticationProvider,
    BasicAuthProviderLibrarySettings,
    BasicAuthProviderSettings,
)
from palace.manager.integration.settings import (
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
)
from palace.manager.service.analytics.analytics import Analytics
from palace.manager.sqlalchemy.model.patron import Patron


class SimpleAuthSettings(BasicAuthProviderSettings):
    test_identifier: str = FormField(
        ...,
        form=ConfigurationFormItem(
            label="Test identifier",
            description="A test identifier to use when testing the authentication provider.",
            required=True,
        ),
    )
    test_password: str | None = FormField(
        None,
        form=ConfigurationFormItem(
            required=True,
            label="Test password",
            description="A test password to use when testing the authentication provider. If you do not want to "
            "collect passwords, leave this field blank and set the 'Keyboard for password entry' option to "
            "'patrons have no password'.",
        ),
    )
    additional_test_identifiers: list[str] | None = FormField(
        None,
        form=ConfigurationFormItem(
            label="Additional test identifiers",
            description="Identifiers for additional patrons to use in testing. "
            "The identifiers will all use the same test password as the first identifier.",
            type=ConfigurationFormItemType.LIST,
        ),
    )


class SimpleAuthenticationProvider(
    BasicAuthenticationProvider[SimpleAuthSettings, BasicAuthProviderLibrarySettings]
):
    """An authentication provider that authenticates a single patron.

    This serves only one purpose: to set up a working circulation
    manager before connecting it to an ILS.
    """

    @classmethod
    def label(cls) -> str:
        return "Simple Authentication Provider"

    @classmethod
    def description(cls) -> str:
        return (
            "An internal authentication service that authenticates a single patron. "
            "This is useful for testing a circulation manager before connecting "
            "it to an ILS."
        )

    @classmethod
    def settings_class(cls) -> type[SimpleAuthSettings]:
        return SimpleAuthSettings

    @classmethod
    def library_settings_class(cls) -> type[BasicAuthProviderLibrarySettings]:
        return BasicAuthProviderLibrarySettings

    def __init__(
        self,
        library_id: int,
        integration_id: int,
        settings: SimpleAuthSettings,
        library_settings: BasicAuthProviderLibrarySettings,
        analytics: Analytics | None = None,
    ):
        super().__init__(
            library_id, integration_id, settings, library_settings, analytics
        )

        self.test_password = settings.test_password
        test_identifier = settings.test_identifier

        self.test_identifiers = [test_identifier, test_identifier + "_username"]
        additional_identifiers = settings.additional_test_identifiers
        if additional_identifiers:
            for identifier in additional_identifiers:
                self.test_identifiers += [identifier, identifier + "_username"]

    def remote_authenticate(
        self, username: str | None, password: str | None
    ) -> PatronData | None:
        """Fake 'remote' authentication."""
        if not username or (self.collects_password and not password):
            return None

        if not self.valid_patron(username, password):
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

    def valid_patron(self, username: str, password: str | None) -> bool:
        """Is this patron associated with the given password in
        the given dictionary?
        """
        if self.collects_password:
            password_match = password == self.test_password
        else:
            password_match = password in (None, "")
        return password_match and username in self.test_identifiers

    def remote_patron_lookup(
        self, patron_or_patrondata: Patron | PatronData
    ) -> PatronData | None:
        if not patron_or_patrondata:
            return None
        if (
            not patron_or_patrondata.authorization_identifier
            or patron_or_patrondata.authorization_identifier
            not in self.test_identifiers
        ):
            return None

        return self.generate_patrondata(patron_or_patrondata.authorization_identifier)
