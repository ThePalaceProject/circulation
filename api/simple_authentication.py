from typing import List, Optional, Type, Union

from core.analytics import Analytics
from core.integration.settings import (
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
)
from core.model import Patron

from .authentication.base import PatronData
from .authentication.basic import (
    BasicAuthenticationProvider,
    BasicAuthProviderLibrarySettings,
    BasicAuthProviderSettings,
)
from .config import CannotLoadConfiguration


class SimpleAuthSettings(BasicAuthProviderSettings):
    test_identifier: str = FormField(
        ...,
        form=ConfigurationFormItem(
            label="Test identifier",
            description="A test identifier to use when testing the authentication provider.",
            required=True,
        ),
    )
    test_password: str = FormField(
        ...,
        form=ConfigurationFormItem(
            label="Test password",
            description="A test password to use when testing the authentication provider.",
        ),
    )
    additional_test_identifiers: Optional[List[str]] = FormField(
        None,
        form=ConfigurationFormItem(
            label="Additional test identifiers",
            description="Identifiers for additional patrons to use in testing. "
            "The identifiers will all use the same test password as the first identifier.",
            type=ConfigurationFormItemType.LIST,
        ),
    )
    neighborhood: Optional[str] = FormField(
        None,
        form=ConfigurationFormItem(
            label="Test neighborhood",
            description="For analytics purposes, all patrons will be 'from' this neighborhood.",
        ),
    )


class SimpleAuthenticationProvider(BasicAuthenticationProvider):
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
    def settings_class(cls) -> Type[SimpleAuthSettings]:
        return SimpleAuthSettings

    def __init__(
        self,
        library_id: int,
        integration_id: int,
        settings: SimpleAuthSettings,
        library_settings: BasicAuthProviderLibrarySettings,
        analytics: Optional[Analytics] = None,
    ):
        super().__init__(
            library_id, integration_id, settings, library_settings, analytics
        )

        self.test_password = settings.test_password
        test_identifier = settings.test_identifier
        if not (test_identifier and self.test_password):
            raise CannotLoadConfiguration("Test identifier and password not set.")

        self.test_identifiers = [test_identifier, test_identifier + "_username"]
        additional_identifiers = settings.additional_test_identifiers
        if additional_identifiers:
            for identifier in additional_identifiers:
                self.test_identifiers += [identifier, identifier + "_username"]

        self.test_neighborhood = settings.neighborhood

    def remote_authenticate(
        self, username: Optional[str], password: Optional[str]
    ) -> Optional[PatronData]:
        """Fake 'remote' authentication."""
        if not username or (self.collects_password and not password):
            return None

        if not self.valid_patron(username, password):
            return None

        return self.generate_patrondata(username, self.test_neighborhood)

    @classmethod
    def generate_patrondata(
        cls, authorization_identifier: str, neighborhood: Optional[str] = None
    ) -> PatronData:
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
            neighborhood=neighborhood,
        )
        return patrondata

    def valid_patron(self, username: str, password: Optional[str]) -> bool:
        """Is this patron associated with the given password in
        the given dictionary?
        """
        if self.collects_password:
            password_match = password == self.test_password
        else:
            password_match = password in (None, "")
        return password_match and username in self.test_identifiers

    def remote_patron_lookup(
        self, patron_or_patrondata: Union[Patron, PatronData]
    ) -> Optional[PatronData]:
        if not patron_or_patrondata:
            return None
        if (
            not patron_or_patrondata.authorization_identifier
            or patron_or_patrondata.authorization_identifier
            not in self.test_identifiers
        ):
            return None

        return self.generate_patrondata(patron_or_patrondata.authorization_identifier)
