from __future__ import annotations

from celery.canvas import Signature
from sqlalchemy.orm import Session
from typing_extensions import Unpack
from uritemplate import URITemplate

from palace.manager.api.circulation.base import BaseCirculationAPI
from palace.manager.api.circulation.exceptions import CannotFulfill
from palace.manager.api.circulation.fulfillment import RedirectFulfillment
from palace.manager.integration.license.opds.base.api import BaseOPDSAPI
from palace.manager.integration.license.opds.opds2.settings import (
    OPDS2ImporterLibrarySettings,
    OPDS2ImporterSettings,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.licensing import (
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.util.http.http import HTTP


class OPDS2API(BaseOPDSAPI):
    TOKEN_AUTH_CONFIG_KEY = "token_auth_endpoint"

    @classmethod
    def settings_class(cls) -> type[OPDS2ImporterSettings]:
        return OPDS2ImporterSettings

    @classmethod
    def library_settings_class(cls) -> type[OPDS2ImporterLibrarySettings]:
        return OPDS2ImporterLibrarySettings

    @classmethod
    def label(cls) -> str:
        return "OPDS 2.0 Import"

    @classmethod
    def description(cls) -> str:
        return "Import books from a publicly-accessible OPDS 2.0 feed."

    def __init__(self, _db: Session, collection: Collection):
        super().__init__(_db, collection)
        self.token_auth_configuration: str | None = (
            collection.integration_configuration.context.get(self.TOKEN_AUTH_CONFIG_KEY)
        )

    @classmethod
    def get_authentication_token(
        cls, patron: Patron, datasource: DataSource, token_auth_url: str
    ) -> str:
        """Get the authentication token for a patron"""
        log = cls.logger()

        patron_id = patron.identifier_to_remote_service(datasource)
        url = URITemplate(token_auth_url).expand(patron_id=patron_id)
        response = HTTP.get_with_timeout(url)
        if response.status_code != 200:
            log.error(
                f"Could not authenticate the patron (authorization identifier: '{patron.authorization_identifier}' "
                f"external identifier: '{patron_id}'): {str(response.content)}"
            )
            raise CannotFulfill()

        # The response should be the JWT token, not wrapped in any format like JSON
        token = response.text
        if not token:
            log.error(
                f"Could not authenticate the patron({patron_id}): {str(response.content)}"
            )
            raise CannotFulfill()

        return token

    def fulfill_token_auth(
        self,
        patron: Patron,
        licensepool: LicensePool,
        fulfillment: RedirectFulfillment,
    ) -> RedirectFulfillment:
        templated = URITemplate(fulfillment.content_link)
        if "authentication_token" not in templated.variable_names:
            self.log.warning(
                "No authentication_token variable found in content_link, unable to fulfill via OPDS2 token auth."
            )
            return fulfillment

        if not self.token_auth_configuration:
            self.log.warning(
                "No token auth configuration found, unable to fulfill via OPDS2 token auth."
            )
            return fulfillment

        token = self.get_authentication_token(
            patron, licensepool.data_source, self.token_auth_configuration
        )
        fulfillment.content_link = templated.expand(authentication_token=token)
        return fulfillment

    def fulfill(
        self,
        patron: Patron,
        pin: str,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism,
        **kwargs: Unpack[BaseCirculationAPI.FulfillKwargs],
    ) -> RedirectFulfillment:
        fulfillment = super().fulfill(
            patron, pin, licensepool, delivery_mechanism, **kwargs
        )
        if self.token_auth_configuration:
            fulfillment = self.fulfill_token_auth(patron, licensepool, fulfillment)
        return fulfillment

    @classmethod
    def import_task(cls, collection_id: int, force: bool = False) -> Signature:
        from palace.manager.celery.tasks.opds2 import import_collection

        return import_collection.s(collection_id, force=force)

    @classmethod
    def update_collection_token_auth_url(cls, collection: Collection, url: str) -> bool:
        """
        Update the collection's integration context with the token authentication URL.

        This method checks if the provided URL matches the current token authentication
        URL in the collection's integration context. If it does not match, it updates
        the context with the new URL and returns True. If it matches, it returns False
        without making any changes.
        """
        integration = collection.integration_configuration
        if integration.context.get(cls.TOKEN_AUTH_CONFIG_KEY) == url:
            # No change, so we don't need to update the context.
            return False

        integration.context_update({cls.TOKEN_AUTH_CONFIG_KEY: url})
        return True
