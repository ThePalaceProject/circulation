from __future__ import annotations

import urllib
from abc import ABC
from typing import Unpack

from sqlalchemy.orm import Session
from typing_extensions import TypeVar

from palace.manager.api.circulation.base import BaseCirculationAPI
from palace.manager.api.circulation.data import HoldInfo, LoanInfo
from palace.manager.api.circulation.exceptions import (
    CurrentlyAvailable,
    FormatNotAvailable,
    NotOnHold,
)
from palace.manager.api.circulation.fulfillment import RedirectFulfillment
from palace.manager.integration.license.opds.opds1.settings import (
    OPDSImporterLibrarySettings,
    OPDSImporterSettings,
)
from palace.manager.integration.license.opds.settings.format_priority import (
    FormatPriorities,
)
from palace.manager.integration.license.opds.settings.wayfless import (
    SAMLWAYFlessConstants,
    SAMLWAYFlessFulfillmentError,
)
from palace.manager.integration.patron_auth.saml.credential import SAMLCredentialManager
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.licensing import (
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.patron import Patron

BaseOPDSApiSettingsT = TypeVar("BaseOPDSApiSettingsT", bound=OPDSImporterSettings)
BaseOPDSLibrarySettingsT = TypeVar(
    "BaseOPDSLibrarySettingsT", bound=OPDSImporterLibrarySettings
)


class BaseOPDSAPI(
    BaseCirculationAPI[BaseOPDSApiSettingsT, BaseOPDSLibrarySettingsT], ABC
):
    def __init__(self, _db: Session, collection: Collection):
        super().__init__(_db, collection)
        self.saml_wayfless_url_template = self.settings.saml_wayfless_url_template
        self.saml_credential_manager = SAMLCredentialManager()
        self._format_priorities = FormatPriorities(
            self.settings.prioritized_drm_schemes,
            self.settings.prioritized_content_types,
            self.settings.deprioritize_lcp_non_epubs,
        )

    @property
    def data_source(self) -> DataSource:
        return DataSource.lookup(self._db, self.settings.data_source, autocreate=True)

    def checkin(self, patron: Patron, pin: str, licensepool: LicensePool) -> None:
        # All the CM side accounting for this loan is handled by CirculationAPI
        # since we don't have any remote API we need to call this method is
        # just a no-op.
        pass

    def release_hold(self, patron: Patron, pin: str, licensepool: LicensePool) -> None:
        # Since there is no such thing as a hold, there is no such
        # thing as releasing a hold.
        raise NotOnHold()

    def place_hold(
        self,
        patron: Patron,
        pin: str | None,
        licensepool: LicensePool,
        notification_email_address: str | None,
    ) -> HoldInfo:
        # Because all OPDS content is assumed to be simultaneously
        # available to all patrons, there is no such thing as a hold.
        raise CurrentlyAvailable()

    def update_availability(self, licensepool: LicensePool) -> None:
        # We already know all the availability information we're going
        # to know, so we don't need to do anything.
        pass

    def fulfill_saml_wayfless(
        self, template: str, patron: Patron, fulfillment: RedirectFulfillment
    ) -> RedirectFulfillment:
        self.log.debug(f"WAYFless acquisition link template: {template}")

        db = Session.object_session(patron)
        saml_credential = self.saml_credential_manager.lookup_saml_token_by_patron(
            db, patron
        )

        self.log.debug(f"SAML credentials: {saml_credential}")

        if not saml_credential:
            raise SAMLWAYFlessFulfillmentError(
                f"There are no existing SAML credentials for patron {patron}"
            )

        saml_subject = self.saml_credential_manager.extract_saml_token(saml_credential)

        self.log.debug(f"SAML subject: {saml_subject}")

        if not saml_subject.idp:
            raise SAMLWAYFlessFulfillmentError(
                f"SAML subject {saml_subject} does not contain an IdP's entityID"
            )

        acquisition_link = template.replace(
            SAMLWAYFlessConstants.IDP_PLACEHOLDER,
            urllib.parse.quote(saml_subject.idp, safe=""),
        )

        acquisition_link = acquisition_link.replace(
            SAMLWAYFlessConstants.ACQUISITION_LINK_PLACEHOLDER,
            urllib.parse.quote(fulfillment.content_link, safe=""),
        )

        self.log.debug(
            f"Old acquisition link {fulfillment.content_link} has been transformed to {acquisition_link}"
        )

        fulfillment.content_link = acquisition_link
        return fulfillment

    def fulfill(
        self,
        patron: Patron,
        pin: str,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism,
        **kwargs: Unpack[BaseCirculationAPI.FulfillKwargs],
    ) -> RedirectFulfillment:
        requested_mechanism = delivery_mechanism.delivery_mechanism
        rep = None
        for lpdm in licensepool.available_delivery_mechanisms:
            if (
                lpdm.resource is None
                or lpdm.resource.representation is None
                or lpdm.resource.representation.public_url is None
            ):
                # This LicensePoolDeliveryMechanism can't actually
                # be used for fulfillment.
                continue
            if lpdm.delivery_mechanism == requested_mechanism:
                # We found it! This is how the patron wants
                # the book to be delivered.
                rep = lpdm.resource.representation
                break

        if not rep:
            # There is just no way to fulfill this loan the way the
            # patron wants.
            raise FormatNotAvailable()

        content_link = rep.public_url
        media_type = rep.media_type

        fulfillment_strategy = RedirectFulfillment(
            content_link=content_link,
            content_type=media_type,
        )

        if self.saml_wayfless_url_template:
            fulfillment_strategy = self.fulfill_saml_wayfless(
                self.saml_wayfless_url_template, patron, fulfillment_strategy
            )

        return fulfillment_strategy

    def checkout(
        self,
        patron: Patron,
        pin: str | None,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism | None,
    ) -> LoanInfo:
        return LoanInfo.from_license_pool(licensepool, end_date=None)

    def can_fulfill_without_loan(
        self,
        patron: Patron | None,
        pool: LicensePool,
        lpdm: LicensePoolDeliveryMechanism,
    ) -> bool:
        return True

    def sort_delivery_mechanisms(
        self, lpdms: list[LicensePoolDeliveryMechanism]
    ) -> list[LicensePoolDeliveryMechanism]:
        return self._format_priorities.prioritize_mechanisms(lpdms)
