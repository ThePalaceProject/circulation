from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from flask import url_for
from uritemplate import URITemplate

from api.circulation import BaseCirculationAPI, FulfillmentInfo, LoanInfo
from api.circulation_exceptions import NoLicenses, NotCheckedOut
from api.problem_details import CANNOT_FULFILL, INVALID_CREDENTIALS
from core.model import (
    Collection,
    ConfigurationSetting,
    ExternalIntegration,
    LicensePoolDeliveryMechanism,
)
from core.model.edition import Edition
from core.model.identifier import Identifier
from core.model.resource import Hyperlink
from core.opds2 import OPDS2Annotator
from core.util.datetime_helpers import utc_now
from core.util.http import HTTP
from core.util.problem_detail import ProblemDetail

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from core.model import LicensePool, Loan, Patron


class OPDS2PublicationsAnnotator(OPDS2Annotator):
    """API level implementation for the publications feed OPDS2 annotator"""

    def loan_link(self, edition: Edition) -> dict:
        identifier: Identifier = edition.primary_identifier
        return {
            "href": url_for(
                "borrow",
                identifier_type=identifier.type,
                identifier=identifier.identifier,
                library_short_name=self.library.short_name,
            ),
            "rel": Hyperlink.BORROW,
        }

    def self_link(self, edition: Edition) -> dict:
        identifier: Identifier = edition.primary_identifier
        return {
            "href": url_for(
                "permalink",
                identifier_type=identifier.type,
                identifier=identifier.identifier,
                library_short_name=self.library.short_name,
            ),
            "rel": "self",
        }


class OPDS2NavigationsAnnotator(OPDS2Annotator):
    """API level implementation for the navigation feed OPDS2 annotator"""

    def navigation_collection(self) -> dict:
        """The OPDS2 navigation collection, currently only serves the publications link"""
        return [
            {
                "href": url_for(
                    "opds2_publications", library_short_name=self.library.short_name
                ),
                "title": "OPDS2 Publications Feed",
                "type": self.OPDS2_TYPE,
            }
        ]

    def feed_metadata(self):
        return {"title": self.title}

    def feed_links(self):
        return [
            {"href": self.url, "rel": "self", "type": self.OPDS2_TYPE},
        ]


class OPDS2API(BaseCirculationAPI):
    """
    Generic OPDS2 Circulation APIs
    This should be a pure implementation with no references to daatasources
    """

    def __init__(self, db: Session, collection: Collection) -> None:
        self._db = db
        self.collection = collection
        self.log = logging.getLogger("OPDS2API")
        self.token_auth_url = self._get_setting(ExternalIntegration.TOKEN_AUTH)

    def _get_setting(self, key) -> str | None:
        return ConfigurationSetting.for_externalintegration(
            key, self.collection.external_integration
        ).value

    def _get_authentication_token(self, patron: Patron) -> ProblemDetail | str:
        """Get the authentication token for a patron"""
        url = URITemplate(self.token_auth_url).expand(patron_id=patron.username)
        response = HTTP.get_with_timeout(url)
        if response.status_code != 200:
            self.log.error(
                f"Could not authenticate the patron({patron.username}): {response.content}"
            )
            return INVALID_CREDENTIALS

        # The response should be the JWT token, not wrapped in any format like JSON
        token = response.json().get("token")
        if not token:
            self.log.error(
                f"Could not authenticate the patron({patron.username}): {response.content}"
            )
            return INVALID_CREDENTIALS

        return token

    def checkout(
        self, patron: Patron, pin: str, licensepool: LicensePool, internal_format
    ):
        # If we have authentication on this feed then authenticate before a checkout
        if self.token_auth_url:
            token = self._get_authentication_token(patron)
            if type(token) == ProblemDetail:
                return token

        if not licensepool.unlimited_access:
            if licensepool.licenses_available <= 0:
                raise NoLicenses

        return LoanInfo(
            self.collection,
            self.collection.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            utc_now(),
            None,
        )

    def internal_format(self, delivery_mechanism: LicensePoolDeliveryMechanism):
        # TODO: This thing
        return delivery_mechanism

    def checkin(self, patron: Patron, pin: str, licensepool: LicensePool):
        """Check if the patron has a loan on this pool
        The rest is managed by the CirculationAPI"""
        # If we have authentication on this feed then authenticate before a checkout
        if self.token_auth_url:
            token = self._get_authentication_token(patron)
            if type(token) == ProblemDetail:
                return token

        loan: Loan = None
        for loan in licensepool.loans:
            if loan.patron == patron:
                break
        else:
            raise NotCheckedOut

    def fulfill(
        self,
        patron: Patron,
        pin: str,
        licensepool: LicensePool,
        internal_format=None,
        part=None,
        fulfill_part_url=None,
    ):
        # If we have authentication on this feed then authenticate before a checkout
        if self.token_auth_url:
            token = self._get_authentication_token(patron)
            if type(token) == ProblemDetail:
                return token

        if (
            internal_format is None
            or type(internal_format) is not LicensePoolDeliveryMechanism
        ):
            self.log(f"No internal format provided, cannot fulfill")
            return CANNOT_FULFILL

        content_url = internal_format.resource.url
        if internal_format.resource.templated and "authentication_token" in content_url:
            content_url = URITemplate(content_url).expand(authentication_token=token)

        return FulfillmentInfo(
            self.collection,
            self.collection.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            content_url,
            internal_format.delivery_mechanism.content_type,
            None,
            None,
        )

    def patron_activity(self, patron, pin):
        return super().patron_activity(patron, pin)

    def place_hold(self, patron, pin, licensepool, notification_email_address):
        return super().place_hold(patron, pin, licensepool, notification_email_address)

    def release_hold(self, patron, pin, licensepool):
        return super().release_hold(patron, pin, licensepool)
