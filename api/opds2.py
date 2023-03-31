from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from flask import url_for
from uritemplate import URITemplate

from api.circulation import CirculationFulfillmentPostProcessor, FulfillmentInfo
from api.circulation_exceptions import CannotFulfill
from core.model import ConfigurationSetting, ExternalIntegration
from core.model.edition import Edition
from core.model.identifier import Identifier
from core.model.licensing import DeliveryMechanism
from core.model.resource import Hyperlink
from core.opds2 import OPDS2Annotator
from core.problem_details import INVALID_CREDENTIALS
from core.util.http import HTTP
from core.util.problem_detail import ProblemDetail

if TYPE_CHECKING:
    from core.model import LicensePool, Patron


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

    def navigation_collection(self) -> list[dict]:
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


class TokenAuthenticationFulfillmentProcessor(CirculationFulfillmentPostProcessor):
    """In case a feed has a token auth endpoint and the content_link requires an authentication token
    Then we must fetch the required authentication token from the token_auth endpoint and
    expand the templated url with the received token.
    The content link should also be a redirect and not a proxy download"""

    def __init__(self, collection) -> None:
        pass

    def fulfill(
        self,
        patron: Patron,
        pin: str,
        licensepool: LicensePool,
        delivery_mechanism: DeliveryMechanism | None,
        fulfillment: FulfillmentInfo,
    ) -> FulfillmentInfo:
        if not fulfillment.content_link:
            return fulfillment

        templated = URITemplate(fulfillment.content_link)
        if "authentication_token" not in templated.variable_names:
            return fulfillment

        token_auth = ConfigurationSetting.for_externalintegration(
            ExternalIntegration.TOKEN_AUTH, licensepool.collection.external_integration
        )
        if not token_auth or token_auth.value is None:
            return fulfillment

        token = self.get_authentication_token(patron, token_auth.value)
        if isinstance(token, ProblemDetail):
            raise CannotFulfill()

        fulfillment.content_link = templated.expand(authentication_token=token)
        fulfillment.content_link_redirect = True
        return fulfillment

    @classmethod
    def get_authentication_token(
        cls, patron: Patron, token_auth_url: str
    ) -> ProblemDetail | str:
        """Get the authentication token for a patron"""
        log = logging.getLogger("OPDS2API")
        if patron.username is None:
            log.error(
                f"Could not authenticate the patron({patron.authorization_identifier}), username is None."
            )
            return INVALID_CREDENTIALS

        url = URITemplate(token_auth_url).expand(patron_id=patron.username)
        response = HTTP.get_with_timeout(url)
        if response.status_code != 200:
            log.error(
                f"Could not authenticate the patron({patron.username}): {str(response.content)}"
            )
            return INVALID_CREDENTIALS

        # The response should be the JWT token, not wrapped in any format like JSON
        token = response.text
        if not token:
            log.error(
                f"Could not authenticate the patron({patron.username}): {str(response.content)}"
            )
            return INVALID_CREDENTIALS

        return token
