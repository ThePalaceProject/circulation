from __future__ import annotations

from typing import TYPE_CHECKING

from uritemplate import URITemplate

from api.circulation import CirculationFulfillmentPostProcessor, FulfillmentInfo
from api.circulation_exceptions import CannotFulfill
from core.model import ConfigurationSetting, DataSource, ExternalIntegration
from core.model.licensing import LicensePoolDeliveryMechanism
from core.problem_details import INVALID_CREDENTIALS
from core.util.http import HTTP
from core.util.log import LoggerMixin
from core.util.problem_detail import ProblemDetail

if TYPE_CHECKING:
    from core.model import LicensePool, Patron


class TokenAuthenticationFulfillmentProcessor(
    CirculationFulfillmentPostProcessor, LoggerMixin
):
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
        delivery_mechanism: LicensePoolDeliveryMechanism | None,
        fulfillment: FulfillmentInfo,
    ) -> FulfillmentInfo:
        if not fulfillment.content_link:
            return fulfillment

        templated = URITemplate(fulfillment.content_link)
        if "authentication_token" not in templated.variable_names:
            return fulfillment

        # TODO: This needs to be refactored to use IntegrationConfiguration,
        #  but it has been temporarily rolled back, since the IntegrationConfiguration
        #  code caused problems fulfilling TOKEN_AUTH books in production.
        #  This should be fixed as part of the work PP-313 to fully remove
        #  ExternalIntegrations from our collections code.
        token_auth = ConfigurationSetting.for_externalintegration(
            ExternalIntegration.TOKEN_AUTH, licensepool.collection.external_integration
        )
        if not token_auth or token_auth.value is None:
            return fulfillment

        token = self.get_authentication_token(
            patron, licensepool.data_source, token_auth.value
        )
        if isinstance(token, ProblemDetail):
            raise CannotFulfill()

        fulfillment.content_link = templated.expand(authentication_token=token)
        fulfillment.content_link_redirect = True
        return fulfillment

    @classmethod
    def get_authentication_token(
        cls, patron: Patron, datasource: DataSource, token_auth_url: str
    ) -> ProblemDetail | str:
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
            return INVALID_CREDENTIALS

        # The response should be the JWT token, not wrapped in any format like JSON
        token = response.text
        if not token:
            log.error(
                f"Could not authenticate the patron({patron_id}): {str(response.content)}"
            )
            return INVALID_CREDENTIALS

        return token
