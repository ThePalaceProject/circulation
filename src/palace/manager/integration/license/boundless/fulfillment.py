from __future__ import annotations

import ssl
import urllib
from urllib.parse import urlparse

import certifi

from palace.manager.api.circulation.fulfillment import UrlFulfillment
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism
from palace.manager.util.flask_util import Response
from palace.manager.util.http.exception import RemoteIntegrationException
from palace.manager.util.log import LoggerMixin


class BoundlessAcsFulfillment(UrlFulfillment, LoggerMixin):
    """This implements Boundless specific Fulfillment for ACS content
    served through AxisNow. The API gives us a link that we can use
    to get the ACSM file that we serve to the mobile apps.

    This link resolves to a redirect, which resolves to the actual ACSM file.
    The URL we are given in the redirect has a percent encoded query string
    in it. The encoding used in this string has lower case characters in it
    like "%3a" for :.

    In versions of urllib3 > 1.24.3 the library normalizes the query string
    before doing the actual request. In doing the normalization it follows the
    recommendation of RFC 3986 and uppercases the percent encoded bytes.

    This causes the API to return an error from Adobe ACS:
    ```
    <error xmlns="http://ns.adobe.com/adept" data="E_URLLINK_AUTH
    https://acsqa.digitalcontentcafe.com/fulfillment/URLLink.acsm"/>
    ```
    instead of the correct ACSM file.

    Others have noted that this is a problem in the urllib3 github but they
    do not seem interested in providing an option to override this behavior
    and closed the ticket.
    https://github.com/urllib3/urllib3/issues/1677

    This Fulfillment implementation uses the built in Python urllib
    implementation instead of requests (and urllib3) to make this request
    to the API, sidestepping the problem, but taking a different
    code path than most of our external HTTP requests.
    """

    def __init__(
        self,
        content_link: str,
        verify: bool,
        content_type: str = DeliveryMechanism.ADOBE_DRM,
    ) -> None:
        super().__init__(content_link, content_type)
        self.verify = verify

    def response(self) -> Response:
        service_name = urlparse(str(self.content_link)).netloc
        try:
            if self.verify:
                # Actually verify the ssl certificates
                ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS)
                ssl_context.verify_mode = ssl.CERT_REQUIRED
                ssl_context.check_hostname = True
                ssl_context.load_verify_locations(cafile=certifi.where())
            else:
                # Default context does no ssl verification
                ssl_context = ssl.SSLContext()
            req = urllib.request.Request(self.content_link)
            with urllib.request.urlopen(
                req, timeout=20, context=ssl_context
            ) as response:
                content = response.read()
                status = response.status
                headers = response.headers

        # Mimic the behavior of the HTTP.request_with_timeout class and
        # wrap the exceptions thrown by urllib and ssl by raising a RemoteIntegrationException
        except urllib.error.HTTPError as e:
            message = f"The server received a bad status code ({e.code}) while contacting {service_name}"
            self.log.warning(message)
            raise RemoteIntegrationException(service_name, message) from e
        except TimeoutError as e:
            message = f"Error connecting to {service_name}. Timeout occurred."
            self.log.warning(message)
            raise RemoteIntegrationException(service_name, message) from e
        except (urllib.error.URLError, ssl.SSLError) as e:
            reason = getattr(e, "reason", e.__class__.__name__)
            message = f"Error connecting to {service_name}. {reason}."
            self.log.warning(message)
            raise RemoteIntegrationException(service_name, message) from e

        return Response(response=content, status=status, headers=headers)
