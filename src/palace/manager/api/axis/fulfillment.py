from __future__ import annotations

import ssl
import urllib
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import certifi
from sqlalchemy.orm import Session

from palace.manager.api.circulation import Fulfillment, UrlFulfillment
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism, LicensePool
from palace.manager.util.flask_util import Response
from palace.manager.util.http import RemoteIntegrationException
from palace.manager.util.log import LoggerMixin

if TYPE_CHECKING:
    from palace.manager.api.axis.api import Axis360API


class Axis360Fulfillment(Fulfillment, LoggerMixin):
    """An Axis 360-specific Fulfillment implementation for audiobooks
    and books served through AxisNow.

    We use these instead of normal Fulfillment objects because
    putting all this information into Fulfillment would require
    one or two extra HTTP requests, and there's often no need to make
    those requests.
    """

    def __init__(
        self,
        api: Axis360API,
        data_source_name: str,
        identifier_type: str,
        identifier: str,
        key: str,
    ):
        """Constructor.

        :param api: An Axis360API instance, in case the parsing of
        a fulfillment document triggers additional API requests.

        :param key: The transaction ID that will be used to fulfill
        the request.
        """
        self.data_source_name = data_source_name
        self.identifier_type = identifier_type
        self.identifier = identifier
        self.api = api
        self.key = key

        self.content_type: str | None = None
        self.content: str | None = None

    def license_pool(self, _db: Session) -> LicensePool:
        """Find the LicensePool model object corresponding to this object."""
        collection = self.api.collection
        pool, is_new = LicensePool.for_foreign_id(
            _db,
            self.data_source_name,
            self.identifier_type,
            self.identifier,
            collection=collection,
        )
        return pool

    def do_fetch(self) -> tuple[str, str]:
        from palace.manager.api.axis.parser import Axis360FulfillmentInfoResponseParser

        _db = self.api._db
        license_pool = self.license_pool(_db)
        transaction_id = self.key
        response = self.api.get_fulfillment_info(transaction_id)
        parser = Axis360FulfillmentInfoResponseParser(self.api)
        manifest, expires = parser.parse(response.content, license_pool=license_pool)
        return str(manifest), manifest.MEDIA_TYPE

    def response(self) -> Response:
        if self.content is None:
            self.content, self.content_type = self.do_fetch()
        return Response(response=self.content, content_type=self.content_type)


class Axis360AcsFulfillment(UrlFulfillment, LoggerMixin):
    """This implements a Axis 360 specific Fulfillment for ACS content
    served through AxisNow. The AxisNow API gives us a link that we can use
    to get the ACSM file that we serve to the mobile apps.

    This link resolves to a redirect, which resolves to the actual ACSM file.
    The URL we are given in the redirect has a percent encoded query string
    in it. The encoding used in this string has lower case characters in it
    like "%3a" for :.

    In versions of urllib3 > 1.24.3 the library normalizes the query string
    before doing the actual request. In doing the normalization it follows the
    recommendation of RFC 3986 and uppercases the percent encoded bytes.

    This causes the Axis360 API to return an error from Adobe ACS:
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
    to the Axis 360 API, sidestepping the problem, but taking a different
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
