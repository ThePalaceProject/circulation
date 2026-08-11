from __future__ import annotations

import hashlib
import hmac
import json
import time
import urllib.parse
from collections.abc import Collection as CollectionT, Generator, Iterable
from datetime import datetime, timedelta
from io import BytesIO
from typing import Any, Unpack

from lxml.etree import Error
from pymarc import Record, parse_xml_to_array
from requests import Response
from sqlalchemy.orm import Session

from palace.util.datetime_helpers import utc_now

from palace.manager.api.circulation.base import (
    BaseCirculationAPI,
    PatronActivityCirculationAPI,
)
from palace.manager.api.circulation.data import HoldInfo, LoanInfo
from palace.manager.api.circulation.exceptions import (
    AlreadyCheckedOut,
    CannotHold,
    CannotReleaseHold,
    RemoteInitiatedServerError,
)
from palace.manager.api.circulation.fulfillment import DirectFulfillment
from palace.manager.api.selftest import HasCollectionSelfTests
from palace.manager.api.web_publication_manifest import FindawayManifest, SpineItem
from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.core.selftest import SelfTestResult
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.integration.license.bibliotheca.constants import (
    BIBLIOTHECA_LABEL,
    BIBLIOTHECA_SERVICE_NAME,
    BIBLIOTHECA_TIME_FORMAT,
)
from palace.manager.integration.license.bibliotheca.parser import (
    CheckoutResponseParser,
    ErrorParser,
    EventParser,
    HoldResponseParser,
    ItemListParser,
    PatronCirculationParser,
)
from palace.manager.integration.license.bibliotheca.settings import (
    BibliothecaLibrarySettings,
    BibliothecaSettings,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.sqlalchemy.model.resource import Representation
from palace.manager.util import base64
from palace.manager.util.http.exception import RemoteIntegrationException
from palace.manager.util.http.http import HTTP, RequestKwargs


class BibliothecaAPI(
    PatronActivityCirculationAPI[BibliothecaSettings, BibliothecaLibrarySettings],
    HasCollectionSelfTests,
):
    AUTH_TIME_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"
    ARGUMENT_TIME_FORMAT = BIBLIOTHECA_TIME_FORMAT
    AUTHORIZATION_FORMAT = "3MCLAUTH %s:%s"

    DATETIME_HEADER = "3mcl-Datetime"
    AUTHORIZATION_HEADER = "3mcl-Authorization"
    VERSION_HEADER = "3mcl-Version"

    DEFAULT_VERSION = "2.0"
    DEFAULT_BASE_URL = "https://partner.yourcloudlibrary.com/"

    MAX_AGE = timedelta(days=730).seconds
    CAN_REVOKE_HOLD_WHEN_RESERVED = False
    SET_DELIVERY_MECHANISM_AT = None

    SERVICE_NAME = BIBLIOTHECA_SERVICE_NAME

    @classmethod
    def settings_class(cls) -> type[BibliothecaSettings]:
        return BibliothecaSettings

    @classmethod
    def library_settings_class(cls) -> type[BibliothecaLibrarySettings]:
        return BibliothecaLibrarySettings

    @classmethod
    def label(cls) -> str:
        return BIBLIOTHECA_LABEL

    @classmethod
    def description(cls) -> str:
        return ""

    def __init__(self, _db: Session, collection: Collection) -> None:
        super().__init__(_db, collection)

        self._db = _db
        self.version = self.DEFAULT_VERSION
        self.account_id = self.settings.username
        self.account_key = self.settings.password
        self.library_id = self.settings.external_account_id
        self.base_url = self.DEFAULT_BASE_URL

        if not self.account_id or not self.account_key or not self.library_id:
            raise CannotLoadConfiguration("Bibliotheca configuration is incomplete.")

        self.item_list_parser = ItemListParser()
        self.collection_id = collection.id

    @property
    def data_source(self) -> DataSource:
        return DataSource.lookup(self._db, DataSource.BIBLIOTHECA, autocreate=True)

    def now(self) -> str:
        """Return the current GMT time in the format 3M expects."""
        return time.strftime(self.AUTH_TIME_FORMAT, time.gmtime())

    def sign(self, method: str, headers: dict[str, str], path: str) -> None:
        """Add appropriate headers to a request."""
        authorization, now = self.authorization(method, path)
        headers[self.DATETIME_HEADER] = now
        headers[self.VERSION_HEADER] = self.version
        headers[self.AUTHORIZATION_HEADER] = authorization

    def authorization(self, method: str, path: str) -> tuple[str, str]:
        signature, now = self.signature(method, path)
        auth = self.AUTHORIZATION_FORMAT % (self.account_id, signature)
        return auth, now

    def signature(self, method: str, path: str) -> tuple[str, str]:
        now = self.now()
        signature_components = [now, method, path]
        signature_string = "\n".join(signature_components)
        digest = hmac.new(
            self.account_key.encode("utf-8"),
            msg=signature_string.encode("utf-8"),
            digestmod=hashlib.sha256,
        ).digest()
        signature = base64.standard_b64encode(digest)
        return signature, now

    def full_url(self, path: str) -> str:
        if not path.startswith("/cirrus"):
            path = self.full_path(path)
        return urllib.parse.urljoin(self.base_url, path)

    def full_path(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        if not path.startswith("/cirrus"):
            path = f"/cirrus/library/{self.library_id}{path}"
        return path

    def request(
        self,
        path: str,
        body: str | None = None,
        method: str = "GET",
    ) -> Response:
        path = self.full_path(path)
        url = self.full_url(path)
        if method == "GET":
            headers = {"Accept": "application/xml"}
        else:
            headers = {"Content-Type": "application/xml"}
        self.sign(method, headers, path)
        return self._request_with_timeout(
            method,
            url,
            data=body,
            headers=headers,
            allow_redirects=False,
            timeout=60,
        )

    def marc_request(
        self, start: datetime, end: datetime, offset: int = 1, limit: int = 50
    ) -> Generator[Record]:
        """Make an HTTP request to look up the MARC records for books purchased
        between two given dates.

        :param start: A datetime to start looking for purchases.
        :param end: A datetime to stop looking for purchases.
        :param offset: An offset used to paginate results.
        :param limit: A limit used to paginate results.
        :raise: An appropriate exception if the request returned a non-200
          status code. An empty response body is not an error: it is treated
          as "no records" and the generator simply yields nothing.
        :yield: A list of MARC records.
        """
        start_param = start.strftime(self.ARGUMENT_TIME_FORMAT)
        end_param = end.strftime(self.ARGUMENT_TIME_FORMAT)
        url = "data/marc?startdate=%s&enddate=%s&offset=%d&limit=%d" % (
            start_param,
            end_param,
            offset,
            limit,
        )
        response = self.request(url)
        if response.status_code != 200:
            raise ErrorParser().process_first(response.content)
        if not response.content.strip():
            # Bibliotheca sometimes returns an empty body (HTTP 200 with no
            # XML document) for a window that contains no purchase records.
            # pymarc's parse_xml_to_array raises SAXException("no element
            # found") on an empty document, so treat an empty body as "no
            # records" and yield nothing rather than letting it propagate.
            self.log.info(
                f"Bibliotheca MARC request to '{url}' returned an empty "
                "response body; treating as no records."
            )
            return
        yield from parse_xml_to_array(BytesIO(response.content))

    def bibliographic_lookup_request(self, identifiers: CollectionT[str]) -> bytes:
        """Make an HTTP request to look up current bibliographic and
        circulation information for the given `identifiers`.

        :param identifiers: Strings containing Bibliotheca identifiers.
        :return: A string containing an XML document, or None if there was
           an error not handled as an exception.
        """
        url = "/items/" + ",".join(identifiers)
        response = self.request(url)
        return response.content

    def bibliographic_lookup(
        self, identifiers: CollectionT[str | Identifier] | str | Identifier
    ) -> list[BibliographicData]:
        """Look up current bibliographic and circulation information for the
        given `identifiers`.

        :param identifiers: A list containing either Identifier
            objects or Bibliotheca identifier strings.
        :raise RemoteInitiatedServerError: If Bibliotheca returns an empty
            response body (HTTP 200 with no XML document). See the comment
            below for why this is treated as a transient error rather than as
            "no items found".
        """
        identifiers_list = (
            [identifiers]
            if isinstance(identifiers, Identifier) or isinstance(identifiers, str)
            else identifiers
        )
        identifier_strings = []
        for i in identifiers_list:
            if isinstance(i, Identifier):
                i = i.identifier
            identifier_strings.append(i)

        data = self.bibliographic_lookup_request(identifier_strings)
        if not data.strip():
            # Bibliotheca occasionally returns an empty body (HTTP 200 with no
            # XML document). An empty document cannot be parsed as XML (lxml
            # raises "Document is empty" even in recovery mode). Unlike a
            # well-formed document that simply omits some of the requested
            # items, an empty body tells us nothing about which titles still
            # exist, so we must not treat it as "no items returned": that would
            # make BibliothecaCirculationUpdater._process_batch zero out the
            # availability of every requested identifier as if it had been
            # removed from circulation. Instead, raise a transient remote error
            # -- mirroring how marc_request/ErrorParser treat empty or malformed
            # Bibliotheca responses -- so the caller retries rather than
            # corrupting availability data.
            raise RemoteInitiatedServerError(
                "Bibliotheca returned an empty response body for a bibliographic lookup.",
                self.SERVICE_NAME,
            )
        return [
            bibliographic for bibliographic in self.item_list_parser.process_all(data)
        ]

    def _request_with_timeout(
        self, http_method: str, url: str, **kwargs: Unpack[RequestKwargs]
    ) -> Response:
        """This will be overridden in MockBibliothecaAPI."""
        return HTTP.request_with_timeout(http_method, url, **kwargs)

    def _run_self_tests(self, _db: Session) -> Generator[SelfTestResult]:
        def _count_events() -> str:
            now = utc_now()
            five_minutes_ago = now - timedelta(minutes=5)
            count = len(list(self.get_events_between(five_minutes_ago, now)))
            return "Found %d event(s)" % count

        yield self.run_test(
            "Asking for circulation events for the last five minutes", _count_events
        )

        for result in self.default_patrons(self.collection):
            if isinstance(result, SelfTestResult):
                yield result
                continue
            library, patron, pin = result

            def _count_activity() -> str:
                result = self.patron_activity(patron, pin)
                return "Found %d loans/holds" % len(list(result))

            yield self.run_test(
                "Checking activity for test patron for library %s" % library.name,
                _count_activity,
            )

    def get_events_between(
        self, start: datetime, end: datetime, no_events_error: bool = False
    ) -> Generator[tuple[str, str, str | None, datetime, datetime | None, str]]:
        """Return event objects for events between the given times."""
        start_str = start.strftime(self.ARGUMENT_TIME_FORMAT)
        end_str = end.strftime(self.ARGUMENT_TIME_FORMAT)
        url = f"data/cloudevents?startdate={start_str}&enddate={end_str}"
        response = self.request(url)
        try:
            events = EventParser().process_all(response.content, no_events_error)
        except Exception as e:
            self.log.error(
                "Error parsing Bibliotheca response content: %s",
                response.content,
                exc_info=e,
            )
            raise e
        return events

    def update_availability(self, licensepool: LicensePool) -> None:
        """Update the availability information for a single LicensePool."""
        # Local import to avoid circular dependency between api.py and
        # circulation_updater.py (the updater imports BibliothecaAPI).
        from palace.manager.integration.license.bibliotheca.circulation_updater import (
            BibliothecaCirculationUpdater,
        )

        updater = BibliothecaCirculationUpdater(
            self._db, licensepool.collection, api=self
        )
        updater.process_identifiers([licensepool.identifier])

    def _patron_activity_request(self, patron: Patron) -> Response:
        patron_id = patron.authorization_identifier
        path = "circulation/patron/%s" % patron_id
        return self.request(path)

    def patron_activity(
        self, patron: Patron, pin: str | None
    ) -> Iterable[LoanInfo | HoldInfo]:
        response = self._patron_activity_request(patron)
        try:
            return PatronCirculationParser(self.collection).process_all(
                response.content
            )
        except Error as e:
            # XML parse error from remote.
            raise RemoteIntegrationException(
                response.url, "Unable to parse response XML."
            ) from e

    TEMPLATE = "<%(request_type)s><ItemId>%(item_id)s</ItemId><PatronId>%(patron_id)s</PatronId></%(request_type)s>"

    def checkout(
        self,
        patron_obj: Patron,
        patron_password: str | None,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism | None,
    ) -> LoanInfo:
        """Check out a book on behalf of a patron.

        :param patron_obj: a Patron object for the patron who wants
            to check out the book.

        :param patron_password: The patron's alleged password.  Not used here
            since Bibliotheca trusts Simplified to do the check ahead of time.

        :param licensepool: LicensePool for the book to be checked out.

        :return: a LoanInfo object
        """
        bibliotheca_id = licensepool.identifier.identifier
        patron_identifier = patron_obj.authorization_identifier
        args = dict(
            request_type="CheckoutRequest",
            item_id=bibliotheca_id,
            patron_id=patron_identifier,
        )
        body = self.TEMPLATE % args
        response = self.request("checkout", body, method="PUT")
        if response.status_code == 201:
            # New loan
            start_date = utc_now()
        elif response.status_code == 200:
            # Old loan -- we don't know the start date
            start_date = None
        else:
            # Error condition.
            error = ErrorParser().process_first(response.content)
            if isinstance(error, AlreadyCheckedOut):
                # It's already checked out. No problem.
                pass
            else:
                raise error

        # At this point we know we have a loan.
        loan_expires = CheckoutResponseParser().process_first(response.content)
        loan = LoanInfo.from_license_pool(
            licensepool,
            end_date=loan_expires,
        )
        return loan

    def fulfill(
        self,
        patron: Patron,
        password: str,
        pool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism,
        **kwargs: Unpack[BaseCirculationAPI.FulfillKwargs],
    ) -> DirectFulfillment:
        """Get the actual resource file to the patron."""
        if (
            delivery_mechanism.delivery_mechanism.drm_scheme
            == DeliveryMechanism.FINDAWAY_DRM
        ):
            fulfill_method = self.get_audio_fulfillment_file
            content_transformation = self.findaway_license_to_webpub_manifest
        else:
            fulfill_method = self.get_fulfillment_file
            content_transformation = None
        response = fulfill_method(
            patron.authorization_identifier, pool.identifier.identifier
        )
        content: str | bytes = response.content
        content_type = None
        if content_transformation:
            try:
                content_type, content = content_transformation(pool, content)
            except Exception as e:
                self.log.error(
                    "Error transforming fulfillment document: %s",
                    response.content,
                    exc_info=e,
                )
        return DirectFulfillment(
            content=content,
            content_type=content_type or response.headers.get("Content-Type"),
        )

    def get_fulfillment_file(
        self, patron_id: str | None, bibliotheca_id: str
    ) -> Response:
        args = dict(
            request_type="ACSMRequest", item_id=bibliotheca_id, patron_id=patron_id
        )
        body = self.TEMPLATE % args
        return self.request("GetItemACSM", body, method="PUT")

    def get_audio_fulfillment_file(
        self, patron_id: str | None, bibliotheca_id: str
    ) -> Response:
        args = dict(
            request_type="AudioFulfillmentRequest",
            item_id=bibliotheca_id,
            patron_id=patron_id,
        )
        body = self.TEMPLATE % args
        return self.request("GetItemAudioFulfillment", body, method="POST")

    def checkin(self, patron: Patron, pin: str, licensepool: LicensePool) -> None:
        patron_id = patron.authorization_identifier
        item_id = licensepool.identifier.identifier
        args = dict(request_type="CheckinRequest", item_id=item_id, patron_id=patron_id)
        body = self.TEMPLATE % args
        self.request("checkin", body, method="PUT")

    def place_hold(
        self,
        patron: Patron,
        pin: str | None,
        licensepool: LicensePool,
        notification_email_address: str | None = None,
    ) -> HoldInfo:
        """Place a hold.

        :return: a HoldInfo object.
        """
        patron_id = patron.authorization_identifier
        item_id = licensepool.identifier.identifier
        args = dict(
            request_type="PlaceHoldRequest", item_id=item_id, patron_id=patron_id
        )
        body = self.TEMPLATE % args
        response = self.request("placehold", body, method="PUT")
        # The response comes in as a byte string that we must
        # convert into a string.
        response_content = None
        if response.content:
            response_content = response.content.decode("utf-8")
        if response.status_code in (200, 201):
            start_date = utc_now()
            end_date = HoldResponseParser().process_first(response_content)
            return HoldInfo.from_license_pool(
                licensepool,
                start_date=start_date,
                end_date=end_date,
                hold_position=None,
            )
        else:
            if not response_content:
                raise CannotHold()
            error = ErrorParser().process_first(response_content)
            if isinstance(error, Exception):
                raise error
            else:
                raise CannotHold(error)

    def release_hold(self, patron: Patron, pin: str, licensepool: LicensePool) -> None:
        patron_id = patron.authorization_identifier
        item_id = licensepool.identifier.identifier
        args = dict(
            request_type="CancelHoldRequest", item_id=item_id, patron_id=patron_id
        )
        body = self.TEMPLATE % args
        response = self.request("cancelhold", body, method="PUT")
        if response.status_code not in (200, 404):
            raise CannotReleaseHold()

    @classmethod
    def findaway_license_to_webpub_manifest(
        cls, license_pool: LicensePool, findaway_license: str | bytes | dict[str, Any]
    ) -> tuple[str, str]:
        """Convert a Bibliotheca license document to a FindawayManifest
        suitable for serving to a mobile client.

        :param license_pool: A LicensePool for the title in question.
            This will be used to fill in basic bibliographic information.

        :param findaway_license: A string containing a Findaway
            license document via Bibliotheca, or a dictionary
            representing such a document loaded into JSON form.
        """
        if isinstance(findaway_license, (bytes, str)):
            findaway_license = json.loads(findaway_license)
            assert isinstance(
                findaway_license, dict
            ), "Expected a JSON object, got %s" % type(findaway_license)

        kwargs = {}
        for findaway_extension in [
            "accountId",
            "checkoutId",
            "fulfillmentId",
            "licenseId",
            "sessionKey",
        ]:
            value = findaway_license.get(findaway_extension, None)
            kwargs[findaway_extension] = value

        # Create the SpineItem objects.
        audio_format = findaway_license.get("format")
        if audio_format == "MP3":
            part_media_type = Representation.MP3_MEDIA_TYPE
        else:
            cls.logger().error(
                "Unknown Findaway audio format encountered: %s", audio_format
            )
            part_media_type = None

        spine_items = []
        for part in findaway_license["items"]:
            title = part.get("title")

            # TODO: Incoming duration appears to be measured in
            # milliseconds. This assumption makes our example
            # audiobook take about 7.9 hours, and no other reasonable
            # assumption is in the right order of magnitude. But this
            # needs to be explicitly verified.
            duration = part.get("duration", 0) / 1000.0

            part_number = int(part.get("part", 0))

            sequence = int(part.get("sequence", 0))

            spine_items.append(SpineItem(title, duration, part_number, sequence))

        # Create a FindawayManifest object and then convert it
        # to a string.
        manifest = FindawayManifest(
            license_pool=license_pool, spine_items=spine_items, **kwargs
        )

        return DeliveryMechanism.FINDAWAY_DRM, str(manifest)


BibliothecaApiClassT = type[BibliothecaAPI] | BibliothecaAPI
