from __future__ import annotations

import hashlib
import hmac
import html
import itertools
import json
import logging
import re
import time
import urllib.parse
from abc import ABC
from collections.abc import Collection as CollectionT, Generator, Iterable
from datetime import datetime, timedelta
from io import BytesIO
from typing import Annotated, Any, Literal, Optional, Unpack, overload

from flask_babel import lazy_gettext as _
from frozendict import frozendict
from lxml.etree import Error, _Element
from pymarc import Record, parse_xml_to_array
from requests import Response
from sqlalchemy.orm import Session

from palace.util.datetime_helpers import (
    strptime_utc,
    to_utc,
    utc_now,
)
from palace.util.log import LoggerMixin

from palace.manager.api.circulation.base import (
    BaseCirculationAPI,
    PatronActivityCirculationAPI,
)
from palace.manager.api.circulation.data import HoldInfo, LoanInfo
from palace.manager.api.circulation.exceptions import (
    AlreadyCheckedOut,
    AlreadyOnHold,
    CannotHold,
    CannotLoan,
    CannotReleaseHold,
    CurrentlyAvailable,
    NoAvailableCopies,
    NoLicenses,
    NotCheckedOut,
    NotOnHold,
    PatronHoldLimitReached,
    PatronLoanLimitReached,
    RemoteInitiatedServerError,
)
from palace.manager.api.circulation.fulfillment import DirectFulfillment
from palace.manager.api.circulation.settings import (
    BaseCirculationApiSettings,
    BaseCirculationLoanSettings,
)
from palace.manager.api.selftest import HasCollectionSelfTests
from palace.manager.api.web_publication_manifest import FindawayManifest, SpineItem
from palace.manager.core.config import (
    CannotLoadConfiguration,
    ConfigurationAttributeValue,
)
from palace.manager.core.selftest import SelfTestResult
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.contributor import ContributorData
from palace.manager.data_layer.format import FormatData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.link import LinkData
from palace.manager.data_layer.measurement import MeasurementData
from palace.manager.data_layer.subject import SubjectData
from palace.manager.integration.settings import (
    FormFieldType,
    FormMetadata,
)
from palace.manager.sqlalchemy.constants import DataSourceConstants
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.classification import Classification, Subject
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    LicensePoolDeliveryMechanism,
    LicensePoolStatus,
)
from palace.manager.sqlalchemy.model.measurement import Measurement
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.util import base64
from palace.manager.util.http.exception import RemoteIntegrationException
from palace.manager.util.http.http import HTTP, RequestKwargs
from palace.manager.util.problem_detail import BaseProblemDetailException
from palace.manager.util.xmlparser import XMLParser, XMLProcessor


class BibliothecaSettings(BaseCirculationApiSettings):
    username: Annotated[
        str,
        FormMetadata(
            label=_("Account ID"),
            required=True,
        ),
    ]
    password: Annotated[
        str,
        FormMetadata(
            label=_("Account Key"),
            required=True,
        ),
    ]
    external_account_id: Annotated[
        str,
        FormMetadata(
            label=_("Library ID"),
            required=True,
        ),
    ]


class BibliothecaLibrarySettings(BaseCirculationLoanSettings):
    dont_display_reserves: Annotated[
        ConfigurationAttributeValue,
        FormMetadata(
            label=_("Show/Hide Titles with No Available Loans"),
            required=False,
            description=_(
                "Titles with no available loans will not be displayed in the Catalog view."
            ),
            type=FormFieldType.SELECT,
            options={
                ConfigurationAttributeValue.YESVALUE: "Show",
                ConfigurationAttributeValue.NOVALUE: "Hide",
            },
        ),
    ] = ConfigurationAttributeValue.YESVALUE


class BibliothecaAPI(
    PatronActivityCirculationAPI[BibliothecaSettings, BibliothecaLibrarySettings],
    HasCollectionSelfTests,
):
    AUTH_TIME_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"
    ARGUMENT_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
    AUTHORIZATION_FORMAT = "3MCLAUTH %s:%s"

    DATETIME_HEADER = "3mcl-Datetime"
    AUTHORIZATION_HEADER = "3mcl-Authorization"
    VERSION_HEADER = "3mcl-Version"

    DEFAULT_VERSION = "2.0"
    DEFAULT_BASE_URL = "https://partner.yourcloudlibrary.com/"

    MAX_AGE = timedelta(days=730).seconds
    CAN_REVOKE_HOLD_WHEN_RESERVED = False
    SET_DELIVERY_MECHANISM_AT = None

    SERVICE_NAME = "Bibliotheca"

    @classmethod
    def settings_class(cls) -> type[BibliothecaSettings]:
        return BibliothecaSettings

    @classmethod
    def library_settings_class(cls) -> type[BibliothecaLibrarySettings]:
        return BibliothecaLibrarySettings

    @classmethod
    def label(cls) -> str:
        return DataSourceConstants.BIBLIOTHECA

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
        # Local import to avoid circular dependency between bibliotheca.py and
        # bibliotheca_circulation_updater.py (the updater imports BibliothecaAPI).
        from palace.manager.integration.license.bibliotheca_circulation_updater import (
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


class ItemListParser(XMLProcessor[BibliographicData], LoggerMixin):
    DATE_FORMAT = "%Y-%m-%d"
    YEAR_FORMAT = "%Y"

    unescape_entity_references = html.unescape

    @property
    def xpath_expression(self) -> str:
        return "//Item"

    parenthetical = re.compile(r" \([^)]+\)$")

    format_data_for_bibliotheca_format = frozendict(
        {
            "EPUB": (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
            "EPUB3": (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
            "PDF": (Representation.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
            "MP3": (None, DeliveryMechanism.FINDAWAY_DRM),
        }
    )

    @classmethod
    def contributors_from_string(
        cls, string: str | None, role: str = Contributor.Role.AUTHOR
    ) -> list[ContributorData]:
        contributors: list[ContributorData] = []
        if not string:
            return contributors

        # Contributors may have two levels of entity reference escaping,
        # one of which will have already been handled by the initial parse.
        # We handle the potential need for a second unescaping here.
        string = cls.unescape_entity_references(string)

        for sort_name in string.split(";"):
            sort_name = cls.parenthetical.sub("", sort_name.strip())
            contributors.append(
                ContributorData(sort_name=sort_name.strip(), roles=[role])
            )
        return contributors

    @classmethod
    def parse_genre_string(self, s: str | None) -> list[SubjectData]:
        genres: list[SubjectData] = []
        if not s:
            return genres
        for i in s.split(","):
            i = i.strip()
            if not i:
                continue
            i = (
                i.replace("&amp;amp;", "&amp;")
                .replace("&amp;", "&")
                .replace("&#39;", "'")
            )
            genres.append(
                SubjectData(
                    type=Subject.BISAC,
                    identifier=None,
                    name=i,
                    weight=Classification.TRUSTED_DISTRIBUTOR_WEIGHT,
                )
            )
        return genres

    def process_one(
        self, tag: _Element, namespaces: dict[str, str] | None
    ) -> BibliographicData:
        """Turn an <item> tag into a BibliographicData and an encompassed CirculationData
        objects, and return the BibliographicData."""

        def value(bibliotheca_key: str) -> str | None:
            return self.text_of_optional_subtag(tag, bibliotheca_key)

        primary_identifier = IdentifierData(
            type=Identifier.BIBLIOTHECA_ID, identifier=value("ItemId")
        )

        identifiers = []
        for key in ("ISBN13", "PhysicalISBN"):
            v = value(key)
            if v:
                identifiers.append(IdentifierData(type=Identifier.ISBN, identifier=v))

        subjects = self.parse_genre_string(value("Genre"))

        title = value("Title")
        subtitle = value("SubTitle")
        publisher = value("Publisher")
        language = value("Language")

        authors = list(self.contributors_from_string(value("Authors")))
        narrators = list(
            self.contributors_from_string(value("Narrator"), Contributor.Role.NARRATOR)
        )

        published_date = None
        published = value("PubDate")
        if published:
            formats = [self.DATE_FORMAT, self.YEAR_FORMAT]
        else:
            published = value("PubYear")
            formats = [self.YEAR_FORMAT]

        for format in formats:
            try:
                published_date = strptime_utc(published, format)  # type: ignore[arg-type]
            except ValueError as e:
                pass

        links = []
        description = value("Description")
        if description:
            links.append(LinkData(rel=Hyperlink.DESCRIPTION, content=description))

        # Presume all images from Bibliotheca are JPEG.
        media_type = Representation.JPEG_MEDIA_TYPE
        cover_url = self.text_of_subtag(tag, "CoverLinkURL").replace("&amp;", "&")

        # Unless the URL format has drastically changed, we should be
        # able to generate a thumbnail URL based on the full-size
        # cover URL found in the response document.
        #
        # NOTE: this is an undocumented feature of the Bibliotheca API
        # which was discovered by investigating the BookLinkURL.
        if "/delivery/img" in cover_url:
            thumbnail_url = cover_url + "&size=NORMAL"
            thumbnail = LinkData(
                rel=Hyperlink.THUMBNAIL_IMAGE, href=thumbnail_url, media_type=media_type
            )
        else:
            thumbnail = None
        cover_link = LinkData(
            rel=Hyperlink.IMAGE,
            href=cover_url,
            media_type=media_type,
            thumbnail=thumbnail,
        )
        links.append(cover_link)

        alternate_url = self.text_of_subtag(tag, "BookLinkURL").replace("&amp;", "&")
        links.append(LinkData(rel="alternate", href=alternate_url))

        measurements = []
        pages = value("NumberOfPages")
        if pages:
            pages_int = int(pages)
            measurements.append(
                MeasurementData(
                    quantity_measured=Measurement.PAGE_COUNT, value=pages_int
                )
            )

        circulation, medium = self._make_circulation_data(tag, primary_identifier)

        bibliographic = BibliographicData(
            data_source_name=DataSource.BIBLIOTHECA,
            title=title,
            subtitle=subtitle,
            language=language,
            medium=medium,
            publisher=publisher,
            published=published_date,
            primary_identifier_data=primary_identifier,
            identifiers=identifiers,
            subjects=subjects,
            contributors=authors + narrators,
            measurements=measurements,
            links=links,
            circulation=circulation,
        )
        return bibliographic

    def _make_circulation_data(
        self, tag: _Element, primary_identifier: IdentifierData
    ) -> tuple[CirculationData, str]:
        """Parse out a CirculationData containing current circulation
        and formatting information.
        """

        def value(bibliotheca_key: str) -> str:
            return self.text_of_subtag(tag, bibliotheca_key)

        def intvalue(key: str) -> int:
            return self.int_of_subtag(tag, key)

        book_format = value("BookFormat")
        medium, formats = self.internal_formats(book_format)

        licenses_owned = intvalue("TotalCopies")
        try:
            licenses_available = intvalue("AvailableCopies")
        except IndexError:
            self.log.warning(
                "No information on available copies for %s",
                primary_identifier.identifier,
            )
            licenses_available = 0

        patrons_in_hold_queue = intvalue("OnHoldCount")
        licenses_reserved = 0

        license_status = (
            LicensePoolStatus.ACTIVE
            if licenses_owned > 0
            else LicensePoolStatus.EXHAUSTED
        )

        circulation = CirculationData(
            data_source_name=DataSource.BIBLIOTHECA,
            primary_identifier_data=primary_identifier,
            licenses_owned=licenses_owned,
            licenses_available=licenses_available,
            licenses_reserved=licenses_reserved,
            patrons_in_hold_queue=patrons_in_hold_queue,
            formats=formats,
            status=license_status,
        )
        return circulation, medium

    @classmethod
    def internal_formats(cls, book_format: str) -> tuple[str, list[FormatData]]:
        """Convert the term Bibliotheca uses to refer to a book
        format into a (medium [formats]) 2-tuple.
        """
        if book_format not in cls.format_data_for_bibliotheca_format:
            cls.logger().error("Unrecognized BookFormat: %s", book_format)
            return Edition.BOOK_MEDIUM, []

        content_type, drm_scheme = cls.format_data_for_bibliotheca_format[book_format]

        format = FormatData(content_type=content_type, drm_scheme=drm_scheme)
        if book_format == "MP3":
            medium = Edition.AUDIO_MEDIUM
        else:
            medium = Edition.BOOK_MEDIUM
        return medium, [format]


class BibliothecaParser[T](XMLProcessor[T], ABC):
    INPUT_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

    @classmethod
    def parse_date(cls, value: str | None) -> datetime | None:
        """Parse the string Bibliotheca sends as a date.

        Usually this is a string in INPUT_TIME_FORMAT, but it might be None.
        """
        if not value:
            parsed = None
        else:
            try:
                parsed = strptime_utc(value, cls.INPUT_TIME_FORMAT)
            except ValueError as e:
                logging.error(
                    'Unable to parse Bibliotheca date: "%s"', value, exc_info=e
                )
                parsed = None
        return to_utc(parsed)

    @overload
    def date_from_subtag(self, tag: _Element, key: str) -> datetime: ...

    @overload
    def date_from_subtag(
        self, tag: _Element, key: str, required: Literal[False]
    ) -> datetime | None: ...

    def date_from_subtag(
        self, tag: _Element, key: str, required: bool = True
    ) -> datetime | None:
        value = (
            self.text_of_subtag(tag, key)
            if required
            else self.text_of_optional_subtag(tag, key)
        )
        return self.parse_date(value)


class ErrorParser(BibliothecaParser[BaseProblemDetailException]):
    """Turn an error document from the Bibliotheca web service into a CheckoutException"""

    wrong_status = re.compile(
        "the patron document status was ([^ ]+) and not one of ([^ ]+)"
    )

    loan_limit_reached = re.compile("Patron cannot loan more than [0-9]+ document")

    hold_limit_reached = re.compile("Patron cannot have more than [0-9]+ hold")

    error_mapping = {
        "The patron does not have the book on hold": NotOnHold,
        "The patron has no eBooks checked out": NotCheckedOut,
    }

    @property
    def xpath_expression(self) -> str:
        return "//Error"

    def process_first(self, string: str | bytes) -> BaseProblemDetailException:
        try:
            return_val = super().process_first(string)
        except Exception as e:
            # The server sent us an error with an incorrect or
            # nonstandard syntax.
            if isinstance(string, bytes):
                try:
                    debug = string.decode("utf-8")
                except UnicodeDecodeError:
                    debug = "Unreadable error message (Unicode decode error)."
            else:
                debug = string
            return RemoteInitiatedServerError(debug, BibliothecaAPI.SERVICE_NAME)

        if return_val is None:
            # We were not able to interpret the result as an error.
            # The most likely cause is that the Bibliotheca app server is down.
            return RemoteInitiatedServerError(
                "Unknown error",
                BibliothecaAPI.SERVICE_NAME,
            )

        return return_val

    def process_one(
        self, error_tag: _Element, namespaces: dict[str, str] | None
    ) -> BaseProblemDetailException:
        message = self.text_of_optional_subtag(error_tag, "Message")
        if not message:
            return RemoteInitiatedServerError(
                "Unknown error",
                BibliothecaAPI.SERVICE_NAME,
            )

        if message in self.error_mapping:
            return self.error_mapping[message](message)
        if message in ("Authentication failed", "Unknown error"):
            # 'Unknown error' is an unknown error on the Bibliotheca side.
            #
            # 'Authentication failed' could _in theory_ be an error on
            # our side, but if authentication is set up improperly we
            # actually get a 401 and no body. When we get a real error
            # document with 'Authentication failed', it's always a
            # transient error on the Bibliotheca side. Possibly some
            # authentication internal to Bibliotheca has failed? Anyway, it
            # happens relatively frequently.
            return RemoteInitiatedServerError(message, BibliothecaAPI.SERVICE_NAME)

        m = self.loan_limit_reached.search(message)
        if m:
            return PatronLoanLimitReached(message)

        m = self.hold_limit_reached.search(message)
        if m:
            return PatronHoldLimitReached(message)

        m = self.wrong_status.search(message)
        if not m:
            return RemoteInitiatedServerError(message, BibliothecaAPI.SERVICE_NAME)
        actual, expected = m.groups()
        expected = expected.split(",")

        if actual == "CAN_WISH":
            return NoLicenses(debug_info=message)

        if "CAN_LOAN" in expected and actual == "CAN_HOLD":
            return NoAvailableCopies(debug_info=message)

        if "CAN_LOAN" in expected and actual == "HOLD":
            return AlreadyOnHold(debug_info=message)

        if "CAN_LOAN" in expected and actual == "LOAN":
            return AlreadyCheckedOut(debug_info=message)

        if "CAN_HOLD" in expected and actual == "CAN_LOAN":
            return CurrentlyAvailable(debug_info=message)

        if "CAN_HOLD" in expected and actual == "HOLD":
            return AlreadyOnHold(debug_info=message)

        if "CAN_HOLD" in expected:
            return CannotHold(debug_info=message)

        if "CAN_LOAN" in expected:
            return CannotLoan(debug_info=message)

        return RemoteInitiatedServerError(message, BibliothecaAPI.SERVICE_NAME)


class PatronCirculationParser(XMLParser):
    """Parse Bibliotheca's patron circulation status document into a list of
    LoanInfo and HoldInfo objects.
    """

    id_type = Identifier.BIBLIOTHECA_ID

    def __init__(self, collection: Collection) -> None:
        self.collection = collection

    def process_all(self, string: bytes | str) -> itertools.chain[LoanInfo | HoldInfo]:
        xml = self._load_xml(string)
        loans = self._process_all(
            xml, "//Checkouts/Item", namespaces={}, handler=self.process_one_loan
        )
        holds = self._process_all(
            xml, "//Holds/Item", namespaces={}, handler=self.process_one_hold
        )
        reserves = self._process_all(
            xml, "//Reserves/Item", namespaces={}, handler=self.process_one_reserve
        )
        return itertools.chain(loans, holds, reserves)

    def process_one_loan(
        self, tag: _Element, namespaces: dict[str, str]
    ) -> LoanInfo | None:
        return self.process_one(tag, namespaces, LoanInfo)

    def process_one_hold(
        self, tag: _Element, namespaces: dict[str, str]
    ) -> HoldInfo | None:
        return self.process_one(tag, namespaces, HoldInfo)

    def process_one_reserve(
        self, tag: _Element, namespaces: dict[str, str]
    ) -> HoldInfo | None:
        hold_info = self.process_one(tag, namespaces, HoldInfo)
        if hold_info is not None:
            hold_info.hold_position = 0
        return hold_info

    def process_one[T](
        self, tag: _Element, namespaces: dict[str, str], source_class: type[T]
    ) -> T | None:
        if not tag.xpath("ItemId"):
            # This happens for events associated with books
            # no longer in our collection.
            return None

        def datevalue(key: str) -> datetime:
            value = self.text_of_subtag(tag, key)
            return strptime_utc(value, BibliothecaAPI.ARGUMENT_TIME_FORMAT)

        identifier = self.text_of_subtag(tag, "ItemId")
        start_date = datevalue("EventStartDateInUTC")
        end_date = datevalue("EventEndDateInUTC")
        kwargs = {
            "collection_id": self.collection.id,
            "identifier_type": self.id_type,
            "identifier": identifier,
            "start_date": start_date,
            "end_date": end_date,
        }
        if source_class is HoldInfo:
            kwargs["hold_position"] = self.int_of_subtag(tag, "Position")
        return source_class(**kwargs)


class DateResponseParser(BibliothecaParser[Optional[datetime]], ABC):
    """Extract a date from a response."""

    RESULT_TAG_NAME: str | None = None
    DATE_TAG_NAME: str | None = None

    @property
    def xpath_expression(self) -> str:
        return f"/{self.RESULT_TAG_NAME}/{self.DATE_TAG_NAME}"

    def process_one(
        self, tag: _Element, namespaces: dict[str, str] | None
    ) -> datetime | None:
        due_date = tag.text
        if not due_date:
            return None
        return strptime_utc(due_date, EventParser.INPUT_TIME_FORMAT)


class CheckoutResponseParser(DateResponseParser):
    """Extract due date from a checkout response."""

    @property
    def xpath_expression(self) -> str:
        return f"/CheckoutResult/DueDateInUTC"


class HoldResponseParser(DateResponseParser):
    """Extract availability date from a hold response."""

    @property
    def xpath_expression(self) -> str:
        return f"/PlaceHoldResult/AvailabilityDateInUTC"


class EventParser(
    BibliothecaParser[tuple[str, str, str | None, datetime, datetime | None, str]]
):
    """Parse Bibliotheca's event file format into our native event objects."""

    EVENT_SOURCE = "Bibliotheca"

    SET_DELIVERY_MECHANISM_AT = BaseCirculationAPI.BORROW_STEP

    # Map Bibliotheca's event names to our names.
    EVENT_NAMES = {
        "CHECKOUT": CirculationEvent.DISTRIBUTOR_CHECKOUT,
        "CHECKIN": CirculationEvent.DISTRIBUTOR_CHECKIN,
        "HOLD": CirculationEvent.DISTRIBUTOR_HOLD_PLACE,
        "RESERVED": CirculationEvent.DISTRIBUTOR_AVAILABILITY_NOTIFY,
        "PURCHASE": CirculationEvent.DISTRIBUTOR_LICENSE_ADD,
        "REMOVED": CirculationEvent.DISTRIBUTOR_LICENSE_REMOVE,
    }

    @property
    def xpath_expression(self) -> str:
        return "//CloudLibraryEvent"

    def process_all(
        self, string: bytes | str, no_events_error: bool = False
    ) -> Generator[tuple[str, str, str | None, datetime, datetime | None, str]]:
        has_events = False
        # Bibliotheca occasionally returns an empty response body. An empty
        # document cannot be parsed as XML (lxml raises "Document is empty"
        # even in recovery mode), so treat a blank body the same as a
        # response that contained no events rather than letting the parse
        # error propagate as an unhandled exception.
        if string.strip():
            for i in super().process_all(string):
                yield i
                has_events = True

        # If we are catching up on events and we expect to have a time
        # period where there are no events, we don't want to consider that
        # action as an error. By default, not having events is not
        # considered to be an error.
        if not has_events and no_events_error:
            # An empty list of events may mean nothing happened, or it
            # may indicate an unreported server-side error. To be
            # safe, we'll treat this as a server-initiated error
            # condition. If this is just a slow day, normal behavior
            # will resume as soon as something happens.
            raise RemoteInitiatedServerError(
                "No events returned from server. This may not be an error, but treating it as one to be safe.",
                BibliothecaAPI.SERVICE_NAME,
            )

    def process_one(
        self, tag: _Element, namespaces: dict[str, str] | None
    ) -> tuple[str, str, str | None, datetime, datetime | None, str]:
        isbn = self.text_of_subtag(tag, "ISBN")
        bibliotheca_id = self.text_of_subtag(tag, "ItemId")
        patron_id = self.text_of_optional_subtag(tag, "PatronId")

        start_time = self.date_from_subtag(tag, "EventStartDateTimeInUTC")
        end_time = self.date_from_subtag(tag, "EventEndDateTimeInUTC", required=False)

        bibliotheca_event_type = self.text_of_subtag(tag, "EventType")
        internal_event_type = self.EVENT_NAMES[bibliotheca_event_type]

        return (
            bibliotheca_id,
            isbn,
            patron_id,
            start_time,
            end_time,
            internal_event_type,
        )
