from __future__ import annotations

import base64
import datetime
import json
import urllib
from collections.abc import Generator, Mapping, Sequence
from datetime import timedelta
from typing import Any

from lxml import etree
from requests import Response as RequestsResponse
from sqlalchemy.orm import Session

from palace.manager.api.axis.constants import Axis360APIConstants
from palace.manager.api.axis.loan_info import AxisLoanInfo
from palace.manager.api.axis.parser import (
    AvailabilityResponseParser,
    BibliographicParser,
    CheckinResponseParser,
    CheckoutResponseParser,
    HoldReleaseResponseParser,
    HoldResponseParser,
    StatusResponseParser,
)
from palace.manager.api.axis.settings import Axis360LibrarySettings, Axis360Settings
from palace.manager.api.circulation import (
    BaseCirculationAPI,
    CirculationInternalFormatsMixin,
    Fulfillment,
    HoldInfo,
    LoanInfo,
    PatronActivityCirculationAPI,
)
from palace.manager.api.circulation_exceptions import (
    CannotFulfill,
    NoActiveLoan,
    NotOnHold,
    RemoteInitiatedServerError,
)
from palace.manager.api.selftest import HasCollectionSelfTests
from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.core.selftest import SelfTestResult
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.policy.replacement import ReplacementPolicy
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.sqlalchemy.model.resource import Representation
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.http import HTTP


class Axis360API(
    PatronActivityCirculationAPI[Axis360Settings, Axis360LibrarySettings],
    HasCollectionSelfTests,
    CirculationInternalFormatsMixin,
    Axis360APIConstants,
):
    SET_DELIVERY_MECHANISM_AT = BaseCirculationAPI.BORROW_STEP

    DATE_FORMAT = "%m-%d-%Y %H:%M:%S"

    access_token_endpoint = "accesstoken"
    availability_endpoint = "availability/v2"
    fulfillment_endpoint = "getfullfillmentInfo/v2"
    audiobook_metadata_endpoint = "getaudiobookmetadata/v2"

    # Create a lookup table between common DeliveryMechanism identifiers
    # and Axis 360 format types.
    epub = Representation.EPUB_MEDIA_TYPE
    pdf = Representation.PDF_MEDIA_TYPE
    adobe_drm = DeliveryMechanism.ADOBE_DRM
    findaway_drm = DeliveryMechanism.FINDAWAY_DRM
    no_drm = DeliveryMechanism.NO_DRM
    axisnow_drm = DeliveryMechanism.AXISNOW_DRM

    delivery_mechanism_to_internal_format = {
        (epub, no_drm): "ePub",
        (epub, adobe_drm): "ePub",
        (pdf, no_drm): "PDF",
        (pdf, adobe_drm): "PDF",
        (None, findaway_drm): "Acoustik",
        (None, axisnow_drm): Axis360APIConstants.AXISNOW,
    }

    @classmethod
    def settings_class(cls) -> type[Axis360Settings]:
        return Axis360Settings

    @classmethod
    def library_settings_class(cls) -> type[Axis360LibrarySettings]:
        return Axis360LibrarySettings

    @classmethod
    def label(cls) -> str:
        return "Axis 360"

    @classmethod
    def description(cls) -> str:
        return ""

    def __init__(
        self, _db: Session, collection: Collection, bearer_token: str | None = None
    ) -> None:
        super().__init__(_db, collection)
        settings = self.settings
        self.library_id = settings.external_account_id
        self.username = settings.username
        self.password = settings.password

        # Convert the nickname for a server into an actual URL.
        base_url = settings.url or self.PRODUCTION_BASE_URL
        if base_url in self.SERVER_NICKNAMES:
            base_url = self.SERVER_NICKNAMES[base_url]
        if not base_url.endswith("/"):
            base_url += "/"
        self.base_url = base_url

        if not self.library_id or not self.username or not self.password:
            raise CannotLoadConfiguration("Axis 360 configuration is incomplete.")

        self._cached_bearer_token: str | None = bearer_token
        self.verify_certificate: bool = (
            settings.verify_certificate
            if settings.verify_certificate is not None
            else True
        )

    @property
    def source(self) -> DataSource:
        return DataSource.lookup(self._db, DataSource.AXIS_360, autocreate=True)

    @property
    def authorization_headers(self) -> dict[str, str]:
        authorization = ":".join([self.username, self.password, self.library_id])
        authorization_encoded = authorization.encode("utf_16_le")
        authorization_b64 = base64.standard_b64encode(authorization_encoded).decode(
            "utf-8"
        )
        return dict(Authorization="Basic " + authorization_b64)

    def _run_self_tests(self, _db: Session) -> Generator[SelfTestResult]:
        result = self.run_test("Refreshing bearer token", self._refresh_bearer_token)
        yield result
        if not result.success:
            # If we can't get a bearer token, there's no point running
            # the rest of the tests.
            return

        def _count_events() -> str:
            now = utc_now()
            five_minutes_ago = now - timedelta(minutes=5)
            count = len(list(self.recent_activity(since=five_minutes_ago)))
            return "Found %d event(s)" % count

        yield self.run_test(
            "Asking for circulation events for the last five minutes", _count_events
        )

        if self.collection is None:
            raise ValueError("Collection is None")

        for library_result in self.default_patrons(self.collection):
            if isinstance(library_result, SelfTestResult):
                yield library_result
                continue
            library, patron, pin = library_result

            def _count_activity() -> str:
                result = self.patron_activity(patron, pin)
                return "Found %d loans/holds" % len(result)

            yield self.run_test(
                "Checking activity for test patron for library %s" % library.name,
                _count_activity,
            )

        # Run the tests defined by HasCollectionSelfTests
        for result in super()._run_self_tests(_db):
            yield result

    def _refresh_bearer_token(self) -> str:
        url = self.base_url + self.access_token_endpoint
        headers = self.authorization_headers
        response = self._make_request(
            url, "post", headers, allowed_response_codes=[200]
        )
        return self.parse_token(response.content)

    def bearer_token(self) -> str:
        if not self._cached_bearer_token:
            self._cached_bearer_token = self._refresh_bearer_token()
        return self._cached_bearer_token

    def request(
        self,
        url: str,
        method: str = "get",
        extra_headers: dict[str, str] | None = None,
        data: Mapping[str, Any] | None = None,
        params: Mapping[str, Any] | None = None,
        request_retried: bool = False,
        **kwargs: Any,
    ) -> RequestsResponse:
        """Make an HTTP request, acquiring/refreshing a bearer token
        if necessary.
        """
        if not extra_headers:
            extra_headers = {}
        headers = dict(extra_headers)
        headers["Authorization"] = "Bearer " + self.bearer_token()
        headers["Library"] = self.library_id
        response = self._make_request(
            url=url,
            method=method,
            headers=headers,
            data=data,
            params=params,
            **kwargs,
        )
        if response.status_code == 401 and not request_retried:
            parsed = StatusResponseParser().process_first(response.content)
            if parsed is None or parsed[0] in [1001, 1002]:
                # The token is probably expired. Get a new token and try again.
                # Axis 360's status codes mean:
                #   1001: Invalid token
                #   1002: Token expired
                self._cached_bearer_token = None
                return self.request(
                    url=url,
                    method=method,
                    extra_headers=extra_headers,
                    data=data,
                    params=params,
                    request_retried=True,
                    **kwargs,
                )

        return response

    def availability(
        self,
        patron_id: str | None = None,
        since: datetime.datetime | None = None,
        title_ids: list[str] | None = None,
    ) -> RequestsResponse:
        url = self.base_url + self.availability_endpoint
        args = dict()
        if since:
            since_str = since.strftime(self.DATE_FORMAT)
            args["updatedDate"] = since_str
        if patron_id:
            args["patronId"] = patron_id
        if title_ids:
            args["titleIds"] = ",".join(title_ids)
        response = self.request(url, params=args, timeout=None)
        return response

    def get_fulfillment_info(self, transaction_id: str) -> RequestsResponse:
        """Make a call to the getFulfillmentInfoAPI."""
        url = self.base_url + self.fulfillment_endpoint
        params = dict(TransactionID=transaction_id)
        # We set an explicit timeout because this request can take a long time and
        # the default was too short. Ideally B&T would fix this on their end, but
        # in the meantime we need to work around it.
        # TODO: Revisit this timeout. Hopefully B&T will fix the performance
        #   of this endpoint and we can remove this. We should be able to query
        #   our logs to see how long these requests are taking.
        return self.request(url, "POST", params=params, timeout=15)

    def get_audiobook_metadata(self, findaway_content_id: str) -> RequestsResponse:
        """Make a call to the getaudiobookmetadata endpoint."""
        base_url = self.base_url
        url = base_url + self.audiobook_metadata_endpoint
        params = dict(fndcontentid=findaway_content_id)
        # We set an explicit timeout because this request can take a long time and
        # the default was too short. Ideally B&T would fix this on their end, but
        # in the meantime we need to work around it.
        # TODO: Revisit this timeout. Hopefully B&T will fix the performance
        #   of this endpoint and we can remove this. We should be able to query
        #   our logs to see how long these requests are taking.
        response = self.request(url, "POST", params=params, timeout=15)
        return response

    def checkin(self, patron: Patron, pin: str, licensepool: LicensePool) -> None:
        """Return a book early.

        :param patron: The Patron who wants to return their book.
        :param pin: Not used.
        :param licensepool: LicensePool for the book to be returned.
        :raise CirculationException: If the API can't carry out the operation.
        :raise RemoteInitiatedServerError: If the API is down.
        """
        title_id = licensepool.identifier.identifier
        patron_id = patron.authorization_identifier
        response = self._checkin(title_id, patron_id)
        try:
            CheckinResponseParser().process_first(response.content)
        except etree.XMLSyntaxError:
            raise RemoteInitiatedServerError(response.text, self.label())

    def _checkin(self, title_id: str | None, patron_id: str | None) -> RequestsResponse:
        """Make a request to the EarlyCheckInTitle endpoint."""
        if title_id is None:
            self.log.warning(
                f"Calling _checkin with title_id None. This is likely a bug. Patron_id: {patron_id}."
            )
            title_id = ""

        if patron_id is None:
            self.log.warning(
                f"Calling _checkin with patron_id None. This is likely a bug. Title_id: {title_id}."
            )
            patron_id = ""

        url = self.base_url + "EarlyCheckInTitle/v3?itemID={}&patronID={}".format(
            urllib.parse.quote(title_id),
            urllib.parse.quote(patron_id),
        )
        return self.request(url, method="GET", verbose=True)

    def checkout(
        self,
        patron: Patron,
        pin: str | None,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism | None,
    ) -> LoanInfo:
        # Because we have SET_DELIVERY_MECHANISM_AT set to BORROW_STEP,
        # delivery mechanism should always be set, but mypy doesn't know
        # that, so we assert it here for type safety.
        assert delivery_mechanism is not None

        title_id = licensepool.identifier.identifier
        patron_id = patron.authorization_identifier
        response = self._checkout(
            title_id, patron_id, self.internal_format(delivery_mechanism)
        )

        try:
            response_text = response.text
            self.log.info(
                f"patron_id={patron_id} tried to checkout title_id={title_id}: "
                f"response_code = {response.status_code}, response_content={response_text}"
            )
            expiration_date = CheckoutResponseParser().process_first(response_text)
            return LoanInfo.from_license_pool(licensepool, end_date=expiration_date)
        except etree.XMLSyntaxError:
            raise RemoteInitiatedServerError(response.text, self.label())

    def _checkout(
        self, title_id: str | None, patron_id: str | None, internal_format: str
    ) -> RequestsResponse:
        url = self.base_url + "checkout/v2"
        args = dict(titleId=title_id, patronId=patron_id, format=internal_format)
        self.log.info(
            f"patron_id={patron_id} about to checkout title_id={title_id}, using format={internal_format} "
            f"posting to url={url}"
        )
        response = self.request(url, data=args, method="POST")

        return response

    def fulfill(
        self,
        patron: Patron,
        pin: str,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism,
    ) -> Fulfillment:
        """Fulfill a patron's request for a specific book."""
        identifier = licensepool.identifier
        # This should include only one 'activity'.
        internal_format = self.internal_format(delivery_mechanism)
        log_messages: list[str] = [
            f"arguments for patron_activity method: "
            f"patron.id={patron.id},"
            f"internal_format={internal_format}, "
            f"licensepool.identifier={identifier}, "
            f"patron_id={patron.id}"
        ]
        activities = self.patron_activity(
            patron, pin, licensepool.identifier, internal_format, log_messages
        )

        log_messages.append(
            f"Patron activities returned from patron_activity method: {activities}"
        )

        for loan in activities:
            if not isinstance(loan, AxisLoanInfo):
                continue
            if not (
                loan.identifier_type == identifier.type
                and loan.identifier == identifier.identifier
            ):
                continue
            # We've found the remote loan corresponding to this
            # license pool.
            fulfillment = loan.fulfillment
            if not fulfillment or not isinstance(fulfillment, Fulfillment):
                raise CannotFulfill()
            return fulfillment
        # If we made it to this point, the patron does not have this
        # book checked out.
        log_messages.insert(
            0,
            "Unable to fulfill because there is no active loan. See info statements below for details:",
        )
        self.log.error("\n  ".join(log_messages))
        raise NoActiveLoan()

    def place_hold(
        self,
        patron: Patron,
        pin: str | None,
        licensepool: LicensePool,
        hold_notification_email: str | None,
    ) -> HoldInfo:
        if not hold_notification_email:
            hold_notification_email = self.default_notification_email_address(
                patron, pin
            )

        url = self.base_url + "addtoHold/v2"
        identifier = licensepool.identifier
        title_id = identifier.identifier
        patron_id = patron.authorization_identifier
        params = dict(
            titleId=title_id, patronId=patron_id, email=hold_notification_email
        )
        response = self.request(url, params=params)
        hold_position = HoldResponseParser().process_first(response.content)
        hold_info = HoldInfo.from_license_pool(
            licensepool,
            start_date=utc_now(),
            hold_position=hold_position,
        )
        return hold_info

    def release_hold(self, patron: Patron, pin: str, licensepool: LicensePool) -> None:
        url = self.base_url + "removeHold/v2"
        identifier = licensepool.identifier
        title_id = identifier.identifier
        patron_id = patron.authorization_identifier
        params = dict(titleId=title_id, patronId=patron_id)
        response = self.request(url, params=params)
        try:
            HoldReleaseResponseParser().process_first(response.content)
        except NotOnHold:
            # Fine, it wasn't on hold and now it's still not on hold.
            pass
        # If we didn't raise an exception, we're fine.
        return None

    def patron_activity(
        self,
        patron: Patron,
        pin: str | None,
        identifier: Identifier | None = None,
        internal_format: str | None = None,
        log_messages: list[str] | None = None,
    ) -> list[AxisLoanInfo | HoldInfo]:
        if identifier:
            assert identifier.identifier is not None
            title_ids = [identifier.identifier]
        else:
            title_ids = None

        availability = self.availability(
            patron_id=patron.authorization_identifier, title_ids=title_ids
        )

        availability_content_str = availability.text
        if log_messages:
            log_messages.append(
                f"arguments to availability call: title_ids={title_ids}"
            )
            log_messages.append(
                f"response to availability call: status={availability.status_code}, content={availability_content_str}"
            )
        loan_info_list = list(
            AvailabilityResponseParser(self, internal_format).process_all(
                availability_content_str
            )
        )

        return loan_info_list

    def update_availability(self, licensepool: LicensePool) -> None:
        """Update the availability information for a single LicensePool.

        Part of the CirculationAPI interface.
        """
        self.update_licensepools_for_identifiers([licensepool.identifier])

    def update_licensepools_for_identifiers(
        self, identifiers: list[Identifier]
    ) -> None:
        """Update availability and bibliographic information for
        a list of books.

        If the book has never been seen before, a new LicensePool
        will be created for the book.

        The book's LicensePool will be updated with current
        circulation information.
        """
        remainder = set(identifiers)
        for bibliographic, availability in self._fetch_remote_availability(identifiers):
            edition, ignore1, license_pool, ignore2 = self.update_book(bibliographic)
            identifier = license_pool.identifier
            if identifier in remainder:
                remainder.remove(identifier)

        # We asked Axis about n books. It sent us n-k responses. Those
        # k books are the identifiers in `remainder`. These books have
        # been removed from the collection without us being notified.
        for removed_identifier in remainder:
            self._reap(removed_identifier)

    def update_book(
        self,
        bibliographic: BibliographicData,
    ) -> tuple[Edition, bool, LicensePool, bool]:
        """Create or update a single book based on bibliographic
        and availability data from the Axis 360 API.

        :param bibliographic: A BibliographicData object containing
            bibliographic and circulation (ie availability) data about this title
        """
        # The axis the bibliographic metadata always includes the circulation data
        assert bibliographic.circulation

        license_pool, new_license_pool = bibliographic.circulation.license_pool(
            self._db, self.collection
        )

        edition, new_edition = bibliographic.edition(self._db)
        license_pool.presentation_edition = edition
        policy = ReplacementPolicy(
            identifiers=False,
            subjects=True,
            contributions=True,
            formats=True,
            links=True,
        )

        bibliographic.apply(self._db, edition, self.collection, replace=policy)
        return edition, new_edition, license_pool, new_license_pool

    def _fetch_remote_availability(
        self, identifiers: list[Identifier]
    ) -> Generator[tuple[BibliographicData, CirculationData]]:
        """Retrieve availability information for the specified identifiers.

        :yield: A stream of (BibliographicData, CirculationData) 2-tuples.
        """
        identifier_strings = self.create_identifier_strings(identifiers)
        return self.availability_by_title_ids(title_ids=identifier_strings)

    def _reap(self, identifier: Identifier) -> None:
        """Update our local circulation information to reflect the fact that
        the identified book has been removed from the remote
        collection.
        """
        collection = self.collection
        pool = identifier.licensed_through_collection(collection)
        if not pool:
            self.log.warning(
                "Was about to reap %r but no local license pool in this collection.",
                identifier,
            )
            return
        if pool.licenses_owned == 0:
            # Already reaped.
            return
        self.log.info("Reaping %r", identifier)

        availability = CirculationData(
            data_source_name=pool.data_source.name,
            primary_identifier_data=IdentifierData.from_identifier(identifier),
            licenses_owned=0,
            licenses_available=0,
            licenses_reserved=0,
            patrons_in_hold_queue=0,
        )
        availability.apply(
            self._db, collection, ReplacementPolicy.from_license_source(self._db)
        )

    def recent_activity(
        self, since: datetime.datetime
    ) -> Generator[tuple[BibliographicData, CirculationData]]:
        """Find books that have had recent activity.

        :yield: A sequence of (BibliographicData, CirculationData) 2-tuples
        """
        availability = self.availability(since=since)
        content = availability.content
        yield from BibliographicParser().process_all(content)

    def availability_by_title_ids(
        self,
        title_ids: list[str],
    ) -> Generator[tuple[BibliographicData, CirculationData]]:
        """Find title availability for a list of titles
        :yield: A sequence of (BibliographicData, CirculationData) 2-tuples
        """
        availability = self.availability(title_ids=title_ids)
        content = availability.content
        yield from BibliographicParser().process_all(content)

    @classmethod
    def create_identifier_strings(
        cls, identifiers: Sequence[Identifier | str]
    ) -> list[str]:
        identifier_strings = []
        for i in identifiers:
            if isinstance(i, Identifier):
                assert i.identifier is not None
                value = i.identifier
            else:
                value = i
            identifier_strings.append(value)

        return identifier_strings

    @classmethod
    def parse_token(cls, token: bytes) -> str:
        data = json.loads(token)
        return data["access_token"]  # type: ignore[no-any-return]

    def _make_request(
        self,
        url: str,
        method: str,
        headers: Mapping[str, str],
        data: Mapping[str, Any] | None = None,
        params: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> RequestsResponse:
        """Actually make an HTTP request."""
        return HTTP.request_with_timeout(
            method, url, headers=headers, data=data, params=params, **kwargs
        )
