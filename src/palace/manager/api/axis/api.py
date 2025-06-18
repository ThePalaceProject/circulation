from __future__ import annotations

import datetime
import html
from collections.abc import Generator, Sequence
from datetime import timedelta

from sqlalchemy.orm import Session

from palace.manager.api.axis.constants import Axis360Format
from palace.manager.api.axis.fulfillment import (
    Axis360AcsFulfillment,
)
from palace.manager.api.axis.manifest import AxisNowManifest
from palace.manager.api.axis.models.json import (
    AxisNowFulfillmentInfoResponse,
    FindawayFulfillmentInfoResponse,
)
from palace.manager.api.axis.models.xml import Title
from palace.manager.api.axis.parser import BibliographicParser
from palace.manager.api.axis.requests import Axis360Requests
from palace.manager.api.axis.settings import Axis360LibrarySettings, Axis360Settings
from palace.manager.api.circulation import (
    BaseCirculationAPI,
    CirculationInternalFormatsMixin,
    DirectFulfillment,
    Fulfillment,
    HoldInfo,
    LoanInfo,
    PatronActivityCirculationAPI,
)
from palace.manager.api.circulation_exceptions import (
    CannotFulfill,
    FormatNotAvailable,
    NoActiveLoan,
    RemoteInitiatedServerError,
)
from palace.manager.api.selftest import HasCollectionSelfTests
from palace.manager.api.web_publication_manifest import FindawayManifest, SpineItem
from palace.manager.core.selftest import SelfTestResult
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.policy.replacement import ReplacementPolicy
from palace.manager.sqlalchemy.model.collection import Collection
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


class Axis360API(
    PatronActivityCirculationAPI[Axis360Settings, Axis360LibrarySettings],
    HasCollectionSelfTests,
    CirculationInternalFormatsMixin,
):
    SET_DELIVERY_MECHANISM_AT = BaseCirculationAPI.BORROW_STEP

    # Create a lookup table between common DeliveryMechanism identifiers
    # and Axis 360 format types.
    epub = Representation.EPUB_MEDIA_TYPE
    pdf = Representation.PDF_MEDIA_TYPE
    adobe_drm = DeliveryMechanism.ADOBE_DRM
    findaway_drm = DeliveryMechanism.FINDAWAY_DRM
    no_drm = DeliveryMechanism.NO_DRM
    axisnow_drm = DeliveryMechanism.AXISNOW_DRM

    delivery_mechanism_to_internal_format = {
        (epub, no_drm): Axis360Format.epub,
        (epub, adobe_drm): Axis360Format.epub,
        (pdf, no_drm): Axis360Format.pdf,
        (pdf, adobe_drm): Axis360Format.pdf,
        (None, findaway_drm): Axis360Format.acoustik,
        (None, axisnow_drm): Axis360Format.axis_now,
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
        self,
        _db: Session,
        collection: Collection,
        requests: Axis360Requests | None = None,
    ) -> None:
        super().__init__(_db, collection)
        self.api_requests = (
            Axis360Requests(self.settings) if requests is None else requests
        )

    def _run_self_tests(self, _db: Session) -> Generator[SelfTestResult]:
        def _refresh() -> str:
            return self.api_requests.refresh_bearer_token().access_token

        result = self.run_test("Refreshing bearer token", _refresh)
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
                result = list(self.patron_activity(patron, pin))
                return "Found %d loans/holds" % len(result)

            yield self.run_test(
                "Checking activity for test patron for library %s" % library.name,
                _count_activity,
            )

        # Run the tests defined by HasCollectionSelfTests
        for result in super()._run_self_tests(_db):
            yield result

    def checkin(self, patron: Patron, pin: str, licensepool: LicensePool) -> None:
        """Return a book early.

        :param patron: The Patron who wants to return their book.
        :param pin: Not used.
        :param licensepool: LicensePool for the book to be returned.

        :raise CirculationException: If the API can't carry out the operation.
        :raise Axis360ValidationError: If the API returns an invalid response.
        """
        title_id = licensepool.identifier.identifier
        patron_id = patron.authorization_identifier
        self.api_requests.early_checkin(title_id, patron_id)

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
        response = self.api_requests.checkout(
            title_id, patron_id, self.internal_format(delivery_mechanism)
        )
        return LoanInfo.from_license_pool(
            licensepool, end_date=response.expiration_date
        )

    def _fulfill_acs(self, title: Title) -> Axis360AcsFulfillment:
        # The patron wants a direct link to the book, which we can deliver
        # immediately, without making any more API requests.
        download_url = title.availability.download_url
        identifier = title.title_id
        if download_url is None:
            # If there's no download URL, we can't fulfill the request.
            self.log.error(
                "No download URL found for identifier %s. %r",
                identifier,
                title,
            )
            raise CannotFulfill()
        return Axis360AcsFulfillment(
            content_link=html.unescape(download_url),
            content_type=DeliveryMechanism.ADOBE_DRM,
            verify=self.api_requests._verify_certificate,
        )

    def _fulfill_acoustik(
        self,
        title: Title,
        fulfillment_info: FindawayFulfillmentInfoResponse,
        licensepool: LicensePool,
    ) -> DirectFulfillment:
        session_key = fulfillment_info.session_key
        if session_key == "Expired":
            message = (
                f"Expired findaway session key for {title.title_id}. "
                f"Title: {title!r}. Fulfillment: {fulfillment_info!r}"
            )
            self.log.error(message)
            raise RemoteInitiatedServerError(
                message,
                self.label(),
            )

        metadata_response = self.api_requests.audiobook_metadata(
            fulfillment_info.content_id
        )
        fnd_manifest = FindawayManifest(
            licensepool,
            accountId=metadata_response.account_id,
            checkoutId=fulfillment_info.transaction_id,
            fulfillmentId=fulfillment_info.content_id,
            licenseId=fulfillment_info.license_id,
            sessionKey=session_key,
            spine_items=[
                SpineItem(item.title, item.duration, item.part, item.sequence)
                for item in metadata_response.reading_order
            ],
        )
        return DirectFulfillment(str(fnd_manifest), fnd_manifest.MEDIA_TYPE)

    def _fulfill_axisnow(
        self, title: Title, fulfillment_info: AxisNowFulfillmentInfoResponse
    ) -> DirectFulfillment:
        axis_manifest = AxisNowManifest(
            fulfillment_info.book_vault_uuid,
            fulfillment_info.isbn,
        )
        return DirectFulfillment(str(axis_manifest), axis_manifest.MEDIA_TYPE)

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

        availability_response = self.api_requests.availability(
            patron_id=patron.authorization_identifier,
            title_ids=[identifier.identifier],
        )

        titles = [
            title
            for title in availability_response.titles
            if title.title_id == identifier.identifier
            and title.availability.is_checked_out
        ]

        if not titles:
            # The Axis 360 API did not return any titles for this identifier, so
            # the patron does not have this book checked out.
            if availability_response.titles:
                # If there are titles but none match, we log a warning.
                self.log.warning(
                    "No active loan found for identifier %s. Titles returned: %r",
                    identifier.identifier,
                    availability_response.titles,
                )
            raise NoActiveLoan()

        title = titles.pop()

        if titles:
            # If there are multiple titles, we log a warning and use the first one.
            self.log.warning(
                "Multiple titles found for identifier %s, using the first one: %r. Other titles: %r",
                identifier.identifier,
                title,
                titles,
            )

        checkout_format = title.availability.checkout_format

        # We treat the Blio format as equivalent to AxisNow for the purposes of fulfillment.
        if checkout_format == Axis360Format.blio:
            checkout_format = Axis360Format.axis_now

        if checkout_format != internal_format:
            # The book is checked out in a format that does not match the requested internal format.
            self.log.error(
                "Cannot fulfill request for identifier %s in format %s. "
                "Checked out format is %s. %r",
                identifier.identifier,
                internal_format,
                checkout_format,
                title,
            )
            raise FormatNotAvailable()

        if (
            checkout_format == Axis360Format.epub
            or checkout_format == Axis360Format.pdf
        ):
            return self._fulfill_acs(title)

        transaction_id = title.availability.transaction_id
        if not transaction_id:
            # If there's no transaction ID, we can't fulfill the request.
            self.log.error(
                "No transaction ID found for identifier %s. %r",
                identifier.identifier,
                title,
            )
            raise CannotFulfill()

        fulfillment_info = self.api_requests.fulfillment_info(transaction_id)

        if checkout_format == Axis360Format.acoustik and isinstance(
            fulfillment_info, FindawayFulfillmentInfoResponse
        ):
            return self._fulfill_acoustik(title, fulfillment_info, licensepool)

        elif checkout_format == Axis360Format.axis_now and isinstance(
            fulfillment_info, AxisNowFulfillmentInfoResponse
        ):
            return self._fulfill_axisnow(title, fulfillment_info)

        self.log.error(
            "Unknown format %s for identifier %s. Fulfillment info: %r",
            checkout_format,
            identifier.identifier,
            fulfillment_info,
        )

        # If we get here, we are dealing with an unknown format that we cannot fulfill.
        raise FormatNotAvailable()

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

        identifier = licensepool.identifier
        title_id = identifier.identifier
        patron_id = patron.authorization_identifier
        response = self.api_requests.add_hold(
            title_id, patron_id, hold_notification_email
        )
        hold_info = HoldInfo.from_license_pool(
            licensepool,
            start_date=utc_now(),
            hold_position=response.holds_queue_position,
        )
        return hold_info

    def release_hold(self, patron: Patron, pin: str, licensepool: LicensePool) -> None:
        identifier = licensepool.identifier
        title_id = identifier.identifier
        patron_id = patron.authorization_identifier
        self.api_requests.remove_hold(title_id, patron_id)

    def patron_activity(
        self,
        patron: Patron,
        pin: str | None,
    ) -> Generator[LoanInfo | HoldInfo]:
        patron_id = patron.authorization_identifier
        availability_response = self.api_requests.availability(patron_id=patron_id)
        for title in availability_response.titles:
            # Figure out which book we're talking about.
            axis_identifier = title.title_id
            axis_identifier_type = Identifier.AXIS_360_ID
            availability = title.availability
            if availability.is_checked_out:
                # When the item is checked out, it can be locked to a particular DRM format. So even though
                # the item supports other formats, it can only be fulfilled in the format that was checked out.
                # This format is stored in availability.checkout_format.
                if (
                    availability.checkout_format == Axis360Format.axis_now
                    or availability.checkout_format == Axis360Format.blio
                ):
                    # Ignore any AxisNow or Blio formats, since
                    # we can't fulfill them. If we add AxisNow and Blio support in the future, we can remove
                    # this check.
                    continue

                yield LoanInfo(
                    collection_id=self.collection_id,
                    identifier_type=axis_identifier_type,
                    identifier=axis_identifier,
                    start_date=availability.checkout_start_date,
                    end_date=availability.checkout_end_date,
                )

            elif availability.is_reserved:
                yield HoldInfo(
                    collection_id=self.collection_id,
                    identifier_type=axis_identifier_type,
                    identifier=axis_identifier,
                    end_date=availability.reserved_end_date,
                    hold_position=0,
                )

            elif availability.is_in_hold_queue:
                yield HoldInfo(
                    collection_id=self.collection_id,
                    identifier_type=axis_identifier_type,
                    identifier=axis_identifier,
                    hold_position=availability.holds_queue_position,
                )

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
        availability_response = self.api_requests.availability(since=since)
        yield from BibliographicParser.parse(availability_response)

    def availability_by_title_ids(
        self,
        title_ids: list[str],
    ) -> Generator[tuple[BibliographicData, CirculationData]]:
        """Find title availability for a list of titles
        :yield: A sequence of (BibliographicData, CirculationData) 2-tuples
        """
        availability_response = self.api_requests.availability(title_ids=title_ids)
        yield from BibliographicParser.parse(availability_response)

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
