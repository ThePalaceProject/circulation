from __future__ import annotations

import html
import re
from collections.abc import Callable, Generator, Sequence
from datetime import timedelta
from typing import Annotated, Unpack, cast

from celery.canvas import Signature
from flask_babel import lazy_gettext as _
from pydantic import StringConstraints, TypeAdapter, ValidationError
from sqlalchemy.orm import Session

from palace.manager.api.circulation.base import (
    BaseCirculationAPI,
    PatronActivityCirculationAPI,
    SupportsImport,
)
from palace.manager.api.circulation.data import HoldInfo, LoanInfo
from palace.manager.api.circulation.exceptions import (
    CannotFulfill,
    DeliveryMechanismError,
    FormatNotAvailable,
    InvalidInputException,
    NoActiveLoan,
    RemoteInitiatedServerError,
)
from palace.manager.api.circulation.fulfillment import DirectFulfillment, Fulfillment
from palace.manager.api.selftest import HasCollectionSelfTests
from palace.manager.api.web_publication_manifest import FindawayManifest, SpineItem
from palace.manager.celery.tasks import boundless
from palace.manager.core.selftest import SelfTestResult
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.format import FormatData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.policy.replacement import ReplacementPolicy
from palace.manager.integration.license.boundless.constants import (
    AXIS_360_PROTOCOL,
    BAKER_TAYLOR_KDRM_PARAMS,
    DELIVERY_MECHANISM_TO_INTERNAL_FORMAT,
    INTERNAL_FORMAT_TO_DELIVERY_MECHANISM,
    BoundlessFormat,
)
from palace.manager.integration.license.boundless.fulfillment import (
    BoundlessAcsFulfillment,
)
from palace.manager.integration.license.boundless.model.json import (
    AxisNowFulfillmentInfoResponse,
    FindawayFulfillmentInfoResponse,
)
from palace.manager.integration.license.boundless.model.response import (
    KdrmFulfillmentResponse,
)
from palace.manager.integration.license.boundless.model.xml import Title
from palace.manager.integration.license.boundless.parser import BibliographicParser
from palace.manager.integration.license.boundless.requests import BoundlessRequests
from palace.manager.integration.license.boundless.settings import (
    BoundlessLibrarySettings,
    BoundlessSettings,
)
from palace.manager.opds.types.link import BaseLink
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    DeliveryMechanismTuple,
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.sqlalchemy.model.resource import Representation
from palace.manager.util.datetime_helpers import utc_now


class BoundlessApi(
    PatronActivityCirculationAPI[BoundlessSettings, BoundlessLibrarySettings],
    HasCollectionSelfTests,
    SupportsImport,
):
    SET_DELIVERY_MECHANISM_AT = BaseCirculationAPI.BORROW_STEP

    @classmethod
    def settings_class(cls) -> type[BoundlessSettings]:
        return BoundlessSettings

    @classmethod
    def library_settings_class(cls) -> type[BoundlessLibrarySettings]:
        return BoundlessLibrarySettings

    @classmethod
    def label(cls) -> str:
        return AXIS_360_PROTOCOL

    @classmethod
    def description(cls) -> str:
        return ""

    def __init__(
        self,
        _db: Session,
        collection: Collection,
        requests: BoundlessRequests | None = None,
    ) -> None:
        super().__init__(_db, collection)
        self.api_requests = (
            BoundlessRequests(self.settings) if requests is None else requests
        )
        self._prioritize_boundless_drm = self.settings.prioritize_boundless_drm

    @staticmethod
    def _delivery_mechanism_to_internal_format(
        delivery_mechanism: LicensePoolDeliveryMechanism,
    ) -> str:
        """Look up the internal format for this delivery mechanism or
        raise an exception.

        :param delivery_mechanism: A LicensePoolDeliveryMechanism
        """
        d = delivery_mechanism.delivery_mechanism
        key = DeliveryMechanismTuple(d.content_type, d.drm_scheme)
        internal_format = DELIVERY_MECHANISM_TO_INTERNAL_FORMAT.get(key)
        if internal_format is None:
            raise DeliveryMechanismError(
                _(
                    "Could not map delivery mechanism %(mechanism_name)s to internal delivery mechanism!",
                    mechanism_name=d.name,
                )
            )
        return internal_format

    @property
    def data_source(self) -> DataSource:
        return DataSource.lookup(self._db, DataSource.BOUNDLESS, autocreate=True)

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
            count = len(self.api_requests.availability(since=five_minutes_ago).titles)
            return "Found %d event(s)" % count

        yield self.run_test(
            "Asking for circulation events for the last five minutes", _count_events
        )

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
        :raise BoundlessValidationError: If the API returns an invalid response.
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
            title_id,
            patron_id,
            self._delivery_mechanism_to_internal_format(delivery_mechanism),
        )
        return LoanInfo.from_license_pool(
            licensepool, end_date=response.expiration_date
        )

    def _fulfill_acs(self, title: Title) -> BoundlessAcsFulfillment:
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
        return BoundlessAcsFulfillment(
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

    # See RFC7515 for base64 encoding specification. Its normal base64 with no trailing
    # = padding, and using URL-safe characters (i.e., '-' instead of '+', and '_' instead of '/').
    _baker_taylor_base64_regex = re.compile(r"^[0-9a-zA-Z_-]+$")
    # RSA 2048 bit modulus is 256 bytes, which is ceil(256 * 4 / 3) = 342 characters when
    # base64 encoded without padding.
    _baker_taylor_kdrm_modulus_validator: Callable[[str], None] = TypeAdapter(
        Annotated[
            str,
            StringConstraints(
                min_length=342, max_length=342, pattern=_baker_taylor_base64_regex
            ),
        ]
    ).validate_python
    # The exponent is typically 65537, which is 4 characters when base64 encoded without padding,
    # but this isn't guaranteed, so we just validate the proper base64 encoding.
    _baker_taylor_kdrm_exponent_validator: Callable[[str], None] = TypeAdapter(
        Annotated[
            str,
            StringConstraints(pattern=_baker_taylor_base64_regex),
        ]
    ).validate_python

    @classmethod
    def _validate_baker_taylor_kdrm_param(
        cls, validation_func: Callable[[str], None], param_name: str, param: str
    ) -> None:
        """Validate the Baker & Taylor KDRM modulus parameter."""
        try:
            validation_func(param)
        except ValidationError as e:
            error = e.errors()[0]
            error_msg = error["msg"]
            error_type = error["type"]

            # In the case of a pattern mismatch, we can provide a more specific error message.
            if error_type == "string_pattern_mismatch":
                error_msg = "String should be a url safe base64 encoded string with no padding, see RFC 7515"

            raise InvalidInputException(
                "Invalid parameters, unable to fulfill loan",
                f"Error validating {param_name} '{param}': {error_msg}",
            )

    @classmethod
    def _validate_baker_taylor_kdrm_params(cls, modulus: str, exponent: str) -> None:
        cls._validate_baker_taylor_kdrm_param(
            cls._baker_taylor_kdrm_modulus_validator, "modulus", modulus
        )
        cls._validate_baker_taylor_kdrm_param(
            cls._baker_taylor_kdrm_exponent_validator, "exponent", exponent
        )

    def _fulfill_baker_taylor_kdrm(
        self,
        title: Title,
        fulfillment_info: AxisNowFulfillmentInfoResponse,
        **kwargs: Unpack[BaseCirculationAPI.FulfillKwargs],
    ) -> DirectFulfillment:
        kdrm_params = set(BAKER_TAYLOR_KDRM_PARAMS) | {"client_ip"}
        missing_params = {param for param in kdrm_params if not kwargs.get(param)}
        params: dict[str, str] = {
            param: cast(str, kwargs.get(param))
            for param in kdrm_params - missing_params
        }

        if missing_params:
            debug_message = (
                f"Missing parameters ({', '.join(missing_params)}) for Baker & Taylor KDRM fulfillment: "
                f"title_id={title.title_id} isbn={fulfillment_info.isbn} device_id={params.get('device_id')}"
            )
            self.log.error(debug_message)
            raise InvalidInputException(
                "Missing required URL parameters for fulfillment",
                debug_message,
            )

        # It appears that Baker & Taylor does basically no validation on the parameters we send
        # so we do a little validation here, so we can give more friendly error messages.
        self._validate_baker_taylor_kdrm_params(
            modulus=params["modulus"],
            exponent=params["exponent"],
        )

        license_response = self.api_requests.license(
            book_vault_uuid=fulfillment_info.book_vault_uuid,
            isbn=fulfillment_info.isbn,
            exponent=params["exponent"],
            modulus=params["modulus"],
            device_id=params["device_id"],
            client_ip=params["client_ip"],
        )

        response_document = KdrmFulfillmentResponse(
            license_document=license_response,
            links=[
                BaseLink(
                    rel="publication",
                    href=self.api_requests.encrypted_content_url(fulfillment_info.isbn),
                    type=Representation.EPUB_MEDIA_TYPE,
                )
            ],
        )

        return DirectFulfillment(
            response_document.model_dump_json(by_alias=True, exclude_defaults=True),
            DeliveryMechanism.BAKER_TAYLOR_KDRM_DRM,
        )

    def fulfill(
        self,
        patron: Patron,
        pin: str,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism,
        **kwargs: Unpack[BaseCirculationAPI.FulfillKwargs],
    ) -> Fulfillment:
        """Fulfill a patron's request for a specific book."""
        identifier = licensepool.identifier
        internal_format = self._delivery_mechanism_to_internal_format(
            delivery_mechanism
        )

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
            # The API did not return any titles for this identifier, so
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

        checkout_format = title.availability.checkout_format_normalized

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
            checkout_format == BoundlessFormat.epub
            or checkout_format == BoundlessFormat.pdf
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

        if checkout_format == BoundlessFormat.acoustik and isinstance(
            fulfillment_info, FindawayFulfillmentInfoResponse
        ):
            return self._fulfill_acoustik(title, fulfillment_info, licensepool)

        elif checkout_format == BoundlessFormat.axis_now and isinstance(
            fulfillment_info, AxisNowFulfillmentInfoResponse
        ):
            return self._fulfill_baker_taylor_kdrm(title, fulfillment_info, **kwargs)

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
            title_id = title.title_id
            identifier_type = Identifier.AXIS_360_ID
            availability = title.availability
            if availability.is_checked_out:
                # When the item is checked out, it can be locked to a particular DRM format. So even though
                # the item supports other formats, it can only be fulfilled in the format that was checked out.
                # This format is stored in availability.checkout_format_normalized.
                internal_format = availability.checkout_format_normalized
                if internal_format is not None:
                    if (
                        delivery_mechanism := INTERNAL_FORMAT_TO_DELIVERY_MECHANISM.get(
                            internal_format
                        )
                    ) is None:
                        self.log.error(
                            "Unknown checkout format %s for identifier %s. %r",
                            availability.checkout_format,
                            title_id,
                            title,
                        )
                        continue

                    locked_to = FormatData(
                        content_type=delivery_mechanism.content_type,
                        drm_scheme=delivery_mechanism.drm_scheme,
                    )
                else:
                    locked_to = None

                yield LoanInfo(
                    collection_id=self.collection_id,
                    identifier_type=identifier_type,
                    identifier=title_id,
                    start_date=availability.checkout_start_date,
                    end_date=availability.checkout_end_date,
                    locked_to=locked_to,
                )

            elif availability.is_reserved:
                yield HoldInfo(
                    collection_id=self.collection_id,
                    identifier_type=identifier_type,
                    identifier=title_id,
                    end_date=availability.reserved_end_date,
                    hold_position=0,
                )

            elif availability.is_in_hold_queue:
                yield HoldInfo(
                    collection_id=self.collection_id,
                    identifier_type=identifier_type,
                    identifier=title_id,
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

        # We asked Boundless about n books. It sent us n-k responses. Those
        # k books are the identifiers in `remainder`. These books have
        # been removed from the collection without us being notified.
        for removed_identifier in remainder:
            self._reap(removed_identifier)

    def update_book(
        self,
        bibliographic: BibliographicData,
    ) -> tuple[Edition, bool, LicensePool, bool]:
        """Create or update a single book based on bibliographic
        and availability data from the Boundless API.

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
            self._db, collection, ReplacementPolicy.from_license_source()
        )

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

    def sort_delivery_mechanisms(
        self, lpdms: list[LicensePoolDeliveryMechanism]
    ) -> list[LicensePoolDeliveryMechanism]:
        """
        Do any custom sorting of delivery mechanisms configured for this API.
        """
        if self._prioritize_boundless_drm:
            # If we prioritize Boundless DRM, we want to put the Boundless DRM
            # delivery mechanism first in the list.
            lpdms = sorted(
                lpdms,
                key=lambda x: (
                    1
                    if (
                        x.delivery_mechanism.drm_scheme
                        != DeliveryMechanism.BAKER_TAYLOR_KDRM_DRM
                    )
                    else 0
                ),
            )

        return lpdms

    @classmethod
    def import_task(cls, collection_id: int, force: bool = False) -> Signature:
        return boundless.import_collection.s(collection_id, import_all=force)
