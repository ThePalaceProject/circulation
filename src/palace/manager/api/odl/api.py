from __future__ import annotations

import binascii
import datetime
import json
import uuid
from functools import cached_property
from typing import Any, Literal

import dateutil
from dependency_injector.wiring import Provide, inject
from flask import url_for
from sqlalchemy import or_
from sqlalchemy.orm import Session
from uritemplate import URITemplate

from palace.manager.api.circulation import (
    BaseCirculationAPI,
    FulfillmentInfo,
    HoldInfo,
    LoanInfo,
    PatronActivityCirculationAPI,
)
from palace.manager.api.circulation_exceptions import (
    AlreadyCheckedOut,
    AlreadyOnHold,
    CannotFulfill,
    CannotLoan,
    CurrentlyAvailable,
    FormatNotAvailable,
    HoldOnUnlimitedAccess,
    HoldsNotPermitted,
    NoAvailableCopies,
    NoLicenses,
    NotCheckedOut,
    NotOnHold,
    PatronHoldLimitReached,
    PatronLoanLimitReached,
)
from palace.manager.api.lcp.hash import Hasher, HasherFactory
from palace.manager.api.odl.auth import ODLAuthenticatedGet
from palace.manager.api.odl.constants import FEEDBOOKS_AUDIO
from palace.manager.api.odl.settings import (
    OPDS2AuthType,
    OPDS2WithODLLibrarySettings,
    OPDS2WithODLSettings,
)
from palace.manager.core.lcp.credential import (
    LCPCredentialFactory,
    LCPHashedPassphrase,
    LCPUnhashedPassphrase,
)
from palace.manager.service.container import Services
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.patron import Hold, Loan, Patron
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util import base64
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.http import BadResponseException


class OPDS2WithODLApi(
    ODLAuthenticatedGet,
    PatronActivityCirculationAPI[OPDS2WithODLSettings, OPDS2WithODLLibrarySettings],
):
    """ODL (Open Distribution to Libraries) is a specification that allows
    libraries to manage their own loans and holds. It offers a deeper level
    of control to the library, but it requires the circulation manager to
    keep track of individual copies rather than just license pools, and
    manage its own holds queues.
    """

    SET_DELIVERY_MECHANISM_AT = BaseCirculationAPI.FULFILL_STEP

    # Possible status values in the License Status Document:

    # The license is available but the user hasn't fulfilled it yet.
    READY_STATUS = "ready"

    # The license is available and has been fulfilled on at least one device.
    ACTIVE_STATUS = "active"

    # The license has been revoked by the distributor.
    REVOKED_STATUS = "revoked"

    # The license has been returned early by the user.
    RETURNED_STATUS = "returned"

    # The license was returned early and was never fulfilled.
    CANCELLED_STATUS = "cancelled"

    # The license has expired.
    EXPIRED_STATUS = "expired"

    STATUS_VALUES = [
        READY_STATUS,
        ACTIVE_STATUS,
        REVOKED_STATUS,
        RETURNED_STATUS,
        CANCELLED_STATUS,
        EXPIRED_STATUS,
    ]

    @classmethod
    def settings_class(cls) -> type[OPDS2WithODLSettings]:
        return OPDS2WithODLSettings

    @classmethod
    def library_settings_class(cls) -> type[OPDS2WithODLLibrarySettings]:
        return OPDS2WithODLLibrarySettings

    @classmethod
    def label(cls) -> str:
        return "ODL 2.0"

    @classmethod
    def description(cls) -> str:
        return "Import books from a distributor that uses OPDS2 + ODL (Open Distribution to Libraries)."

    @inject
    def __init__(
        self,
        _db: Session,
        collection: Collection,
        analytics: Any = Provide[Services.analytics.analytics],
    ) -> None:
        super().__init__(_db, collection)

        if collection.protocol != self.label():
            raise ValueError(
                "Collection protocol is %s, but passed into %s!"
                % (collection.protocol, self.__class__.__name__)
            )
        self.collection_id = collection.id
        settings = self.settings
        self.data_source_name = settings.data_source
        # Create the data source if it doesn't exist yet.
        DataSource.lookup(_db, self.data_source_name, autocreate=True)
        self.analytics = analytics

        self._hasher_factory = HasherFactory()
        self._credential_factory = LCPCredentialFactory()
        self._hasher_instance: Hasher | None = None

        self.loan_limit = settings.loan_limit
        self.hold_limit = settings.hold_limit

    @cached_property
    def _username(self) -> str:
        return self.settings.username

    @cached_property
    def _password(self) -> str:
        return self.settings.password

    @cached_property
    def _auth_type(self) -> OPDS2AuthType:
        return self.settings.auth_type

    @cached_property
    def _feed_url(self) -> str:
        return self.settings.external_account_id

    def _get_hasher(self) -> Hasher:
        """Returns a Hasher instance

        :return: Hasher instance
        """
        settings = self.settings
        if self._hasher_instance is None:
            self._hasher_instance = self._hasher_factory.create(
                settings.encryption_algorithm
            )

        return self._hasher_instance

    def _url_for(self, *args: Any, **kwargs: Any) -> str:
        """Wrapper around flask's url_for to be overridden for tests."""
        return url_for(*args, **kwargs)

    def get_license_status_document(self, loan: Loan) -> dict[str, Any]:
        """Get the License Status Document for a loan.

        For a new loan, create a local loan with no external identifier and
        pass it in to this method.

        This will create the remote loan if one doesn't exist yet. The loan's
        internal database id will be used to receive notifications from the
        distributor when the loan's status changes.
        """
        _db = Session.object_session(loan)

        if loan.external_identifier:
            url = loan.external_identifier
        else:
            id = loan.license.identifier
            checkout_id = str(uuid.uuid1())
            if self.collection is None:
                raise ValueError(f"Collection not found: {self.collection_id}")
            default_loan_period = self.collection.default_loan_period(
                loan.patron.library
            )

            expires = utc_now() + datetime.timedelta(days=default_loan_period)
            # The patron UUID is generated randomly on each loan, so the distributor
            # doesn't know when multiple loans come from the same patron.
            patron_id = str(uuid.uuid1())

            library_short_name = loan.patron.library.short_name

            db = Session.object_session(loan)
            patron = loan.patron
            hasher = self._get_hasher()

            unhashed_pass: LCPUnhashedPassphrase = (
                self._credential_factory.get_patron_passphrase(db, patron)
            )
            hashed_pass: LCPHashedPassphrase = unhashed_pass.hash(hasher)
            self._credential_factory.set_hashed_passphrase(db, patron, hashed_pass)
            encoded_pass: str = base64.b64encode(binascii.unhexlify(hashed_pass.hashed))

            notification_url = self._url_for(
                "odl_notify",
                library_short_name=library_short_name,
                loan_id=loan.id,
                _external=True,
            )

            checkout_url = str(loan.license.checkout_url)
            url_template = URITemplate(checkout_url)
            url = url_template.expand(
                id=str(id),
                checkout_id=checkout_id,
                patron_id=patron_id,
                expires=expires.isoformat(),
                notification_url=notification_url,
                passphrase=encoded_pass,
                hint=self.settings.passphrase_hint,
                hint_url=self.settings.passphrase_hint_url,
            )

        response = self._get(url)
        if not (200 <= response.status_code < 300):
            header_string = ", ".join(
                {f"{k}: {v}" for k, v in response.headers.items()}
            )
            response_string = (
                response.text
                if len(response.text) < 100
                else response.text[:100] + "..."
            )
            self.log.error(
                f"Error getting License Status Document for loan ({loan.id}):  Url '{url}' returned "
                f"status code {response.status_code}. Expected 2XX. Response headers: {header_string}. "
                f"Response content: {response_string}."
            )
            raise BadResponseException(url, "License Status Document request failed.")

        try:
            status_doc = json.loads(response.content)
        except ValueError as e:
            raise BadResponseException(
                url, "License Status Document was not valid JSON."
            )
        if status_doc.get("status") not in self.STATUS_VALUES:
            raise BadResponseException(
                url, "License Status Document had an unknown status value."
            )
        return status_doc  # type: ignore[no-any-return]

    def checkin(self, patron: Patron, pin: str, licensepool: LicensePool) -> None:
        """Return a loan early."""
        _db = Session.object_session(patron)

        loan = (
            _db.query(Loan)
            .filter(Loan.patron == patron)
            .filter(Loan.license_pool_id == licensepool.id)
        )
        if loan.count() < 1:
            raise NotCheckedOut()
        loan_result = loan.one()

        if licensepool.open_access or licensepool.unlimited_access:
            # If this is an open-access or unlimited access book, we don't need to do anything.
            return

        self._checkin(loan_result)

    def _checkin(self, loan: Loan) -> bool:
        _db = Session.object_session(loan)
        doc = self.get_license_status_document(loan)
        status = doc.get("status")
        if status in [
            self.REVOKED_STATUS,
            self.RETURNED_STATUS,
            self.CANCELLED_STATUS,
            self.EXPIRED_STATUS,
        ]:
            # This loan was already returned early or revoked by the distributor, or it expired.
            self.update_loan(loan, doc)
            raise NotCheckedOut()

        return_url = None
        links = doc.get("links", [])
        for link in links:
            if link.get("rel") == "return":
                return_url = link.get("href")
                break

        if not return_url:
            # The distributor didn't provide a link to return this loan.
            # This may be because the book has already been fulfilled and
            # must be returned through the DRM system. If that's true, the
            # app will already be doing that on its own, so we'll silently
            # do nothing.
            return False

        # Hit the distributor's return link.
        self._get(return_url)
        # Get the status document again to make sure the return was successful,
        # and if so update the pool availability and delete the local loan.
        self.update_loan(loan)

        # At this point, if the loan still exists, something went wrong.
        # However, it might be because the loan has already been fulfilled
        # and must be returned through the DRM system, which the app will
        # do on its own, so we can ignore the problem.
        new_loan = get_one(_db, Loan, id=loan.id)
        if new_loan:
            return False
        return True

    def checkout(
        self,
        patron: Patron,
        pin: str | None,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism,
    ) -> LoanInfo:
        """Create a new loan."""
        _db = Session.object_session(patron)

        loan = (
            _db.query(Loan)
            .filter(Loan.patron == patron)
            .filter(Loan.license_pool_id == licensepool.id)
        )
        if loan.count() > 0:
            raise AlreadyCheckedOut()

        if licensepool.open_access or licensepool.unlimited_access:
            loan_start = None
            loan_end = None
            external_identifier = None
        else:
            hold = get_one(_db, Hold, patron=patron, license_pool_id=licensepool.id)
            loan_obj = self._checkout(patron, licensepool, hold)
            loan_start = loan_obj.start
            loan_end = loan_obj.end
            external_identifier = loan_obj.external_identifier

        return LoanInfo(
            licensepool.collection,
            licensepool.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            loan_start,
            loan_end,
            external_identifier=external_identifier,
        )

    def _checkout(
        self, patron: Patron, licensepool: LicensePool, hold: Hold | None = None
    ) -> Loan:
        # If the loan limit is not None or 0
        if self.loan_limit:
            loans = list(
                filter(
                    lambda x: x.license_pool.collection.id == self.collection_id,
                    patron.loans,
                )
            )
            if len(loans) >= self.loan_limit:
                raise PatronLoanLimitReached(limit=self.loan_limit)

        _db = Session.object_session(patron)

        if not any(l for l in licensepool.licenses if not l.is_inactive):
            raise NoLicenses()

        # Make sure pool info is updated.
        self.update_licensepool(licensepool)

        if hold:
            self._update_hold_data(hold)

        # If there's a holds queue, the patron must have a non-expired hold
        # with position 0 to check out the book.
        if (
            not hold
            or (hold.position and hold.position > 0)
            or (hold.end and hold.end < utc_now())
        ) and licensepool.licenses_available < 1:
            raise NoAvailableCopies()

        # Create a local loan so its database id can be used to
        # receive notifications from the distributor.
        license = licensepool.best_available_license()
        if not license:
            raise NoAvailableCopies()
        loan, ignore = license.loan_to(patron)

        doc = self.get_license_status_document(loan)
        status = doc.get("status")

        if status not in [self.READY_STATUS, self.ACTIVE_STATUS]:
            # Something went wrong with this loan and we don't actually
            # have the book checked out. This should never happen.
            # Remove the loan we created.
            _db.delete(loan)
            raise CannotLoan()

        links = doc.get("links", [])
        external_identifier = None
        for link in links:
            if link.get("rel") == "self":
                external_identifier = link.get("href")
                break
        if not external_identifier:
            _db.delete(loan)
            raise CannotLoan()

        start = utc_now()
        expires = doc.get("potential_rights", {}).get("end")
        if expires:
            expires = dateutil.parser.parse(expires)

        # We need to set the start and end dates on our local loan since
        # the code that calls this only sets them when a new loan is created.
        loan.start = start
        loan.end = expires
        loan.external_identifier = external_identifier

        # We also need to update the remaining checkouts for the license.
        loan.license.checkout()

        # We have successfully borrowed this book.
        if hold:
            _db.delete(hold)
            # log circulation event:  hold converted to loan
        self.update_licensepool(licensepool)
        return loan

    def fulfill(
        self,
        patron: Patron,
        pin: str,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism,
    ) -> FulfillmentInfo:
        """Get the actual resource file to the patron."""
        _db = Session.object_session(patron)

        loan = (
            _db.query(Loan)
            .filter(Loan.patron == patron)
            .filter(Loan.license_pool_id == licensepool.id)
        ).one()
        return self._fulfill(loan, delivery_mechanism)

    @staticmethod
    def _find_content_link_and_type(
        links: list[dict[str, str]],
        drm_scheme: str | None,
    ) -> tuple[str | None, str | None]:
        """Find a content link with the type information corresponding to the selected delivery mechanism.

        :param links: List of dict-like objects containing information about available links in the LCP license file
        :param drm_scheme: Selected delivery mechanism DRM scheme

        :return: Two-tuple containing a content link and content type
        """
        candidates = []
        for link in links:
            # Depending on the format being served, the crucial information
            # may be in 'manifest' or in 'license'.
            if link.get("rel") not in ("manifest", "license"):
                continue
            href = link.get("href")
            type = link.get("type")
            candidates.append((href, type))

        if len(candidates) == 0:
            # No candidates
            return None, None

        # For DeMarque audiobook content, we need to translate the type property
        # to reflect what we have stored in our delivery mechanisms.
        if drm_scheme == DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM:
            drm_scheme = FEEDBOOKS_AUDIO

        return next(filter(lambda x: x[1] == drm_scheme, candidates), (None, None))

    def _unlimited_access_fulfill(
        self, loan: Loan, delivery_mechanism: LicensePoolDeliveryMechanism
    ) -> FulfillmentInfo:
        licensepool = loan.license_pool
        fulfillment = self._find_matching_delivery_mechanism(
            delivery_mechanism.delivery_mechanism, licensepool
        )
        content_link = fulfillment.resource.representation.public_url
        content_type = fulfillment.resource.representation.media_type
        return FulfillmentInfo(
            licensepool.collection,
            licensepool.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            content_link,
            content_type,
            None,
            None,
        )

    def _find_matching_delivery_mechanism(
        self, requested_delivery_mechanism: DeliveryMechanism, licensepool: LicensePool
    ) -> LicensePoolDeliveryMechanism:
        fulfillment = next(
            (
                lpdm
                for lpdm in licensepool.delivery_mechanisms
                if lpdm.delivery_mechanism == requested_delivery_mechanism
            ),
            None,
        )
        if fulfillment is None:
            raise FormatNotAvailable()
        return fulfillment

    def _lcp_fulfill(
        self, loan: Loan, delivery_mechanism: LicensePoolDeliveryMechanism
    ) -> FulfillmentInfo:
        doc = self.get_license_status_document(loan)
        status = doc.get("status")

        if status not in [self.READY_STATUS, self.ACTIVE_STATUS]:
            # This loan isn't available for some reason. It's possible
            # the distributor revoked it or the patron already returned it
            # through the DRM system, and we didn't get a notification
            # from the distributor yet.
            self.update_loan(loan, doc)
            raise CannotFulfill()

        expires = doc.get("potential_rights", {}).get("end")
        expires = dateutil.parser.parse(expires)

        links = doc.get("links", [])

        content_link, content_type = self._find_content_link_and_type(
            links, delivery_mechanism.delivery_mechanism.drm_scheme
        )

        return FulfillmentInfo(
            loan.license_pool.collection,
            loan.license_pool.data_source.name,
            loan.license_pool.identifier.type,
            loan.license_pool.identifier.identifier,
            content_link,
            content_type,
            None,
            expires,
        )

    def _bearer_token_fulfill(
        self, loan: Loan, delivery_mechanism: LicensePoolDeliveryMechanism
    ) -> FulfillmentInfo:
        licensepool = loan.license_pool
        fulfillment_mechanism = self._find_matching_delivery_mechanism(
            delivery_mechanism.delivery_mechanism, licensepool
        )

        # Make sure we have a session token to pass to the app. If the token expires in the
        # next 10 minutes, we'll refresh it to make sure the app has enough time to download the book.
        if (
            self._session_token is None
            or self._session_token.expires - datetime.timedelta(minutes=10) < utc_now()
        ):
            self._refresh_token()

        # At this point the token should never be None, but for mypy to be happy we'll assert it.
        assert self._session_token is not None

        # Build an application/vnd.librarysimplified.bearer-token
        # document using information from the credential.
        token_document = dict(
            token_type="Bearer",
            access_token=self._session_token.token,
            expires_in=(int((self._session_token.expires - utc_now()).total_seconds())),
            location=fulfillment_mechanism.resource.url,
        )

        return FulfillmentInfo(
            licensepool.collection,
            licensepool.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            content_link=None,
            content_type=DeliveryMechanism.BEARER_TOKEN,
            content=json.dumps(token_document),
            content_expires=self._session_token.expires,
        )

    def _fulfill(
        self,
        loan: Loan,
        delivery_mechanism: LicensePoolDeliveryMechanism,
    ) -> FulfillmentInfo:
        if loan.license_pool.open_access or loan.license_pool.unlimited_access:
            if (
                delivery_mechanism.delivery_mechanism.drm_scheme
                == DeliveryMechanism.BEARER_TOKEN
                and self._auth_type == OPDS2AuthType.OAUTH
            ):
                return self._bearer_token_fulfill(loan, delivery_mechanism)
            else:
                return self._unlimited_access_fulfill(loan, delivery_mechanism)
        else:
            return self._lcp_fulfill(loan, delivery_mechanism)

    def _count_holds_before(self, holdinfo: HoldInfo, pool: LicensePool) -> int:
        # Count holds on the license pool that started before this hold and
        # aren't expired.
        _db = Session.object_session(pool)
        return (
            _db.query(Hold)
            .filter(Hold.license_pool_id == pool.id)
            .filter(Hold.start < holdinfo.start_date)
            .filter(
                or_(
                    Hold.end == None,
                    Hold.end > utc_now(),
                    Hold.position > 0,
                )
            )
            .count()
        )

    def _update_hold_data(self, hold: Hold) -> None:
        pool: LicensePool = hold.license_pool
        holdinfo = HoldInfo(
            pool.collection,
            pool.data_source.name,
            pool.identifier.type,
            pool.identifier.identifier,
            hold.start,
            hold.end,
            hold.position,
        )
        library = hold.patron.library
        self._update_hold_end_date(holdinfo, pool, library=library)
        hold.end = holdinfo.end_date
        hold.position = holdinfo.hold_position

    def _update_hold_end_date(
        self, holdinfo: HoldInfo, pool: LicensePool, library: Library
    ) -> None:
        _db = Session.object_session(pool)

        # First make sure the hold position is up-to-date, since we'll
        # need it to calculate the end date.
        original_position = holdinfo.hold_position
        self._update_hold_position(holdinfo, pool)
        assert holdinfo.hold_position is not None

        if self.collection is None:
            raise ValueError(f"Collection not found: {self.collection_id}")
        default_loan_period = self.collection.default_loan_period(library)
        default_reservation_period = self.collection.default_reservation_period

        # If the hold was already to check out and already has an end date,
        # it doesn't need an update.
        if holdinfo.hold_position == 0 and original_position == 0 and holdinfo.end_date:
            return

        # If the patron is in the queue, we need to estimate when the book
        # will be available for check out. We can do slightly better than the
        # default calculation since we know when all current loans will expire,
        # but we're still calculating the worst case.
        elif holdinfo.hold_position > 0:
            # Find the current loans and reserved holds for the licenses.
            current_loans = (
                _db.query(Loan)
                .filter(Loan.license_pool_id == pool.id)
                .filter(or_(Loan.end == None, Loan.end > utc_now()))
                .order_by(Loan.start)
                .all()
            )
            current_holds = (
                _db.query(Hold)
                .filter(Hold.license_pool_id == pool.id)
                .filter(
                    or_(
                        Hold.end == None,
                        Hold.end > utc_now(),
                        Hold.position > 0,
                    )
                )
                .order_by(Hold.start)
                .all()
            )
            assert pool.licenses_owned is not None
            licenses_reserved = min(
                pool.licenses_owned - len(current_loans), len(current_holds)
            )
            current_reservations = current_holds[:licenses_reserved]

            # The licenses will have to go through some number of cycles
            # before one of them gets to this hold. This leavs out the first cycle -
            # it's already started so we'll handle it separately.
            cycles = (
                holdinfo.hold_position - licenses_reserved - 1
            ) // pool.licenses_owned

            # Each of the owned licenses is currently either on loan or reserved.
            # Figure out which license this hold will eventually get if every
            # patron keeps their loans and holds for the maximum time.
            copy_index = (
                holdinfo.hold_position - licenses_reserved - 1
            ) % pool.licenses_owned

            # In the worse case, the first cycle ends when a current loan expires, or
            # after a current reservation is checked out and then expires.
            if len(current_loans) > copy_index:
                next_cycle_start = current_loans[copy_index].end
            else:
                reservation = current_reservations[copy_index - len(current_loans)]
                next_cycle_start = reservation.end + datetime.timedelta(
                    days=default_loan_period
                )

            # Assume all cycles after the first cycle take the maximum time.
            cycle_period = default_loan_period + default_reservation_period
            holdinfo.end_date = next_cycle_start + datetime.timedelta(
                days=(cycle_period * cycles)
            )

        # If the end date isn't set yet or the position just became 0, the
        # hold just became available. The patron's reservation period starts now.
        else:
            holdinfo.end_date = utc_now() + datetime.timedelta(
                days=default_reservation_period
            )

    def _update_hold_position(self, holdinfo: HoldInfo, pool: LicensePool) -> None:
        _db = Session.object_session(pool)
        loans_count = (
            _db.query(Loan)
            .filter(
                Loan.license_pool_id == pool.id,
            )
            .filter(or_(Loan.end == None, Loan.end > utc_now()))
            .count()
        )
        holds_count = self._count_holds_before(holdinfo, pool)

        assert pool.licenses_owned is not None
        remaining_licenses = pool.licenses_owned - loans_count

        if remaining_licenses > holds_count:
            # The hold is ready to check out.
            holdinfo.hold_position = 0

        else:
            # Add 1 since position 0 indicates the hold is ready.
            holdinfo.hold_position = holds_count + 1

    def update_licensepool(self, licensepool: LicensePool) -> None:
        # Update the pool and the next holds in the queue when a license is reserved.
        licensepool.update_availability_from_licenses(
            as_of=utc_now(),
        )
        holds = licensepool.get_active_holds()
        for hold in holds[: licensepool.licenses_reserved]:
            if hold.position != 0:
                # This hold just got a reserved license.
                self._update_hold_data(hold)

    def place_hold(
        self,
        patron: Patron,
        pin: str | None,
        licensepool: LicensePool,
        notification_email_address: str | None,
    ) -> HoldInfo:
        """Create a new hold."""
        if licensepool.open_access or licensepool.unlimited_access:
            raise HoldOnUnlimitedAccess()

        return self._place_hold(patron, licensepool)

    def _place_hold(self, patron: Patron, licensepool: LicensePool) -> HoldInfo:
        if self.hold_limit is not None:
            holds = list(
                filter(
                    lambda x: x.license_pool.collection.id == self.collection_id,
                    patron.holds,
                )
            )
            if self.hold_limit == 0:
                raise HoldsNotPermitted()
            if len(holds) >= self.hold_limit:
                raise PatronHoldLimitReached(limit=self.hold_limit)

        _db = Session.object_session(patron)

        # Make sure pool info is updated.
        self.update_licensepool(licensepool)

        if licensepool.licenses_available > 0:
            raise CurrentlyAvailable()

        # Check for local hold
        hold = get_one(
            _db,
            Hold,
            patron_id=patron.id,
            license_pool_id=licensepool.id,
        )

        if hold is not None:
            raise AlreadyOnHold()

        patrons_in_hold_queue = (
            licensepool.patrons_in_hold_queue
            if licensepool.patrons_in_hold_queue
            else 0
        )
        licensepool.patrons_in_hold_queue = patrons_in_hold_queue + 1
        holdinfo = HoldInfo(
            licensepool.collection,
            licensepool.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            utc_now(),
            None,
            0,
        )
        library = patron.library
        self._update_hold_end_date(holdinfo, licensepool, library=library)

        return holdinfo

    def release_hold(self, patron: Patron, pin: str, licensepool: LicensePool) -> None:
        """Cancel a hold."""
        _db = Session.object_session(patron)
        hold = get_one(
            _db,
            Hold,
            license_pool_id=licensepool.id,
            patron=patron,
        )
        if not hold:
            raise NotOnHold()
        self._release_hold(hold)

    def _release_hold(self, hold: Hold) -> Literal[True]:
        # If the book was ready and the patron revoked the hold instead
        # of checking it out, but no one else had the book on hold, the
        # book is now available for anyone to check out. If someone else
        # had a hold, the license is now reserved for the next patron.
        # If someone else had a hold, the license is now reserved for the
        # next patron, and we need to update that hold.
        _db = Session.object_session(hold)
        licensepool = hold.license_pool
        _db.delete(hold)

        # log a circulation event : hold_released
        self.update_licensepool(licensepool)
        return True

    def patron_activity(
        self, patron: Patron, pin: str | None
    ) -> list[LoanInfo | HoldInfo]:
        """Look up non-expired loans for this collection in the database."""
        _db = Session.object_session(patron)
        loans = (
            _db.query(Loan)
            .join(Loan.license_pool)
            .filter(LicensePool.collection_id == self.collection_id)
            .filter(Loan.patron == patron)
            .filter(
                or_(
                    Loan.end >= utc_now(),
                    Loan.end == None,
                )
            )
        )

        # Get the patron's holds. If there are any expired holds, delete them.
        # Update the end date and position for the remaining holds.
        holds = (
            _db.query(Hold)
            .join(Hold.license_pool)
            .filter(LicensePool.collection_id == self.collection_id)
            .filter(Hold.patron == patron)
        )
        remaining_holds = []
        for hold in holds:
            if hold.end and hold.end < utc_now():
                _db.delete(hold)
                # log circulation event:  hold expired
                self.update_licensepool(hold.license_pool)
            else:
                self._update_hold_data(hold)
                remaining_holds.append(hold)

        return [
            LoanInfo(
                loan.license_pool.collection,
                loan.license_pool.data_source.name,
                loan.license_pool.identifier.type,
                loan.license_pool.identifier.identifier,
                loan.start,
                loan.end,
                external_identifier=loan.external_identifier,
            )
            for loan in loans
        ] + [
            HoldInfo(
                hold.license_pool.collection,
                hold.license_pool.data_source.name,
                hold.license_pool.identifier.type,
                hold.license_pool.identifier.identifier,
                start_date=hold.start,
                end_date=hold.end,
                hold_position=hold.position,
            )
            for hold in remaining_holds
        ]

    def update_loan(self, loan: Loan, status_doc: dict[str, Any] | None = None) -> None:
        """Check a loan's status, and if it is no longer active, delete the loan
        and update its pool's availability.
        """
        _db = Session.object_session(loan)

        if not status_doc:
            status_doc = self.get_license_status_document(loan)

        status = status_doc.get("status")
        # We already check that the status is valid in get_license_status_document,
        # but if the document came from a notification it hasn't been checked yet.
        if status not in self.STATUS_VALUES:
            raise BadResponseException(
                str(loan.license.checkout_url),
                "The License Status Document had an unknown status value.",
            )

        if status in [
            self.REVOKED_STATUS,
            self.RETURNED_STATUS,
            self.CANCELLED_STATUS,
            self.EXPIRED_STATUS,
        ]:
            # This loan is no longer active. Update the pool's availability
            # and delete the loan.

            # Update the license
            loan.license.checkin()

            # If there are holds, the license is reserved for the next patron.
            _db.delete(loan)
            self.update_licensepool(loan.license_pool)

    def update_availability(self, licensepool: LicensePool) -> None:
        pass

    def can_fulfill_without_loan(
        self,
        patron: Patron | None,
        pool: LicensePool,
        lpdm: LicensePoolDeliveryMechanism,
    ) -> bool:
        return False
