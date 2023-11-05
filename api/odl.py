from __future__ import annotations

import binascii
import datetime
import json
import uuid
from abc import ABC
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Type, TypeVar

import dateutil
from dependency_injector.wiring import Provide, inject
from flask import url_for
from flask_babel import lazy_gettext as _
from lxml.etree import Element
from pydantic import HttpUrl, PositiveInt
from requests import Response
from sqlalchemy.sql.expression import or_
from uritemplate import URITemplate

from api.circulation import (
    BaseCirculationAPI,
    BaseCirculationEbookLoanSettings,
    FulfillmentInfo,
    HoldInfo,
    LoanInfo,
    PatronActivityCirculationAPI,
)
from api.circulation_exceptions import *
from api.lcp.hash import Hasher, HasherFactory, HashingAlgorithm
from core import util
from core.integration.settings import (
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
)
from core.lcp.credential import (
    LCPCredentialFactory,
    LCPHashedPassphrase,
    LCPUnhashedPassphrase,
)
from core.metadata_layer import FormatData, LicenseData, TimestampData
from core.model import (
    Collection,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Hold,
    Hyperlink,
    Library,
    LicensePool,
    LicensePoolDeliveryMechanism,
    Loan,
    MediaTypes,
    Representation,
    RightsStatus,
    Session,
    get_one,
)
from core.model.licensing import LicenseStatus
from core.model.patron import Patron
from core.monitor import CollectionMonitor
from core.opds_import import (
    BaseOPDSImporter,
    OPDSImporter,
    OPDSImporterSettings,
    OPDSImportMonitor,
    OPDSXMLParser,
)
from core.service.container import Services
from core.util import base64
from core.util.datetime_helpers import to_utc, utc_now
from core.util.http import HTTP, BadResponseException


class ODLAPIConstants:
    DEFAULT_PASSPHRASE_HINT = "View the help page for more information."
    DEFAULT_PASSPHRASE_HINT_URL = "https://lyrasis.zendesk.com/"


class ODLSettings(OPDSImporterSettings):
    external_account_id: Optional[HttpUrl] = FormField(
        form=ConfigurationFormItem(
            label=_("ODL feed URL"),
            required=True,
        ),
    )

    username: str = FormField(
        form=ConfigurationFormItem(
            label=_("Library's API username"),
            required=True,
        )
    )

    password: str = FormField(
        key=ExternalIntegration.PASSWORD,
        form=ConfigurationFormItem(
            label=_("Library's API password"),
            required=True,
        ),
    )

    default_reservation_period: Optional[PositiveInt] = FormField(
        default=Collection.STANDARD_DEFAULT_RESERVATION_PERIOD,
        form=ConfigurationFormItem(
            label=_("Default Reservation Period (in Days)"),
            description=_(
                "The number of days a patron has to check out a book after a hold becomes available."
            ),
            type=ConfigurationFormItemType.NUMBER,
            required=False,
        ),
    )

    passphrase_hint: str = FormField(
        default=ODLAPIConstants.DEFAULT_PASSPHRASE_HINT,
        form=ConfigurationFormItem(
            label=_("Passphrase hint"),
            description=_(
                "Hint displayed to the user when opening an LCP protected publication."
            ),
            type=ConfigurationFormItemType.TEXT,
            required=True,
        ),
    )

    passphrase_hint_url: HttpUrl = FormField(
        default=ODLAPIConstants.DEFAULT_PASSPHRASE_HINT_URL,
        form=ConfigurationFormItem(
            label=_("Passphrase hint URL"),
            description=_(
                "Hint URL available to the user when opening an LCP protected publication."
            ),
            type=ConfigurationFormItemType.TEXT,
            required=True,
        ),
    )

    encryption_algorithm: HashingAlgorithm = FormField(
        default=HashingAlgorithm.SHA256,
        form=ConfigurationFormItem(
            label=_("Passphrase encryption algorithm"),
            description=_("Algorithm used for encrypting the passphrase."),
            type=ConfigurationFormItemType.SELECT,
            required=False,
            options={alg: alg.name for alg in HashingAlgorithm},
        ),
    )


class ODLLibrarySettings(BaseCirculationEbookLoanSettings):
    pass


SettingsType = TypeVar("SettingsType", bound=ODLSettings, covariant=True)
LibrarySettingsType = TypeVar(
    "LibrarySettingsType", bound=ODLLibrarySettings, covariant=True
)


class BaseODLAPI(PatronActivityCirculationAPI[SettingsType, LibrarySettingsType], ABC):
    """ODL (Open Distribution to Libraries) is a specification that allows
    libraries to manage their own loans and holds. It offers a deeper level
    of control to the library, but it requires the circulation manager to
    keep track of individual copies rather than just license pools, and
    manage its own holds queues.

    In addition to circulating books to patrons of a library on the current circulation
    manager, this API can be used to circulate books to patrons of external libraries.
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

        self.username = settings.username
        self.password = settings.password
        self.analytics = analytics

        self._hasher_factory = HasherFactory()
        self._credential_factory = LCPCredentialFactory()
        self._hasher_instance: Optional[Hasher] = None

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

    def _get(self, url: str, headers: Optional[Dict[str, str]] = None) -> Response:
        """Make a normal HTTP request, but include an authentication
        header with the credentials for the collection.
        """

        username = self.username
        password = self.password
        headers = dict(headers or {})
        auth_header = "Basic %s" % base64.b64encode(f"{username}:{password}")
        headers["Authorization"] = auth_header

        return HTTP.get_with_timeout(url, headers=headers)

    def _url_for(self, *args: Any, **kwargs: Any) -> str:
        """Wrapper around flask's url_for to be overridden for tests."""
        return url_for(*args, **kwargs)

    def get_license_status_document(self, loan: Loan) -> Dict[str, Any]:
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
        pin: str,
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

        hold = get_one(_db, Hold, patron=patron, license_pool_id=licensepool.id)
        loan_obj = self._checkout(patron, licensepool, hold)
        return LoanInfo(
            licensepool.collection,
            licensepool.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            loan_obj.start,
            loan_obj.end,
            external_identifier=loan_obj.external_identifier,
        )

    def _checkout(
        self, patron: Patron, licensepool: LicensePool, hold: Optional[Hold] = None
    ) -> Loan:
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
        links: List[Dict[str, str]],
        drm_scheme: Optional[str],
    ) -> Tuple[Optional[str], Optional[str]]:
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
            drm_scheme = ODLImporter.FEEDBOOKS_AUDIO

        return next(filter(lambda x: x[1] == drm_scheme, candidates), (None, None))

    def _fulfill(
        self,
        loan: Loan,
        delivery_mechanism: LicensePoolDeliveryMechanism,
    ) -> FulfillmentInfo:
        licensepool = loan.license_pool
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
            licensepool.collection,
            licensepool.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            content_link,
            content_type,
            None,
            expires,
        )

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
        pin: str,
        licensepool: LicensePool,
        notification_email_address: Optional[str],
    ) -> HoldInfo:
        """Create a new hold."""
        return self._place_hold(patron, licensepool)

    def _place_hold(self, patron: Patron, licensepool: LicensePool) -> HoldInfo:
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
        self.update_licensepool(licensepool)
        return True

    def patron_activity(self, patron: Patron, pin: str) -> List[LoanInfo | HoldInfo]:
        """Look up non-expired loans for this collection in the database."""
        _db = Session.object_session(patron)
        loans = (
            _db.query(Loan)
            .join(Loan.license_pool)
            .filter(LicensePool.collection_id == self.collection_id)
            .filter(Loan.patron == patron)
            .filter(Loan.end >= utc_now())
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

    def update_loan(
        self, loan: Loan, status_doc: Optional[Dict[str, Any]] = None
    ) -> None:
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


class ODLAPI(
    BaseODLAPI[ODLSettings, ODLLibrarySettings],
):
    """ODL (Open Distribution to Libraries) is a specification that allows
    libraries to manage their own loans and holds. It offers a deeper level
    of control to the library, but it requires the circulation manager to
    keep track of individual copies rather than just license pools, and
    manage its own holds queues.

    In addition to circulating books to patrons of a library on the current circulation
    manager, this API can be used to circulate books to patrons of external libraries.
    """

    @classmethod
    def settings_class(cls) -> Type[ODLSettings]:
        return ODLSettings

    @classmethod
    def library_settings_class(cls) -> Type[ODLLibrarySettings]:
        return ODLLibrarySettings

    @classmethod
    def label(cls) -> str:
        return ExternalIntegration.ODL

    @classmethod
    def description(cls) -> str:
        return "Import books from a distributor that uses ODL (Open Distribution to Libraries)."


class ODLXMLParser(OPDSXMLParser):
    NAMESPACES = dict(OPDSXMLParser.NAMESPACES, odl="http://opds-spec.org/odl")


class BaseODLImporter(BaseOPDSImporter[SettingsType], ABC):
    FEEDBOOKS_AUDIO = "{}; protection={}".format(
        MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
        DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM,
    )

    CONTENT_TYPE = "content-type"
    DRM_SCHEME = "drm-scheme"

    LICENSE_FORMATS = {
        FEEDBOOKS_AUDIO: {
            CONTENT_TYPE: MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
            DRM_SCHEME: DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM,
        }
    }

    @classmethod
    def fetch_license_info(
        cls, document_link: str, do_get: Callable[..., Tuple[int, Any, bytes]]
    ) -> Optional[Dict[str, Any]]:
        status_code, _, response = do_get(document_link, headers={})
        if status_code in (200, 201):
            license_info_document = json.loads(response)
            return license_info_document  # type: ignore[no-any-return]
        else:
            cls.logger().warning(
                f"License Info Document is not available. "
                f"Status link {document_link} failed with {status_code} code."
            )
            return None

    @classmethod
    def parse_license_info(
        cls,
        license_info_document: Dict[str, Any],
        license_info_link: str,
        checkout_link: Optional[str],
    ) -> Optional[LicenseData]:
        """Check the license's attributes passed as parameters:
        - if they're correct, turn them into a LicenseData object
        - otherwise, return a None

        :param license_info_document: License Info Document
        :param license_info_link: Link to fetch License Info Document
        :param checkout_link: License's checkout link

        :return: LicenseData if all the license's attributes are correct, None, otherwise
        """

        identifier = license_info_document.get("identifier")
        document_status = license_info_document.get("status")
        document_checkouts = license_info_document.get("checkouts", {})
        document_left = document_checkouts.get("left")
        document_available = document_checkouts.get("available")
        document_terms = license_info_document.get("terms", {})
        document_expires = document_terms.get("expires")
        document_concurrency = document_terms.get("concurrency")
        document_format = license_info_document.get("format")

        if identifier is None:
            cls.logger().error("License info document has no identifier.")
            return None

        expires = None
        if document_expires is not None:
            expires = dateutil.parser.parse(document_expires)
            expires = util.datetime_helpers.to_utc(expires)

        if document_status is not None:
            status = LicenseStatus.get(document_status)
            if status.value != document_status:
                cls.logger().warning(
                    f"Identifier # {identifier} unknown status value "
                    f"{document_status} defaulting to {status.value}."
                )
        else:
            status = LicenseStatus.unavailable
            cls.logger().warning(
                f"Identifier # {identifier} license info document does not have "
                f"required key 'status'."
            )

        if document_available is not None:
            available = int(document_available)
        else:
            available = 0
            cls.logger().warning(
                f"Identifier # {identifier} license info document does not have "
                f"required key 'checkouts.available'."
            )

        left = None
        if document_left is not None:
            left = int(document_left)

        concurrency = None
        if document_concurrency is not None:
            concurrency = int(document_concurrency)

        content_types = None
        if document_format is not None:
            if isinstance(document_format, str):
                content_types = [document_format]
            elif isinstance(document_format, list):
                content_types = document_format

        return LicenseData(
            identifier=identifier,
            checkout_url=checkout_link,
            status_url=license_info_link,
            expires=expires,
            checkouts_left=left,
            checkouts_available=available,
            status=status,
            terms_concurrency=concurrency,
            content_types=content_types,
        )

    @classmethod
    def get_license_data(
        cls,
        license_info_link: str,
        checkout_link: Optional[str],
        feed_license_identifier: Optional[str],
        feed_license_expires: Optional[datetime.datetime],
        feed_concurrency: Optional[int],
        do_get: Callable[..., Tuple[int, Any, bytes]],
    ) -> Optional[LicenseData]:
        license_info_document = cls.fetch_license_info(license_info_link, do_get)

        if not license_info_document:
            return None

        parsed_license = cls.parse_license_info(
            license_info_document, license_info_link, checkout_link
        )

        if not parsed_license:
            return None

        if parsed_license.identifier != feed_license_identifier:
            # There is a mismatch between the license info document and
            # the feed we are importing. Since we don't know which to believe
            # we log an error and continue.
            cls.logger().error(
                f"Mismatch between license identifier in the feed ({feed_license_identifier}) "
                f"and the identifier in the license info document "
                f"({parsed_license.identifier}) ignoring license completely."
            )
            return None

        if parsed_license.expires != feed_license_expires:
            cls.logger().error(
                f"License identifier {feed_license_identifier}. Mismatch between license "
                f"expiry in the feed ({feed_license_expires}) and the expiry in the license "
                f"info document ({parsed_license.expires}) setting license status "
                f"to unavailable."
            )
            parsed_license.status = LicenseStatus.unavailable

        if parsed_license.terms_concurrency != feed_concurrency:
            cls.logger().error(
                f"License identifier {feed_license_identifier}. Mismatch between license "
                f"concurrency in the feed ({feed_concurrency}) and the "
                f"concurrency in the license info document ("
                f"{parsed_license.terms_concurrency}) setting license status "
                f"to unavailable."
            )
            parsed_license.status = LicenseStatus.unavailable

        return parsed_license


class ODLImporter(OPDSImporter, BaseODLImporter[ODLSettings]):
    """Import information and formats from an ODL feed.

    The only change from OPDSImporter is that this importer extracts
    format information from 'odl:license' tags.
    """

    NAME = ODLAPI.label()
    PARSER_CLASS = ODLXMLParser

    # The media type for a License Info Document, used to get information
    # about the license.
    LICENSE_INFO_DOCUMENT_MEDIA_TYPE = "application/vnd.odl.info+json"

    @classmethod
    def settings_class(cls) -> Type[ODLSettings]:
        return ODLSettings

    @classmethod
    def _detail_for_elementtree_entry(
        cls,
        parser: OPDSXMLParser,
        entry_tag: Element,
        feed_url: Optional[str] = None,
        do_get: Optional[Callable[..., Tuple[int, Any, bytes]]] = None,
    ) -> Dict[str, Any]:
        do_get = do_get or Representation.cautious_http_get

        # TODO: Review for consistency when updated ODL spec is ready.
        subtag = parser.text_of_optional_subtag
        data = OPDSImporter._detail_for_elementtree_entry(parser, entry_tag, feed_url)
        formats = []
        licenses = []

        odl_license_tags = parser._xpath(entry_tag, "odl:license") or []
        medium = None
        for odl_license_tag in odl_license_tags:
            identifier = subtag(odl_license_tag, "dcterms:identifier")
            full_content_type = subtag(odl_license_tag, "dcterms:format")

            if not medium:
                medium = Edition.medium_from_media_type(full_content_type)

            # By default, dcterms:format includes the media type of a
            # DRM-free resource.
            content_type = full_content_type
            drm_schemes: List[str | None] = []

            # But it may instead describe an audiobook protected with
            # the Feedbooks access-control scheme.
            if full_content_type == cls.FEEDBOOKS_AUDIO:
                content_type = MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE
                drm_schemes.append(DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM)

            # Additional DRM schemes may be described in <odl:protection>
            # tags.
            protection_tags = parser._xpath(odl_license_tag, "odl:protection") or []
            for protection_tag in protection_tags:
                drm_scheme = subtag(protection_tag, "dcterms:format")
                if drm_scheme:
                    drm_schemes.append(drm_scheme)

            for drm_scheme in drm_schemes or [None]:
                formats.append(
                    FormatData(
                        content_type=content_type,
                        drm_scheme=drm_scheme,
                        rights_uri=RightsStatus.IN_COPYRIGHT,
                    )
                )

            data["medium"] = medium

            checkout_link = None
            for link_tag in parser._xpath(odl_license_tag, "odl:tlink") or []:
                rel = link_tag.attrib.get("rel")
                if rel == Hyperlink.BORROW:
                    checkout_link = link_tag.attrib.get("href")
                    break

            # Look for a link to the License Info Document for this license.
            odl_status_link = None
            for link_tag in parser._xpath(odl_license_tag, "atom:link") or []:
                attrib = link_tag.attrib
                rel = attrib.get("rel")
                type = attrib.get("type", "")
                if rel == "self" and type.startswith(
                    cls.LICENSE_INFO_DOCUMENT_MEDIA_TYPE
                ):
                    odl_status_link = attrib.get("href")
                    break

            expires = None
            concurrent_checkouts = None

            terms = parser._xpath(odl_license_tag, "odl:terms")
            if terms:
                concurrent_checkouts = subtag(terms[0], "odl:concurrent_checkouts")
                expires = subtag(terms[0], "odl:expires")

            concurrent_checkouts_int = (
                int(concurrent_checkouts) if concurrent_checkouts is not None else None
            )
            expires_datetime = (
                to_utc(dateutil.parser.parse(expires)) if expires is not None else None
            )

            if not odl_status_link:
                parsed_license = None
            else:
                parsed_license = cls.get_license_data(
                    odl_status_link,
                    checkout_link,
                    identifier,
                    expires_datetime,
                    concurrent_checkouts_int,
                    do_get,
                )

            if parsed_license is not None:
                licenses.append(parsed_license)

        if not data.get("circulation"):
            data["circulation"] = dict()
        if not data["circulation"].get("formats"):
            data["circulation"]["formats"] = []
        data["circulation"]["formats"].extend(formats)
        if not data["circulation"].get("licenses"):
            data["circulation"]["licenses"] = []
        data["circulation"]["licenses"].extend(licenses)
        data["circulation"]["licenses_owned"] = None
        data["circulation"]["licenses_available"] = None
        data["circulation"]["licenses_reserved"] = None
        data["circulation"]["patrons_in_hold_queue"] = None
        return data


class ODLImportMonitor(OPDSImportMonitor):
    """Import information from an ODL feed."""

    PROTOCOL = ODLImporter.NAME
    SERVICE_NAME = "ODL Import Monitor"

    def __init__(
        self,
        _db: Session,
        collection: Collection,
        import_class: Type[OPDSImporter],
        **import_class_kwargs: Any,
    ):
        # Always force reimport ODL collections to get up to date license information
        super().__init__(
            _db, collection, import_class, force_reimport=True, **import_class_kwargs
        )


class ODLHoldReaper(CollectionMonitor):
    """Check for holds that have expired and delete them, and update
    the holds queues for their pools."""

    SERVICE_NAME = "ODL Hold Reaper"
    PROTOCOL = ODLAPI.label()

    def __init__(
        self,
        _db: Session,
        collection: Collection,
        api: Optional[ODLAPI] = None,
        **kwargs: Any,
    ):
        super().__init__(_db, collection, **kwargs)
        self.api = api or ODLAPI(_db, collection)

    def run_once(self, progress: TimestampData) -> TimestampData:
        # Find holds that have expired.
        expired_holds = (
            self._db.query(Hold)
            .join(Hold.license_pool)
            .filter(LicensePool.collection_id == self.api.collection_id)
            .filter(Hold.end < utc_now())
            .filter(Hold.position == 0)
        )

        changed_pools = set()
        total_deleted_holds = 0
        for hold in expired_holds:
            changed_pools.add(hold.license_pool)
            self._db.delete(hold)
            total_deleted_holds += 1

        for pool in changed_pools:
            self.api.update_licensepool(pool)

        message = "Holds deleted: %d. License pools updated: %d" % (
            total_deleted_holds,
            len(changed_pools),
        )
        progress = TimestampData(achievements=message)
        return progress
