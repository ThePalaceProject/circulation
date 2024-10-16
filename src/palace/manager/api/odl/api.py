from __future__ import annotations

import binascii
import datetime
import json
import uuid
from collections.abc import Callable
from functools import cached_property, partial
from typing import Any, Literal

from dependency_injector.wiring import Provide, inject
from flask import url_for
from pydantic import ValidationError
from sqlalchemy import or_
from sqlalchemy.orm import Session
from uritemplate import URITemplate

from palace.manager.api.circulation import (
    BaseCirculationAPI,
    DirectFulfillment,
    FetchFulfillment,
    Fulfillment,
    HoldInfo,
    LoanInfo,
    PatronActivityCirculationAPI,
    RedirectFulfillment,
    UrlFulfillment,
)
from palace.manager.api.circulation_exceptions import (
    AlreadyCheckedOut,
    AlreadyOnHold,
    CannotFulfill,
    CannotLoan,
    CannotReturn,
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
from palace.manager.api.odl.auth import OdlAuthenticatedRequest, OpdsWithOdlException
from palace.manager.api.odl.constants import FEEDBOOKS_AUDIO
from palace.manager.api.odl.settings import (
    OPDS2AuthType,
    OPDS2WithODLLibrarySettings,
    OPDS2WithODLSettings,
)
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.core.lcp.credential import LCPCredentialFactory
from palace.manager.opds.base import BaseLink
from palace.manager.opds.lcp.license import LicenseDocument
from palace.manager.opds.lcp.status import LoanStatus
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
from palace.manager.util.http import BadResponseException, RemoteIntegrationException


class OPDS2WithODLApi(
    OdlAuthenticatedRequest,
    PatronActivityCirculationAPI[OPDS2WithODLSettings, OPDS2WithODLLibrarySettings],
):
    """ODL (Open Distribution to Libraries) is a specification that allows
    libraries to manage their own loans and holds. It offers a deeper level
    of control to the library, but it requires the circulation manager to
    keep track of individual copies rather than just license pools, and
    manage its own holds queues.
    """

    SET_DELIVERY_MECHANISM_AT = BaseCirculationAPI.FULFILL_STEP

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

    def _request_loan_status(
        self, method: str, url: str, ignored_problem_types: list[str] | None = None
    ) -> LoanStatus:
        try:
            response = self._request(method, url, allowed_response_codes=["2xx"])
            status_doc = LoanStatus.model_validate_json(response.content)
        except ValidationError as e:
            self.log.exception(
                f"Error validating Loan Status Document. '{url}' returned and invalid document. {e}"
            )
            raise RemoteIntegrationException(
                url, "Loan Status Document not valid."
            ) from e
        except BadResponseException as e:
            response = e.response
            error_message = f"Error requesting Loan Status Document. '{url}' returned status code {response.status_code}."
            if isinstance(e, OpdsWithOdlException):
                # It this problem type is explicitly ignored, we just raise the exception instead of proceeding with
                # logging the information about it. The caller will handle the exception.
                if ignored_problem_types and e.type in ignored_problem_types:
                    raise
                error_message += f" Problem Detail: '{e.type}' - {e.title}"
                if e.detail:
                    error_message += f" - {e.detail}"
            else:
                header_string = ", ".join(
                    {f"{k}: {v}" for k, v in response.headers.items()}
                )
                response_string = (
                    response.text
                    if len(response.text) < 100
                    else response.text[:100] + "..."
                )
                error_message += f" Response headers: {header_string}. Response content: {response_string}."
            self.log.exception(error_message)
            raise

        return status_doc

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
        if loan.external_identifier is None:
            # We can't return a loan that doesn't have an external identifier. This should never happen
            # but if it does, we log an error and delete the loan, so it doesn't stay on the patrons
            # bookshelf forever.
            self.log.error(f"Loan {loan.id} has no external identifier.")
            return False

        doc = self._request_loan_status("GET", loan.external_identifier)
        if not doc.active:
            self.log.warning(
                f"Loan {loan.id} was already returned early, revoked by the distributor, or it expired."
            )
            self.update_loan(loan, doc)
            return False

        return_link = doc.links.get(rel="return", type=LoanStatus.content_type())
        if not return_link:
            # The distributor didn't provide a link to return this loan. This means that the distributor
            # does not support early returns, and the patron will have to wait until the loan expires.
            raise CannotReturn()

        # The parameters for this link (if its templated) are defined here:
        # https://readium.org/lcp-specs/releases/lsd/latest.html#34-returning-a-publication
        # None of them are required, and often the link is not templated. But in the case
        # of the open source LCP server, the link is templated, so we need to process the
        # template before we can make the request.
        return_url = return_link.href_templated({"name": "Palace Manager"})

        # Hit the distributor's return link, and if it's successful, update the pool
        # availability and delete the local loan.
        doc = self._request_loan_status("PUT", return_url)
        if doc.active:
            # If the distributor says the loan is still active, we didn't return it, and
            # something went wrong. We log an error and don't delete the loan, so the patron
            # can try again later.
            self.log.error(
                f"Loan {loan.id} was not returned. The distributor says it's still active. {doc.model_dump_json()}"
            )
            raise CannotReturn()
        self.update_loan(loan, doc)
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

        return LoanInfo.from_license_pool(
            licensepool,
            start_date=loan_start,
            end_date=loan_end,
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

        identifier = loan.license.identifier
        checkout_id = str(uuid.uuid4())
        if self.collection is None:
            raise PalaceValueError(f"Collection not found: {self.collection_id}")
        default_loan_period = self.collection.default_loan_period(loan.patron.library)

        requested_expiry = utc_now() + datetime.timedelta(days=default_loan_period)
        patron_id = patron.identifier_to_remote_service(licensepool.data_source)
        library_short_name = loan.patron.library.short_name

        db = Session.object_session(loan)
        patron = loan.patron
        hasher = self._get_hasher()

        unhashed_pass = self._credential_factory.get_patron_passphrase(db, patron)
        hashed_pass = unhashed_pass.hash(hasher)
        self._credential_factory.set_hashed_passphrase(db, patron, hashed_pass)
        encoded_pass = base64.b64encode(binascii.unhexlify(hashed_pass.hashed))

        notification_url = self._url_for(
            "odl_notify",
            library_short_name=library_short_name,
            loan_id=loan.id,
            _external=True,
        )

        # We should never be able to get here if the license doesn't have a checkout_url, but
        # we assert it anyway, to be sure we fail fast if it happens.
        assert license.checkout_url is not None
        url_template = URITemplate(license.checkout_url)
        checkout_url = url_template.expand(
            id=str(identifier),
            checkout_id=checkout_id,
            patron_id=patron_id,
            expires=requested_expiry.isoformat(),
            notification_url=notification_url,
            passphrase=encoded_pass,
            hint=self.settings.passphrase_hint,
            hint_url=self.settings.passphrase_hint_url,
        )

        try:
            doc = self._request_loan_status(
                "POST",
                checkout_url,
                ignored_problem_types=[
                    "http://opds-spec.org/odl/error/checkout/unavailable"
                ],
            )
        except BadResponseException as e:
            _db.delete(loan)
            if isinstance(e, OpdsWithOdlException):
                if e.type == "http://opds-spec.org/odl/error/checkout/unavailable":
                    # TODO: This would be a good place to do an async availability update, since we know
                    #   the book is unavailable, when we thought it was available. For now, we know that
                    #   the license has no checkouts_available, so we do that update.
                    license.checkouts_available = 0
                    licensepool.update_availability_from_licenses()
                    raise NoAvailableCopies()
            raise

        if not doc.active:
            # Something went wrong with this loan and we don't actually
            # have the book checked out. This should never happen.
            # Remove the loan we created.
            _db.delete(loan)
            raise CannotLoan()

        # We save the link to the loan status document in the loan's external_identifier field, so
        # we are able to retrieve it later.
        loan_status_document_link: BaseLink | None = doc.links.get(
            rel="self", type=LoanStatus.content_type()
        )

        # The ODL spec requires that a 'self' link be present in the links section of the response.
        # See: https://drafts.opds.io/odl-1.0.html#54-interacting-with-a-checkout-link
        # However, the open source LCP license status server does not provide this link, so we make
        # an extra request to try to get the information we need from the 'status' link in the license
        # document, which the LCP server does provide.
        # TODO: Raise this issue with LCP server maintainers, and try to get a fix in place.
        #   once that is done, we should be able to remove this fallback.
        if not loan_status_document_link:
            license_document_link = doc.links.get(
                rel="license", type=LicenseDocument.content_type()
            )
            if license_document_link:
                response = self._request(
                    "GET", license_document_link.href, allowed_response_codes=["2xx"]
                )
                license_doc = LicenseDocument.model_validate_json(response.content)
                loan_status_document_link = license_doc.links.get(
                    rel="status", type=LoanStatus.content_type()
                )

        if not loan_status_document_link:
            _db.delete(loan)
            raise CannotLoan()

        # We need to set the start and end dates on our local loan since
        # the code that calls this only sets them when a new loan is created.
        loan.start = utc_now()
        loan.end = doc.potential_rights.end
        loan.external_identifier = loan_status_document_link.href

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
    ) -> Fulfillment:
        """Get the actual resource file to the patron."""
        _db = Session.object_session(patron)

        loan = (
            _db.query(Loan)
            .filter(Loan.patron == patron)
            .filter(Loan.license_pool_id == licensepool.id)
        ).one()
        return self._fulfill(loan, delivery_mechanism)

    @staticmethod
    def _check_delivery_mechanism_available(
        requested_delivery_mechanism: DeliveryMechanism, licensepool: LicensePool
    ) -> None:
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

    def _unlimited_access_fulfill(
        self, loan: Loan, delivery_mechanism: LicensePoolDeliveryMechanism
    ) -> Fulfillment:
        licensepool = loan.license_pool
        self._check_delivery_mechanism_available(
            delivery_mechanism.delivery_mechanism, licensepool
        )
        content_link = delivery_mechanism.resource.representation.public_url
        content_type = delivery_mechanism.resource.representation.media_type
        return RedirectFulfillment(content_link, content_type)

    def _license_fulfill(
        self, loan: Loan, delivery_mechanism: LicensePoolDeliveryMechanism
    ) -> Fulfillment:
        # We are unable to fulfill a loan that doesn't have its external identifier set,
        # since we use this to get to the checkout link. It shouldn't be possible to get
        # into this state.
        license_status_url = loan.external_identifier
        assert license_status_url is not None

        doc = self._request_loan_status("GET", license_status_url)

        if not doc.active:
            # This loan isn't available for some reason. It's possible
            # the distributor revoked it or the patron already returned it
            # through the DRM system, and we didn't get a notification
            # from the distributor yet.
            self.update_loan(loan, doc)
            raise CannotFulfill()

        drm_scheme = delivery_mechanism.delivery_mechanism.drm_scheme
        fulfill_cls: Callable[[str, str | None], UrlFulfillment]
        if drm_scheme == DeliveryMechanism.NO_DRM:
            # If we have no DRM, we can just redirect to the content link and let the patron download the book.
            fulfill_link = doc.links.get(
                rel="publication",
                type=delivery_mechanism.delivery_mechanism.content_type,
            )
            fulfill_cls = RedirectFulfillment
        elif drm_scheme == DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM:
            # For DeMarque audiobook content using "FEEDBOOKS_AUDIOBOOK_DRM", the link
            # we are looking for is stored in the 'manifest' rel.
            fulfill_link = doc.links.get(rel="manifest", type=FEEDBOOKS_AUDIO)
            fulfill_cls = partial(FetchFulfillment, allowed_response_codes=["2xx"])
        else:
            # We are getting content via a license document, so we need to find the link
            # that corresponds to the delivery mechanism we are using.
            fulfill_link = doc.links.get(rel="license", type=drm_scheme)
            fulfill_cls = partial(FetchFulfillment, allowed_response_codes=["2xx"])

        if fulfill_link is None:
            raise CannotFulfill()

        return fulfill_cls(fulfill_link.href, fulfill_link.type)

    def _bearer_token_fulfill(
        self, loan: Loan, delivery_mechanism: LicensePoolDeliveryMechanism
    ) -> Fulfillment:
        licensepool = loan.license_pool
        self._check_delivery_mechanism_available(
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
            location=delivery_mechanism.resource.url,
        )

        return DirectFulfillment(
            content_type=DeliveryMechanism.BEARER_TOKEN,
            content=json.dumps(token_document),
        )

    def _fulfill(
        self,
        loan: Loan,
        delivery_mechanism: LicensePoolDeliveryMechanism,
    ) -> Fulfillment:
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
            return self._license_fulfill(loan, delivery_mechanism)

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
        holdinfo = HoldInfo.from_license_pool(
            pool,
            start_date=hold.start,
            end_date=hold.end,
            hold_position=hold.position,
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

        _db.expire_all()

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
        holdinfo = HoldInfo.from_license_pool(
            licensepool,
            start_date=utc_now(),
            hold_position=0,
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
            LoanInfo.from_license_pool(
                loan.license_pool,
                start_date=loan.start,
                end_date=loan.end,
                external_identifier=loan.external_identifier,
            )
            for loan in loans
        ] + [
            HoldInfo.from_license_pool(
                hold.license_pool,
                start_date=hold.start,
                end_date=hold.end,
                hold_position=hold.position,
            )
            for hold in remaining_holds
        ]

    def update_loan(self, loan: Loan, status_doc: LoanStatus) -> None:
        """Check a loan's status, and if it is no longer active, delete the loan
        and update its pool's availability.
        """
        _db = Session.object_session(loan)

        if not status_doc.active:
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
