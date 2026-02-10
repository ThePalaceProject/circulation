from __future__ import annotations

import binascii
import datetime
import json
import uuid
from collections.abc import Callable
from functools import partial
from typing import Unpack

from celery.canvas import Signature
from flask import url_for
from pydantic import ValidationError
from sqlalchemy.orm import Session
from uritemplate import URITemplate

from palace.manager.api.circulation.base import BaseCirculationAPI
from palace.manager.api.circulation.data import HoldInfo, LoanInfo
from palace.manager.api.circulation.exceptions import (
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
from palace.manager.api.circulation.fulfillment import (
    DirectFulfillment,
    FetchFulfillment,
    Fulfillment,
    RedirectFulfillment,
    StreamingFulfillment,
    UrlFulfillment,
)
from palace.manager.api.lcp.hash import Hasher, HasherFactory
from palace.manager.core.lcp.credential import LCPCredentialFactory
from palace.manager.integration.license.opds.exception import OpdsResponseException
from palace.manager.integration.license.opds.odl.constants import FEEDBOOKS_AUDIO
from palace.manager.integration.license.opds.odl.demarque import (
    DEMARQUE_WEBREADER_REL,
    DeMarqueWebReader,
)
from palace.manager.integration.license.opds.odl.settings import (
    OPDS2WithODLLibrarySettings,
    OPDS2WithODLSettings,
)
from palace.manager.integration.license.opds.requests import (
    OAuthOpdsRequest,
    get_opds_requests,
)
from palace.manager.integration.license.opds.settings.format_priority import (
    FormatPriorities,
)
from palace.manager.opds.lcp.license import LicenseDocument
from palace.manager.opds.lcp.status import LoanStatus
from palace.manager.opds.types.link import BaseLink
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    License,
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.patron import Hold, Loan, Patron
from palace.manager.sqlalchemy.model.resource import Resource
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util import base64
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.http.exception import (
    BadResponseException,
    RemoteIntegrationException,
)


class OPDS2WithODLApi(
    BaseCirculationAPI[OPDS2WithODLSettings, OPDS2WithODLLibrarySettings],
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

    def __init__(
        self,
        _db: Session,
        collection: Collection,
        demarque_webreader: DeMarqueWebReader | None = None,
    ) -> None:
        super().__init__(_db, collection)

        if collection.protocol != self.label():
            raise ValueError(
                "Collection protocol is %s, but passed into %s!"
                % (collection.protocol, self.__class__.__name__)
            )
        self.collection_id = collection.id

        self._hasher_factory = HasherFactory()
        self._credential_factory = LCPCredentialFactory()
        self._hasher_instance: Hasher | None = None

        self.loan_limit = self.settings.loan_limit
        self.hold_limit = self.settings.hold_limit
        self._request = get_opds_requests(
            self.settings.auth_type,
            self.settings.username,
            self.settings.password,
            self.settings.external_account_id,
        )
        self._format_priorities = FormatPriorities(
            self.settings.prioritized_drm_schemes,
            self.settings.prioritized_content_types,
        )

        # Create DeMarque WebReader client (None if not configured)
        self._demarque_webreader = demarque_webreader or DeMarqueWebReader.create()

        # Create the data source for this collection if it doesn't exist.
        _ = self.data_source

    @property
    def data_source(self) -> DataSource:
        return DataSource.lookup(self._db, self.settings.data_source, autocreate=True)

    def _get_hasher(self) -> Hasher:
        """Returns a Hasher instance

        :return: Hasher instance
        """
        if self._hasher_instance is None:
            self._hasher_instance = self._hasher_factory.create(
                self.settings.encryption_algorithm
            )

        return self._hasher_instance

    @staticmethod
    def _notification_url(
        short_name: str | None, patron_id: str, license_id: str
    ) -> str:
        """Get the notification URL that should be passed in the ODL checkout link.

        This is broken out into a separate function to make it easier to override
        in tests.
        """
        return url_for(
            "opds2_with_odl_notification",
            library_short_name=short_name,
            patron_identifier=patron_id,
            license_identifier=license_id,
            _external=True,
        )

    def _request_loan_status(
        self, method: str, url: str, ignored_problem_types: list[str] | None = None
    ) -> LoanStatus:
        try:
            status_doc = self._request(
                method,
                url,
                parser=LoanStatus.model_validate_json,
                allowed_response_codes=["2xx"],
            )
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
            if isinstance(e, OpdsResponseException):
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

        if licensepool.unlimited_type:
            # If this is an unlimited access book, we don't need to do anything.
            return

        self._checkin(loan_result)

    def _checkin(self, loan: Loan) -> None:
        _db = Session.object_session(loan)
        if loan.external_identifier is None:
            # We can't return a loan that doesn't have an external identifier. This should never happen
            # but if it does, we log an error and continue on, so it doesn't stay on the patrons
            # bookshelf forever.
            self.log.error(f"Loan {loan.id} has no external identifier.")
            return
        if loan.license is None:
            # We can't return a loan that doesn't have a license. This should never happen but if it does,
            # we log an error and continue on, so it doesn't stay on the patrons bookshelf forever.
            self.log.error(f"Loan {loan.id} has no license.")
            return

        doc = self._request_loan_status("GET", loan.external_identifier)
        if not doc.active:
            self.log.warning(
                f"Loan {loan.id} was already returned early, revoked by the distributor, or it expired."
            )
            loan.license.checkin()
            loan.license_pool.update_availability_from_licenses()
            return

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
        # availability.
        doc = self._request_loan_status("PUT", return_url)
        if doc.active:
            # If the distributor says the loan is still active, we didn't return it, and
            # something went wrong. We log an error and don't delete the loan, so the patron
            # can try again later.
            self.log.error(
                f"Loan {loan.id} was not returned. The distributor says it's still active. {doc.model_dump_json()}"
            )
            raise CannotReturn()
        loan.license.checkin()
        loan.license_pool.update_availability_from_licenses()

    def checkout(
        self,
        patron: Patron,
        pin: str | None,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism | None,
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

        if licensepool.unlimited_type:
            return LoanInfo.from_license_pool(
                licensepool,
                end_date=None,
            )
        else:
            hold = get_one(_db, Hold, patron=patron, license_pool_id=licensepool.id)
            return self._checkout(patron, licensepool, hold)

    def _checkout(
        self, patron: Patron, licensepool: LicensePool, hold: Hold | None = None
    ) -> LoanInfo:
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

        db = Session.object_session(patron)

        if not any(l for l in licensepool.licenses if not l.is_inactive):
            raise NoLicenses()

        # Make sure pool info is updated.
        # Update the pool and the next holds in the queue when a license is reserved.
        licensepool.update_availability_from_licenses()

        # If there's a holds queue, the patron must have a non-expired hold
        # with position 0 to check out the book.
        if (
            not hold
            or (hold.position and hold.position > 0)
            or (hold.end and hold.end < utc_now())
        ) and licensepool.licenses_available < 1:
            raise NoAvailableCopies()

        default_loan_period = self.collection.default_loan_period(patron.library)
        requested_expiry = utc_now() + datetime.timedelta(days=default_loan_period)
        patron_id = patron.identifier_to_remote_service(licensepool.data_source)
        library_short_name = patron.library.short_name
        hasher = self._get_hasher()
        unhashed_pass = self._credential_factory.get_patron_passphrase(db, patron)
        hashed_pass = unhashed_pass.hash(hasher)
        self._credential_factory.set_hashed_passphrase(db, patron, hashed_pass)
        encoded_pass = base64.b64encode(binascii.unhexlify(hashed_pass.hashed))

        licenses = licensepool.best_available_licenses()

        license_: License | None = None
        loan_status: LoanStatus | None = None
        for license_ in licenses:
            try:
                loan_status = self._checkout_license(
                    license_,
                    library_short_name,
                    patron_id,
                    requested_expiry.isoformat(),
                    encoded_pass,
                )
                break
            except NoAvailableCopies:
                # This license had no available copies, so we try the next one.
                ...

        if license_ is None or loan_status is None:
            if hold:
                # If we have a hold, it means we thought the book was available, but it wasn't.
                # So we need to update its position in the hold queue. We will put it at position
                # 1, since the patron should be first in line. This may mean that there are two
                # patrons in position 1 in the hold queue, but this will be resolved next time
                # the hold queue is recalculated.
                hold.position = 1
                hold.end = None
            licensepool.update_availability_from_licenses()
            raise NoAvailableCopies()

        if not loan_status.active:
            # Something went wrong with this loan, and we don't actually
            # have the book checked out. This should never happen.
            raise CannotLoan()

        # We save the link to the loan status document in the loan's external_identifier field, so
        # we are able to retrieve it later.
        loan_status_document_link: BaseLink | None = loan_status.links.get(
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
            license_document_link = loan_status.links.get(
                rel="license", type=LicenseDocument.content_type()
            )
            if license_document_link:
                license_doc = self._request(
                    "GET",
                    license_document_link.href,
                    parser=LicenseDocument.model_validate_json,
                    allowed_response_codes=["2xx"],
                )
                loan_status_document_link = license_doc.links.get(
                    rel="status", type=LoanStatus.content_type()
                )

        if not loan_status_document_link:
            raise CannotLoan()

        loan = LoanInfo.from_license_pool(
            licensepool,
            end_date=loan_status.potential_rights.end,
            external_identifier=loan_status_document_link.href,
            license_identifier=license_.identifier,
        )

        # We also need to update the remaining checkouts for the license.
        license_.checkout()

        # If there was a hold CirculationAPI will take care of deleting it. So we just need to
        # update the license pool to reflect the loan. Since update_availability_from_licenses
        # takes into account holds, we need to tell it to ignore the hold about to be deleted.
        licensepool.update_availability_from_licenses(
            ignored_holds={hold} if hold else None
        )
        return loan

    def _checkout_license(
        self,
        license_: License,
        library_short_name: str | None,
        patron_id: str,
        expiry: str,
        encoded_pass: str,
    ) -> LoanStatus:
        identifier = str(license_.identifier)
        checkout_id = str(uuid.uuid4())

        notification_url = self._notification_url(
            library_short_name,
            patron_id,
            identifier,
        )

        # We should never be able to get here if the license doesn't have a checkout_url, but
        # we assert it anyway, to be sure we fail fast if it happens.
        assert license_.checkout_url is not None
        url_template = URITemplate(license_.checkout_url)
        checkout_url = url_template.expand(
            id=identifier,
            checkout_id=checkout_id,
            patron_id=patron_id,
            expires=expiry,
            notification_url=notification_url,
            passphrase=encoded_pass,
            hint=self.settings.passphrase_hint,
            hint_url=self.settings.passphrase_hint_url,
        )

        try:
            return self._request_loan_status(
                "POST",
                checkout_url,
                ignored_problem_types=[
                    "http://opds-spec.org/odl/error/checkout/unavailable"
                ],
            )
        except OpdsResponseException as e:
            if e.type == "http://opds-spec.org/odl/error/checkout/unavailable":
                # TODO: This would be a good place to do an async availability update, since we know
                #   the book is unavailable, when we thought it was available. For now, we know that
                #   the license has no checkouts_available, so we do that update.
                license_.checkouts_available = 0
                raise NoAvailableCopies() from e
            raise

    def fulfill(
        self,
        patron: Patron,
        pin: str,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism,
        **kwargs: Unpack[BaseCirculationAPI.FulfillKwargs],
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
    def _get_resource_for_delivery_mechanism(
        requested_delivery_mechanism: DeliveryMechanism, licensepool: LicensePool
    ) -> Resource:
        resource = next(
            (
                lpdm.resource
                for lpdm in licensepool.available_delivery_mechanisms
                if lpdm.delivery_mechanism == requested_delivery_mechanism
                and lpdm.resource is not None
            ),
            None,
        )
        if resource is None:
            raise FormatNotAvailable()
        return resource

    def _unlimited_access_fulfill(
        self, loan: Loan, delivery_mechanism: LicensePoolDeliveryMechanism
    ) -> Fulfillment:
        licensepool = loan.license_pool
        resource = self._get_resource_for_delivery_mechanism(
            delivery_mechanism.delivery_mechanism, licensepool
        )
        if resource.representation is None:
            raise FormatNotAvailable()
        content_link = resource.representation.public_url
        content_type = resource.representation.media_type
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
            db = Session.object_session(loan)
            db.delete(loan)
            raise CannotFulfill()

        content_type = delivery_mechanism.delivery_mechanism.content_type
        drm_scheme = delivery_mechanism.delivery_mechanism.drm_scheme
        fulfill_cls: Callable[[str, str | None], UrlFulfillment]
        if drm_scheme == DeliveryMechanism.NO_DRM:
            # If we have no DRM, we can just redirect to the content link and let the patron download the book.
            fulfill_link = doc.links.get(
                rel="publication",
                type=content_type,
            )
            fulfill_cls = RedirectFulfillment
        elif drm_scheme == DeliveryMechanism.STREAMING_DRM:
            # Streaming DRM returns an OPDS entry containing the streaming reader link.
            link_content_type = (
                DeliveryMechanism.MEDIA_TYPES_FOR_STREAMING.get(content_type)
                if content_type
                else None
            )
            if link_content_type is None:
                self.log.error(
                    f"Unsupported streaming content type: {content_type!r}. "
                    f"Supported types: {list(DeliveryMechanism.MEDIA_TYPES_FOR_STREAMING.keys())}."
                )
                raise CannotFulfill("The requested streaming format is not available.")

            # Check for DeMarque WebReader link first (requires JWT config)
            if (
                self._demarque_webreader is not None
                and (
                    webreader_link := doc.links.get(
                        rel=DEMARQUE_WEBREADER_REL, type=link_content_type
                    )
                )
                is not None
            ):
                fulfill_link = self._demarque_webreader.fulfill_link(webreader_link)
            else:
                fulfill_link = doc.links.get(
                    rel="publication",
                    type=link_content_type,
                )
            fulfill_cls = StreamingFulfillment
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
        make_request = self._request
        if not isinstance(make_request, OAuthOpdsRequest):
            raise CannotFulfill(
                debug_info="Bearer token fulfillment is not configured for this integration."
            )

        licensepool = loan.license_pool
        resource = self._get_resource_for_delivery_mechanism(
            delivery_mechanism.delivery_mechanism, licensepool
        )

        # Make sure we have a session token to pass to the app. If the token expires in the
        # next 10 minutes, we'll refresh it to make sure the app has enough time to download the book.
        token = make_request.session_token
        if token is None or token.expires - datetime.timedelta(minutes=10) < utc_now():
            token = make_request.refresh_token()

        # Build an application/vnd.librarysimplified.bearer-token
        # document using information from the credential.
        token_document = dict(
            token_type="Bearer",
            access_token=token.access_token,
            expires_in=(int((token.expires - utc_now()).total_seconds())),
            location=resource.url,
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
        if loan.license_pool.unlimited_type:
            if (
                delivery_mechanism.delivery_mechanism.drm_scheme
                == DeliveryMechanism.BEARER_TOKEN
            ):
                return self._bearer_token_fulfill(loan, delivery_mechanism)
            else:
                return self._unlimited_access_fulfill(loan, delivery_mechanism)
        else:
            return self._license_fulfill(loan, delivery_mechanism)

    def place_hold(
        self,
        patron: Patron,
        pin: str | None,
        licensepool: LicensePool,
        notification_email_address: str | None,
    ) -> HoldInfo:
        """Create a new hold."""
        if licensepool.unlimited_type:
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
        # Update the pool and the next holds in the queue when a license is reserved.
        licensepool.update_availability_from_licenses()

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

        # This potentially has a race condition, if two web workers are creating a hold on the
        # licensepool at the same time, then patrons_in_hold_queue may be inaccurate. This is
        # fine, as the number is mostly informational and its regularly recalculated by the
        # recalculate_hold_queue_collection celery task. So the number will be accurate soon
        # enough.
        patrons_in_hold_queue = (
            licensepool.patrons_in_hold_queue
            if licensepool.patrons_in_hold_queue
            else 0
        )
        licensepool.patrons_in_hold_queue = patrons_in_hold_queue + 1
        holdinfo = HoldInfo.from_license_pool(
            licensepool,
            start_date=utc_now(),
            hold_position=licensepool.patrons_in_hold_queue,
        )

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

        # The hold itself will be deleted by the caller (usually CirculationAPI),
        # so we just need to update the license pool to reflect the released hold.
        # Since we are calling this before the hold is deleted, we need to pass the
        # hold as an ignored hold to get the correct count.
        hold.license_pool.update_availability_from_licenses(ignored_holds={hold})

    def update_availability(self, licensepool: LicensePool) -> None:
        pass

    def can_fulfill_without_loan(
        self,
        patron: Patron | None,
        pool: LicensePool,
        lpdm: LicensePoolDeliveryMechanism,
    ) -> bool:
        return False

    def sort_delivery_mechanisms(
        self, lpdms: list[LicensePoolDeliveryMechanism]
    ) -> list[LicensePoolDeliveryMechanism]:
        return self._format_priorities.prioritize_mechanisms(lpdms)

    @classmethod
    def import_task(cls, collection_id: int, force: bool = False) -> Signature:
        from palace.manager.celery.tasks.opds_odl import import_collection

        return import_collection.s(collection_id, force=force)
