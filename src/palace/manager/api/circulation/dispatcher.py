from __future__ import annotations

import datetime
import logging
from collections.abc import Mapping
from typing import Literal, Unpack

import flask
from flask_babel import lazy_gettext as _
from sqlalchemy.orm import Session

from palace.manager.api.circulation.base import (
    BaseCirculationAPI,
    CirculationApiType,
    PatronActivityCirculationAPI,
)
from palace.manager.api.circulation.data import HoldInfo, LoanInfo
from palace.manager.api.circulation.exceptions import (
    AlreadyCheckedOut,
    AlreadyOnHold,
    CannotFulfill,
    CannotRenew,
    CannotReturn,
    CurrentlyAvailable,
    DeliveryMechanismConflict,
    DeliveryMechanismMissing,
    NoAcceptableFormat,
    NoActiveLoan,
    NoAvailableCopies,
    NoLicenses,
    NotCheckedOut,
    NotOnHold,
    PatronHoldLimitReached,
    PatronLoanLimitReached,
)
from palace.manager.api.circulation.fulfillment import Fulfillment
from palace.manager.api.util.flask import get_request_library
from palace.manager.api.util.patron import PatronUtility
from palace.manager.service.analytics.analytics import Analytics
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import (
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.patron import Hold, Loan, Patron
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.log import LoggerMixin


class CirculationApiDispatcher(LoggerMixin):
    """Implement basic circulation logic and abstract away the details
    between different circulation APIs behind generic operations like
    'borrow'.
    """

    def __init__(
        self,
        db: Session,
        library: Library,
        library_collection_apis: Mapping[int | None, CirculationApiType],
        analytics: Analytics | None = None,
    ):
        """Constructor.

        :param db: A database session (probably a scoped session, which is
            why we can't derive it from `library`).

        :param library: A Library object representing the library
          whose circulation we're concerned with.

        :param analytics: An Analytics object for tracking
          circulation events.

        :param registry: An IntegrationRegistry mapping Collection protocols to
           API classes that should be instantiated to deal with these
           protocols. The default registry will work fine unless you're a
           unit test.

           Since instantiating these API classes may result in API
           calls, we only instantiate one CirculationAPI per library,
           and keep them around as long as possible.
        """
        self._db = db
        self.library_id = library.id
        self.analytics = analytics

        # Each of the Library's relevant Collections is going to be
        # associated with an API object.
        self.api_for_collection = library_collection_apis

    @property
    def library(self) -> Library | None:
        return Library.by_id(self._db, self.library_id)

    def api_for_license_pool(
        self, licensepool: LicensePool
    ) -> CirculationApiType | None:
        """Find the API to use for the given license pool."""
        return self.api_for_collection.get(licensepool.collection.id)

    def can_revoke_hold(self, licensepool: LicensePool, hold: Hold) -> bool:
        """Some circulation providers allow you to cancel a hold
        when the book is reserved to you. Others only allow you to cancel
        a hold while you're in the hold queue.
        """
        if hold.position is None or hold.position > 0:
            return True
        api = self.api_for_license_pool(licensepool)
        if api and api.CAN_REVOKE_HOLD_WHEN_RESERVED:
            return True
        return False

    def _collect_event(
        self,
        patron: Patron | None,
        licensepool: LicensePool | None,
        name: str,
    ) -> None:
        """Collect an analytics event.

        :param patron: The Patron associated with the event. If this
            is not specified, the current request's authenticated
            patron will be used.
        :param licensepool: The LicensePool associated with the event.
        :param name: The name of the event.
        """
        if not self.analytics:
            return

        # It would be really useful to know which patron caused
        # this event -- this will help us get a library
        if flask.request:
            request_patron = getattr(flask.request, "patron", None)
        else:
            request_patron = None
        patron = patron or request_patron

        # We need to figure out which library is associated with
        # this circulation event.
        if patron:
            # The library of the patron who caused the event.
            library = patron.library
        else:
            # The library associated with the current request, defaulting to
            # the library associated with the CirculationAPI itself if we are
            # outside a request context, or if the request context does not
            # have a library associated with it.
            library = get_request_library(default=self.library)

        self.analytics.collect_event(
            library,
            licensepool,
            name,
            patron=patron,
        )

    def _collect_checkout_event(self, patron: Patron, licensepool: LicensePool) -> None:
        """A simple wrapper around _collect_event for handling checkouts.

        This is called in two different places -- one when loaning
        licensed books and one when 'loaning' open-access books.
        """
        return self._collect_event(patron, licensepool, CirculationEvent.CM_CHECKOUT)

    def borrow(
        self,
        patron: Patron,
        pin: str | None,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism | None,
        hold_notification_email: str | None = None,
    ) -> tuple[Loan | None, Hold | None, bool]:
        """Either borrow a book or put it on hold. Don't worry about fulfilling
        the loan yet.

        :return: A 3-tuple (`Loan`, `Hold`, `is_new`). Either `Loan`
            or `Hold` must be None, but not both.
        """
        # Short-circuit the request if the patron lacks borrowing
        # privileges. This can happen for a few different reasons --
        # fines, blocks, expired card, etc.
        PatronUtility.assert_borrowing_privileges(patron)

        now = utc_now()
        api = self.api_for_license_pool(licensepool)

        # Okay, it's not an open-access book. This means we need to go
        # to an external service to get the book.

        if not api:
            # If there's no API for the pool, the pool is probably associated
            # with a collection that this library doesn't have access to.
            raise NoLicenses()

        if licensepool.unlimited_type and not licensepool.active_status:
            # The license pool is of unlimited type but is not currently active.
            # This means that although there are no limits on the number of
            # simultaneous users, the licensepool is not currently available
            # for borrowing for some reason.
            raise NoLicenses()

        must_set_delivery_mechanism = (
            api.SET_DELIVERY_MECHANISM_AT == BaseCirculationAPI.BORROW_STEP
        )

        if must_set_delivery_mechanism and not delivery_mechanism:
            raise DeliveryMechanismMissing()

        # Do we (think we) already have this book out on loan?
        existing_loan = get_one(
            self._db,
            Loan,
            patron=patron,
            license_pool=licensepool,
            on_multiple="interchangeable",
        )

        loan_info = None
        hold_info = None
        new_loan = False

        # Some exceptions may be raised during the borrow process even
        # if the book is not actually available for loan.  In those
        # cases, we will store the exception here and try to place the
        # book on hold. If the hold placement succeeds, there's no
        # problem. If the hold placement fails because the book is
        # actually available, it's better to raise this exception than
        # one that says "you tried to place a currently available book
        # on hold" -- that's probably not what the patron actually
        # tried to do.
        loan_exception = None

        # Enforce any library-specific limits on loans or holds.
        self.enforce_limits(patron, licensepool)

        # Since that didn't raise an exception, we don't know of any
        # reason why the patron shouldn't be able to get a loan or a
        # hold. There are race conditions that will allow someone to
        # get a hold in excess of their hold limit (because we thought
        # they were getting a loan but someone else checked out the
        # book right before we got to it) but they're rare and not
        # serious. There are also vendor-side restrictions that may
        # impose additional limits on patron activity, but that will
        # just result in exceptions being raised later in this method
        # rather than in enforce_limits.

        # We try to check out the book even if we believe it's not
        # available -- someone else may have checked it in since we
        # last looked.
        try:
            checkout_result = api.checkout(
                patron, pin, licensepool, delivery_mechanism=delivery_mechanism
            )

            if isinstance(checkout_result, HoldInfo):
                # If the API couldn't give us a loan, it may have given us
                # a hold instead of raising an exception.
                hold_info = checkout_result
                loan_info = None
            else:
                # We asked the API to create a loan and it gave us a
                # LoanInfo object, rather than raising an exception like
                # AlreadyCheckedOut.
                #
                # For record-keeping purposes we're going to treat this as
                # a newly transacted loan, although it's possible that the
                # API does something unusual like return LoanInfo instead
                # of raising AlreadyCheckedOut.
                new_loan = True
                loan_info = checkout_result
                hold_info = None
        except AlreadyCheckedOut:
            # This is good, but we didn't get the real loan info.
            # Just fake it.
            loan_info = LoanInfo.from_license_pool(
                licensepool,
                start_date=None,
                end_date=now + datetime.timedelta(hours=1),
                external_identifier=(
                    existing_loan.external_identifier if existing_loan else None
                ),
            )
        except AlreadyOnHold:
            # We're trying to check out a book that we already have on hold.
            hold_info = HoldInfo.from_license_pool(
                licensepool,
                hold_position=None,
            )
        except NoAvailableCopies:
            if existing_loan:
                # The patron tried to renew a loan but there are
                # people waiting in line for them to return the book,
                # so renewals are not allowed.
                raise CannotRenew(
                    _("You cannot renew a loan if other patrons have the work on hold.")
                )
            else:
                # That's fine, we'll just (try to) place a hold.
                #
                # Since the patron incorrectly believed there were
                # copies available, update availability information
                # immediately.
                api.update_availability(licensepool)
        except NoLicenses:
            # Since the patron incorrectly believed there were
            # licenses available, update availability information
            # immediately.
            api.update_availability(licensepool)
            raise
        except PatronLoanLimitReached as e:
            # The server-side loan limits didn't apply to this patron,
            # but there's a vendor-side loan limit that does. However,
            # we don't necessarily know whether or not this book is
            # available! We'll try putting the book on hold just in
            # case, and raise this exception only if that doesn't
            # work.
            loan_exception = e

        if loan_info:
            # We successfully secured a loan.  Now create it in our
            # database.
            __transaction = self._db.begin_nested()
            loan, new_loan_record = loan_info.create_or_update(patron, licensepool)

            if must_set_delivery_mechanism:
                loan.fulfillment = delivery_mechanism
            existing_hold = get_one(
                self._db,
                Hold,
                patron=patron,
                license_pool=licensepool,
                on_multiple="interchangeable",
            )
            if existing_hold:
                # The book was on hold, and now we have a loan. Call
                # collect cm event and delete the record of the hold.
                existing_hold.collect_event_and_delete(analytics=self.analytics)

            __transaction.commit()

            if loan and new_loan:
                # Send out an analytics event to record the fact that
                # a loan was initiated through the circulation
                # manager.
                self._collect_checkout_event(patron, licensepool)
            return loan, None, new_loan_record

        # At this point we know that we neither successfully
        # transacted a loan, nor discovered a preexisting loan.

        # Checking out a book didn't work, so let's try putting
        # the book on hold.
        if not hold_info:
            try:
                hold_info = api.place_hold(
                    patron, pin, licensepool, hold_notification_email
                )
            except AlreadyOnHold as e:
                hold_info = HoldInfo.from_license_pool(
                    licensepool,
                    hold_position=None,
                )
            except CurrentlyAvailable:
                if loan_exception:
                    # We tried to take out a loan and got an
                    # exception.  But we weren't sure whether the real
                    # problem was the exception we got or the fact
                    # that the book wasn't available. Then we tried to
                    # place a hold, which didn't work because the book
                    # is currently available. That answers the
                    # question: we should have let the first exception
                    # go through.  Raise it now.
                    raise loan_exception

                # This shouldn't normally happen, but if it does,
                # treat it as any other exception.
                raise

        # It's pretty rare that we'd go from having a loan for a book
        # to needing to put it on hold, but we do check for that case.
        __transaction = self._db.begin_nested()
        hold, is_new = hold_info.create_or_update(patron, licensepool)

        if hold and is_new:
            # Send out an analytics event to record the fact that
            # a hold was initiated through the circulation
            # manager.
            self._collect_event(patron, licensepool, CirculationEvent.CM_HOLD_PLACE)

        if existing_loan:
            # Send out analytics event capturing the unusual circumstance  that a loan was converted to a hold
            # TODO: Do we know what the conditions under which this situation can occur?
            self._collect_event(
                patron, licensepool, CirculationEvent.CM_LOAN_CONVERTED_TO_HOLD
            )
            self._db.delete(existing_loan)
        __transaction.commit()
        return None, hold, is_new

    def enforce_limits(self, patron: Patron, pool: LicensePool) -> None:
        """Enforce library-specific patron loan and hold limits.

        :param patron: A Patron.
        :param pool: A LicensePool the patron is trying to access. As
           a side effect, this method may update `pool` with the latest
           availability information from the remote API.
        :raises PatronLoanLimitReached: If `pool` is currently
            available but the patron is at their loan limit.
        :raises PatronHoldLimitReached: If `pool` is currently
            unavailable and the patron is at their hold limit.
        """
        if pool.unlimited_type:
            # Unlimited-access books are able to be checked out even if the patron is
            # at their loan limit.
            return

        at_loan_limit = self.patron_at_loan_limit(patron)
        at_hold_limit = self.patron_at_hold_limit(patron)

        if not at_loan_limit and not at_hold_limit:
            # This patron can take out either a loan or a hold, so the
            # limits don't apply.
            return

        if at_loan_limit and at_hold_limit:
            # This patron can neither take out a loan or place a hold.
            # Raise PatronLoanLimitReached for the most understandable
            # error message.
            raise PatronLoanLimitReached(limit=patron.library.settings.loan_limit)

        # At this point it's important that we get up-to-date
        # availability information about this LicensePool, to reduce
        # the risk that (e.g.) we apply the loan limit to a book that
        # would be placed on hold instead.
        api = self.api_for_license_pool(pool)
        if api is not None:
            api.update_availability(pool)

        currently_available = pool.licenses_available > 0
        if currently_available and at_loan_limit:
            raise PatronLoanLimitReached(limit=patron.library.settings.loan_limit)
        if not currently_available and at_hold_limit:
            raise PatronHoldLimitReached(limit=patron.library.settings.hold_limit)

    def patron_at_loan_limit(self, patron: Patron) -> bool:
        """Is the given patron at their loan limit?

        This doesn't belong in Patron because the loan limit is not core functionality.
        Of course, Patron itself isn't really core functionality...

        :param patron: A Patron.
        """
        loan_limit = patron.library.settings.loan_limit
        if not loan_limit:
            return False

        # Unlimited-access loans, and loans of indefinite duration, don't count towards the loan limit
        # because they don't block anyone else.
        non_unlimited_access_loans_with_end_date = [
            loan
            for loan in patron.loans
            if loan.license_pool and not loan.license_pool.unlimited_type and loan.end
        ]
        return len(non_unlimited_access_loans_with_end_date) >= loan_limit

    def patron_at_hold_limit(self, patron: Patron) -> bool:
        """Is the given patron at their hold limit?

        This doesn't belong in Patron because the hold limit is not core functionality.
        Of course, Patron itself isn't really core functionality...

        :param patron: A Patron.
        """
        hold_limit = patron.library.settings.hold_limit
        if not hold_limit:
            return False
        return len(patron.holds) >= hold_limit

    def can_fulfill_without_loan(
        self,
        patron: Patron | None,
        pool: LicensePool | None,
        lpdm: LicensePoolDeliveryMechanism | None,
    ) -> bool:
        """Can we deliver the given book in the given format to the given
        patron, even though the patron has no active loan for that
        book?

        In general this is not possible, but there are some
        exceptions, managed in subclasses of BaseCirculationAPI.

        :param patron: A Patron. This is probably None, indicating
            that someone is trying to fulfill a book without identifying
            themselves.

        :param delivery_mechanism: The LicensePoolDeliveryMechanism
            representing a format for a specific title.
        """
        if not lpdm or not pool:
            return False
        if pool.open_access:
            return True
        api = self.api_for_license_pool(pool)
        if not api:
            return False
        return api.can_fulfill_without_loan(patron, pool, lpdm)

    def fulfill(
        self,
        patron: Patron,
        pin: str,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism,
        **kwargs: Unpack[BaseCirculationAPI.FulfillKwargs],
    ) -> Fulfillment:
        """Fulfill a book that a patron has previously checked out.

        :param delivery_mechanism: A LicensePoolDeliveryMechanism
            explaining how the patron wants the book to be delivered. If
            the book has previously been delivered through some other
            mechanism, this parameter is ignored and the previously used
            mechanism takes precedence.

        :return: A Fulfillment object.

        """
        loan = get_one(
            self._db,
            Loan,
            patron=patron,
            license_pool=licensepool,
            on_multiple="interchangeable",
        )
        api = self.api_for_license_pool(licensepool)
        if not api:
            raise CannotFulfill()

        if not loan and not self.can_fulfill_without_loan(
            patron, licensepool, delivery_mechanism
        ):
            raise NoActiveLoan(_("Cannot find your active loan for this work."))
        if (
            loan
            and loan.fulfillment is not None
            and not loan.fulfillment.compatible_with(delivery_mechanism)
        ):
            raise DeliveryMechanismConflict(
                _(
                    "You already fulfilled this loan as %(loan_delivery_mechanism)s, you can't also do it as %(requested_delivery_mechanism)s",
                    loan_delivery_mechanism=loan.fulfillment.delivery_mechanism.name,
                    requested_delivery_mechanism=delivery_mechanism.delivery_mechanism.name,
                )
            )

        fulfillment = api.fulfill(
            patron,
            pin,
            licensepool,
            delivery_mechanism=delivery_mechanism,
            **kwargs,
        )
        if not fulfillment:
            raise NoAcceptableFormat()

        # Send out an analytics event to record the fact that
        # a fulfillment was initiated through the circulation
        # manager.
        self._collect_event(patron, licensepool, CirculationEvent.CM_FULFILL)

        # Make sure the delivery mechanism we just used is associated
        # with the loan, if any.
        if (
            loan
            and loan.fulfillment is None
            and not delivery_mechanism.delivery_mechanism.is_streaming
        ):
            __transaction = self._db.begin_nested()
            loan.fulfillment = delivery_mechanism
            __transaction.commit()

        return fulfillment

    def revoke_loan(
        self, patron: Patron, pin: str, licensepool: LicensePool
    ) -> Literal[True]:
        """Revoke a patron's loan for a book."""
        loan = get_one(
            self._db,
            Loan,
            patron=patron,
            license_pool=licensepool,
            on_multiple="interchangeable",
        )
        if loan is not None:
            api = self.api_for_license_pool(licensepool)
            if api is None:
                self.log.error(
                    f"Patron: {patron!r} tried to revoke loan for licensepool: {licensepool!r} but no api was found."
                )
                raise CannotReturn("No API available.")
            try:
                api.checkin(patron, pin, licensepool)
            except NotCheckedOut as e:
                # The book wasn't checked out in the first
                # place. Everything's fine.
                pass

            __transaction = self._db.begin_nested()
            logging.info(f"In revoke_loan(), deleting loan #{loan.id}")
            self._db.delete(loan)
            __transaction.commit()

            # Send out an analytics event to record the fact that
            # a loan was revoked through the circulation
            # manager.
            self._collect_event(patron, licensepool, CirculationEvent.CM_CHECKIN)

        # Any other CannotReturn exception will be propagated upwards
        # at this point.
        return True

    def release_hold(
        self, patron: Patron, pin: str, licensepool: LicensePool
    ) -> Literal[True]:
        """Remove a patron's hold on a book."""
        hold = get_one(
            self._db,
            Hold,
            patron=patron,
            license_pool=licensepool,
            on_multiple="interchangeable",
        )
        api = self.api_for_license_pool(licensepool)
        if api is None:
            raise TypeError(f"No api for licensepool: {licensepool}")
        try:
            api.release_hold(patron, pin, licensepool)
        except NotOnHold:
            # The book wasn't on hold in the first place. Everything's
            # fine.
            pass
        # Any other CannotReleaseHold exception will be propagated
        # upwards at this point
        if hold:
            __transaction = self._db.begin_nested()
            self._db.delete(hold)
            __transaction.commit()

            # Send out an analytics event to record the fact that
            # a hold was revoked through the circulation
            # manager.
            self._collect_event(
                patron,
                licensepool,
                CirculationEvent.CM_HOLD_RELEASE,
            )

        return True

    def supports_patron_activity(self, pool: LicensePool) -> bool:
        api = self.api_for_license_pool(pool)
        return isinstance(api, PatronActivityCirculationAPI)
