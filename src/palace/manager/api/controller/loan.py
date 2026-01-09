from __future__ import annotations

import flask
from flask import Response, request
from flask_babel import lazy_gettext as _
from pydantic import TypeAdapter
from werkzeug import Response as wkResponse

from palace.manager.api.circulation.exceptions import (
    CirculationException,
    RemoteInitiatedServerError,
)
from palace.manager.api.controller.circulation_manager import (
    CirculationManagerController,
)
from palace.manager.api.problem_details import (
    BAD_DELIVERY_MECHANISM,
    CANNOT_RELEASE_HOLD,
    FILTERED_BY_LIBRARY_POLICY,
    HOLD_FAILED,
    NO_ACTIVE_LOAN,
    NO_ACTIVE_LOAN_OR_HOLD,
    NO_LICENSES,
    NOT_FOUND_ON_REMOTE,
)
from palace.manager.api.util.flask import get_request_library, get_request_patron
from palace.manager.celery.tasks.patron_activity import sync_patron_activity
from palace.manager.feed.acquisition import OPDSAcquisitionFeed
from palace.manager.integration.license.boundless.constants import (
    BAKER_TAYLOR_KDRM_PARAMS,
)
from palace.manager.service.redis.models.patron_activity import PatronActivity
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import (
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.patron import Hold, Loan, Patron
from palace.manager.util.flask_util import OPDSEntryResponse
from palace.manager.util.problem_detail import BaseProblemDetailException, ProblemDetail


class LoanController(CirculationManagerController):
    def sync(self) -> Response:
        """Sync the authenticated patron's loans and holds with all third-party
        providers.

        :return: A Response containing an OPDS feed with up-to-date information.
        """
        patron: Patron = get_request_patron()

        try:
            # Parse the refresh query parameter as a boolean.
            refresh = TypeAdapter(bool).validate_python(
                flask.request.args.get("refresh", "true")
            )
        except ValueError:
            # If we can't parse the refresh query parameter, default to True.
            self.log.exception(f"Could not parse refresh query parameter.")
            refresh = True

        # Queue up tasks to sync the patron's activity with any third-party providers,
        # that need to be synced. We don't wait for the task to complete, so we can return
        # the feed immediately. If our knowledge of the loans is out of date, the patron will
        # see the updated information when they refresh the page.
        if patron.authorization_identifier and refresh:
            header = self.authorization_header()
            credential = self.manager.auth.get_credential_from_header(header)
            for collection in PatronActivity.collections_ready_for_sync(
                self.redis_client, patron
            ):
                sync_patron_activity.apply_async((collection.id, patron.id, credential))

        # Then make the feed.
        feed = OPDSAcquisitionFeed.active_loans_for(self.circulation, patron)
        response = feed.as_response(
            max_age=0,
            private=True,
            mime_types=flask.request.accept_mimetypes,
        )

        return response

    def borrow(
        self, identifier_type: str, identifier: str, mechanism_id: int | None = None
    ) -> OPDSEntryResponse | ProblemDetail | None:
        """Create a new loan or hold for a book.

        :return: A Response containing an OPDS entry that includes a link of rel
           "http://opds-spec.org/acquisition", which can be used to fetch the
           book or the license file.
        """
        patron = get_request_patron()
        library = get_request_library()

        header = self.authorization_header()
        credential = self.manager.auth.get_credential_from_header(header)

        result = self.best_lendable_pool(
            library, patron, identifier_type, identifier, mechanism_id
        )
        if not result:
            # No LicensePools were found and no ProblemDetail
            # was returned. Send a generic ProblemDetail.
            return NO_LICENSES.detailed(_("I've never heard of this work."))
        if isinstance(result, ProblemDetail):
            # There was a problem determining the appropriate
            # LicensePool to use.
            return result

        loan_or_hold: Loan | Hold
        if isinstance(result, Loan):
            # We already have a Loan, so there's no need to go to the API.
            loan_or_hold = result
            is_new = False
        else:
            # We need to actually go out to the API
            # and try to take out a loan.
            pool, mechanism = result
            loan_or_hold_or_pd, is_new = self._borrow(
                patron, credential, pool, mechanism
            )
            if isinstance(loan_or_hold_or_pd, ProblemDetail):
                return loan_or_hold_or_pd
            loan_or_hold = loan_or_hold_or_pd

        # At this point we have either a loan or a hold.

        # If it is a new loan or hold, queue up a task to sync the patron's activity with the remote.
        # This way we are sure we have the most up-to-date information.
        if is_new and self.circulation.supports_patron_activity(
            loan_or_hold.license_pool
        ):
            sync_patron_activity.apply_async(
                (loan_or_hold.license_pool.collection.id, patron.id, credential),
                {"force": True},
                countdown=5,
            )

        # If we have a loan, serve a feed that tells the patron how to fulfill the loan. If a hold,
        # serve a feed that talks about the hold. We also need to drill in the Accept header, so that
        # it eventually gets sent to core.feed.opds.BaseOPDSFeed.entry_as_response
        response_kwargs = {
            "status": 201 if is_new else 200,
            "mime_types": flask.request.accept_mimetypes,
        }
        return OPDSAcquisitionFeed.single_entry_loans_feed(
            self.circulation, loan_or_hold, **response_kwargs  # type: ignore[arg-type]
        )

    def _borrow(
        self,
        patron: Patron,
        credential: str | None,
        pool: LicensePool,
        mechanism: LicensePoolDeliveryMechanism | None,
    ) -> tuple[Loan | Hold | ProblemDetail, bool]:
        """Go out to the API, try to take out a loan, and handle errors as
        problem detail documents.

        :param patron: The Patron who's trying to take out the loan
        :param credential: A Credential to use when authenticating
           as this Patron with the external API.
        :param pool: The LicensePool for the book the Patron wants.
        :mechanism: The DeliveryMechanism to request when asking for
           a loan.
        :return: a 2-tuple (result, is_new) `result` is a Loan (if one
           could be created or found), a Hold (if a Loan could not be
           created but a Hold could be), or a ProblemDetail (if the
           entire operation failed).
        """
        try:
            loan, hold, is_new = self.circulation.borrow(
                patron, credential, pool, mechanism
            )
            result = loan or hold
        except (CirculationException, RemoteInitiatedServerError) as e:
            return e.problem_detail, False

        if result is None:
            # This shouldn't happen, but if it does, it means no exception
            # was raised but we just didn't get a loan or hold. Return a
            # generic circulation error.
            return HOLD_FAILED, False
        return result, is_new

    def best_lendable_pool(
        self,
        library: Library,
        patron: Patron,
        identifier_type: str,
        identifier: str,
        mechanism_id: int | None,
    ) -> (
        Loan
        | ProblemDetail
        | tuple[LicensePool, LicensePoolDeliveryMechanism | None]
        | None
    ):
        """
        Of the available LicensePools for the given Identifier, return the
        one that's the best candidate for loaning out right now.

        :return: A Loan if this patron already has an active loan, otherwise a LicensePool.
        """
        # Turn source + identifier into a set of LicensePools
        pools = self.load_licensepools(library, identifier_type, identifier)
        if isinstance(pools, ProblemDetail):
            # Something went wrong.
            return pools

        best: LicensePool | None = None
        mechanism = None
        problem_doc = None

        existing_loans: list[Loan] = (
            self._db.query(Loan)
            .filter(
                Loan.license_pool_id.in_([lp.id for lp in pools]), Loan.patron == patron
            )
            .all()
        )
        if existing_loans:
            # The patron already has at least one loan on this book already.
            # To make the "borrow" operation idempotent, return one of
            # those loans instead of an error.
            return existing_loans[0]

        # Check library content filtering. This check comes after the existing
        # loan check above so that patrons can still access works they already
        # have on loan, even if filtering rules change after the loan was made.
        work = next((pool.work for pool in pools if pool.work), None)
        if work and work.is_filtered_for_library(library):
            return FILTERED_BY_LIBRARY_POLICY

        # We found a number of LicensePools. Try to locate one that
        # we can actually loan to the patron.
        for pool in pools:
            problem_doc = self.apply_borrowing_policy(patron, pool)
            if problem_doc:
                # As a matter of policy, the patron is not allowed to borrow
                # this book.
                continue

            # Beyond this point we know that site policy does not prohibit
            # us from lending this pool to this patron.

            if mechanism_id:
                # But the patron has requested a license pool that
                # supports a specific delivery mechanism. This pool
                # must offer that mechanism.
                mechanism_or_pd = self.load_licensepooldelivery(pool, mechanism_id)
                if isinstance(mechanism_or_pd, ProblemDetail):
                    problem_doc = mechanism_or_pd
                    continue
                else:
                    mechanism = mechanism_or_pd

            # Beyond this point we have a license pool that we can
            # actually loan or put on hold.

            # But there might be many such LicensePools, and we want
            # to pick the one that will get the book to the patron
            # with the shortest wait.
            if (
                not best
                or pool.licenses_available > best.licenses_available
                or pool.patrons_in_hold_queue < best.patrons_in_hold_queue
            ):
                best = pool

        if not best:
            # We were unable to find any LicensePool that fit the
            # criteria.
            return problem_doc
        return best, mechanism

    def fulfill(
        self,
        license_pool_id: int,
        mechanism_id: int | None = None,
    ) -> wkResponse | ProblemDetail:
        """Fulfill a book that has already been checked out,
        or which can be fulfilled with no active loan.

        If successful, this will serve the patron a downloadable copy
        of the book, a key (such as a DRM license file or bearer
        token) which can be used to get the book, or an OPDS entry
        containing a link to the book.

        :param license_pool_id: Database ID of a LicensePool.
        :param mechanism_id: Database ID of a DeliveryMechanism.
        """
        # Unlike most controller methods, this one has different
        # behavior whether or not the patron is authenticated. This is
        # why we're about to do something we don't usually do--call
        # authenticated_patron_from_request from within a controller
        # method.
        authentication_response = self.authenticated_patron_from_request()
        if isinstance(authentication_response, Patron):
            # The patron is authenticated.
            patron = authentication_response
        else:
            # The patron is not authenticated, either due to bad credentials
            # (in which case authentication_response is a Response)
            # or due to an integration error with the auth provider (in
            # which case it is a ProblemDetail).
            #
            # There's still a chance this request can succeed, but if not,
            # we'll be sending out authentication_response.
            patron = None
        library = get_request_library()
        header = self.authorization_header()
        credential = self.manager.auth.get_credential_from_header(header)

        # Turn source + identifier into a LicensePool.
        pool = self.load_licensepool(license_pool_id)
        if isinstance(pool, ProblemDetail):
            return pool

        loan, loan_license_pool = self.get_patron_loan(patron, [pool])

        requested_license_pool = loan_license_pool or pool

        # Find the LicensePoolDeliveryMechanism they asked for.
        mechanism = None
        if mechanism_id:
            mechanism_or_pd = self.load_licensepooldelivery(
                requested_license_pool, mechanism_id
            )
            if isinstance(mechanism_or_pd, ProblemDetail):
                return mechanism_or_pd
            else:
                mechanism = mechanism_or_pd

        if (not loan or not loan_license_pool) and not (
            self.can_fulfill_without_loan(
                library, patron, requested_license_pool, mechanism
            )
        ):
            if patron:
                # Since a patron was identified, the problem is they have
                # no active loan.
                return NO_ACTIVE_LOAN.detailed(
                    _("You have no active loan for this title.")
                )
            else:
                # Since no patron was identified, the problem is
                # whatever problem was revealed by the earlier
                # authenticated_patron_from_request() call -- either the
                # patron didn't authenticate or there's a problem
                # integrating with the auth provider.
                return authentication_response  # type: ignore[return-value]

        if not mechanism:
            # See if the loan already has a mechanism set. We can use that.
            if loan and loan.fulfillment:
                mechanism = loan.fulfillment
            else:
                return BAD_DELIVERY_MECHANISM.detailed(
                    _("You must specify a delivery mechanism to fulfill this loan.")
                )

        try:
            # Get any additional information that may be present in the request that is
            # needed to fulfill the loan.
            baker_taylor_request_args = {
                k: request.args.get(k) for k in BAKER_TAYLOR_KDRM_PARAMS
            }
            client_ip = request.remote_addr
            if forward_for_header := request.headers.get("X-Forwarded-For"):
                # If the request has an X-Forwarded-For header, use that to determine the client IP.
                # This is useful for integrations that require the client IP for DRM or other purposes.
                client_ip = forward_for_header.split(",")[0].strip()

            fulfillment = self.circulation.fulfill(
                patron,  # type: ignore[arg-type]
                credential,
                requested_license_pool,
                mechanism,
                client_ip=client_ip,
                **baker_taylor_request_args,
            )
        except (CirculationException, RemoteInitiatedServerError) as e:
            return e.problem_detail

        try:
            return fulfillment.response(self.circulation, loan)
        except BaseProblemDetailException as e:
            return e.problem_detail

    def can_fulfill_without_loan(
        self,
        library: Library,
        patron: Patron | None,
        pool: LicensePool,
        lpdm: LicensePoolDeliveryMechanism | None,
    ) -> bool:
        """Is it acceptable to fulfill the given LicensePoolDeliveryMechanism
        for the given Patron without creating a Loan first?

        This question is usually asked because no Patron has been
        authenticated, and thus no Loan can be created, but somebody
        wants a book anyway.

        :param library: A Library.
        :param patron: A Patron, probably None.
        :param lpdm: A LicensePoolDeliveryMechanism.
        """
        authenticator = self.manager.auth.library_authenticators.get(library.short_name)
        if authenticator and authenticator.identifies_individuals:
            # This library identifies individual patrons, so there is
            # no reason to fulfill books without a loan. Even if the
            # books are free and the 'loans' are nominal, having a
            # Loan object makes it possible for a patron to sync their
            # collection across devices, so that's the way we do it.
            return False

        # If the library doesn't require that individual patrons
        # identify themselves, it's up to the CirculationAPI object.
        # Most of them will say no. (This would indicate that the
        # collection is improperly associated with a library that
        # doesn't identify its patrons.)
        return self.circulation.can_fulfill_without_loan(patron, pool, lpdm)

    def revoke(self, license_pool_id: int) -> OPDSEntryResponse | ProblemDetail:
        patron = get_request_patron()
        pool = self.load_licensepool(license_pool_id)
        if isinstance(pool, ProblemDetail):
            return pool

        loan, _ignore = self.get_patron_loan(patron, [pool])

        if loan:
            hold = None
        else:
            hold, _ignore = self.get_patron_hold(patron, [pool])

        if not loan and not hold:
            if not pool.work:
                title = "this book"
            else:
                title = '"%s"' % pool.work.title
            return NO_ACTIVE_LOAN_OR_HOLD.detailed(
                _(
                    'Can\'t revoke because you have no active loan or hold for "%(title)s".',
                    title=title,
                ),
                status_code=404,
            )
        work = pool.work
        if not work:
            # Somehow we have a loan or hold for a LicensePool that has no Work.
            self.log.error(
                "Can't revoke loan or hold for LicensePool %r which has no Work!",
                pool,
            )
            return NOT_FOUND_ON_REMOTE

        header = self.authorization_header()
        credential = self.manager.auth.get_credential_from_header(header)
        if loan:
            try:
                self.circulation.revoke_loan(patron, credential, pool)
            except (CirculationException, RemoteInitiatedServerError) as e:
                return e.problem_detail
        elif hold:
            if not self.circulation.can_revoke_hold(pool, hold):
                title = _("Cannot release a hold once it enters reserved state.")
                return CANNOT_RELEASE_HOLD.detailed(title, 400)
            try:
                self.circulation.release_hold(patron, credential, pool)
            except (CirculationException, RemoteInitiatedServerError) as e:
                return e.problem_detail

        # At this point we have successfully revoked the loan or hold.
        # If the api supports it, queue up a task to sync the patron's activity with the remote.
        # That way we are sure we have the most up-to-date information.
        if self.circulation.supports_patron_activity(pool):
            sync_patron_activity.apply_async(
                (pool.collection.id, patron.id, credential),
                {"force": True},
                countdown=5,
            )

        annotator = self.manager.annotator(None)
        single_entry_feed = OPDSAcquisitionFeed.single_entry(work, annotator)
        return OPDSAcquisitionFeed.entry_as_response(
            single_entry_feed,
            mime_types=flask.request.accept_mimetypes,
        )

    def detail(
        self, identifier_type: str, identifier: str
    ) -> OPDSEntryResponse | ProblemDetail | None:
        patron = get_request_patron()
        library = get_request_library()
        pools = self.load_licensepools(library, identifier_type, identifier)
        if isinstance(pools, ProblemDetail):
            return pools

        loan, pool = self.get_patron_loan(patron, pools)
        if loan:
            return OPDSAcquisitionFeed.single_entry_loans_feed(self.circulation, loan)

        hold, pool = self.get_patron_hold(patron, pools)
        if hold:
            return OPDSAcquisitionFeed.single_entry_loans_feed(self.circulation, hold)

        if pool and pool.work and pool.work.title:
            title = pool.work.title
        else:
            title = "unknown"
        return NO_ACTIVE_LOAN_OR_HOLD.detailed(
            _(
                'You have no active loan or hold for "%(title)s".',
                title=title,
            ),
            status_code=404,
        )
