from __future__ import annotations

from typing import Any

import flask
from flask import Response, redirect
from flask_babel import lazy_gettext as _
from lxml import etree
from werkzeug import Response as wkResponse

from api.circulation_exceptions import (
    AuthorizationBlocked,
    AuthorizationExpired,
    CannotFulfill,
    CannotHold,
    CannotLoan,
    CannotReleaseHold,
    CannotRenew,
    CannotReturn,
    CirculationException,
    DeliveryMechanismConflict,
    DeliveryMechanismError,
    FormatNotAvailable,
    NoActiveLoan,
    NoOpenAccessDownload,
    NotFoundOnRemote,
    OutstandingFines,
    PatronAuthorizationFailedException,
    PatronHoldLimitReached,
    PatronLoanLimitReached,
    RemoteRefusedReturn,
)
from api.controller.circulation_manager import CirculationManagerController
from api.problem_details import (
    BAD_DELIVERY_MECHANISM,
    CANNOT_FULFILL,
    CANNOT_RELEASE_HOLD,
    CHECKOUT_FAILED,
    COULD_NOT_MIRROR_TO_REMOTE,
    DELIVERY_CONFLICT,
    HOLD_FAILED,
    INVALID_CREDENTIALS,
    NO_ACCEPTABLE_FORMAT,
    NO_ACTIVE_LOAN,
    NO_ACTIVE_LOAN_OR_HOLD,
    NO_LICENSES,
    NOT_FOUND_ON_REMOTE,
    OUTSTANDING_FINES,
    RENEW_FAILED,
)
from core.feed.acquisition import OPDSAcquisitionFeed
from core.model import DataSource, DeliveryMechanism, Loan, Patron, Representation
from core.util.http import RemoteIntegrationException
from core.util.opds_writer import OPDSFeed
from core.util.problem_detail import ProblemDetail


class LoanController(CirculationManagerController):
    def sync(self):
        """Sync the authenticated patron's loans and holds with all third-party
        providers.

        :return: A Response containing an OPDS feed with up-to-date information.
        """
        patron = flask.request.patron

        # Save some time if we don't believe the patron's loans or holds have
        # changed since the last time the client requested this feed.
        response = self.handle_conditional_request(patron.last_loan_activity_sync)
        if isinstance(response, Response):
            return response

        # TODO: SimplyE used to make a HEAD request to the bookshelf feed
        # as a quick way of checking authentication. Does this still happen?
        # It shouldn't -- the patron profile feed should be used instead.
        # If it's not used, we can take this out.
        if flask.request.method == "HEAD":
            return Response()

        # First synchronize our local list of loans and holds with all
        # third-party loan providers.
        if patron.authorization_identifier:
            header = self.authorization_header()
            credential = self.manager.auth.get_credential_from_header(header)
            try:
                self.circulation.sync_bookshelf(patron, credential)
            except Exception as e:
                # If anything goes wrong, omit the sync step and just
                # display the current active loans, as we understand them.
                self.manager.log.error(
                    "ERROR DURING SYNC for %s: %r", patron.id, e, exc_info=e
                )

        # Then make the feed.
        feed = OPDSAcquisitionFeed.active_loans_for(self.circulation, patron)
        response = feed.as_response(
            max_age=0,
            private=True,
            mime_types=flask.request.accept_mimetypes,
        )

        last_modified = patron.last_loan_activity_sync
        if last_modified:
            response.last_modified = last_modified
        return response

    def borrow(self, identifier_type, identifier, mechanism_id=None):
        """Create a new loan or hold for a book.

        :return: A Response containing an OPDS entry that includes a link of rel
           "http://opds-spec.org/acquisition", which can be used to fetch the
           book or the license file.
        """
        patron = flask.request.patron
        library = flask.request.library

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

        if isinstance(result, Loan):
            # We already have a Loan, so there's no need to go to the API.
            loan_or_hold = result
            is_new = False
        else:
            # We need to actually go out to the API
            # and try to take out a loan.
            pool, mechanism = result
            loan_or_hold, is_new = self._borrow(patron, credential, pool, mechanism)

        if isinstance(loan_or_hold, ProblemDetail):
            return loan_or_hold

        # At this point we have either a loan or a hold. If a loan, serve
        # a feed that tells the patron how to fulfill the loan. If a hold,
        # serve a feed that talks about the hold.
        response_kwargs = {}
        if is_new:
            response_kwargs["status"] = 201
        else:
            response_kwargs["status"] = 200
        return OPDSAcquisitionFeed.single_entry_loans_feed(
            self.circulation, loan_or_hold, **response_kwargs
        )

    def _borrow(self, patron, credential, pool, mechanism):
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
        result = None
        is_new = False
        try:
            loan, hold, is_new = self.circulation.borrow(
                patron, credential, pool, mechanism
            )
            result = loan or hold
        except NoOpenAccessDownload as e:
            result = NO_LICENSES.detailed(
                _("Couldn't find an open-access download link for this book."),
                status_code=404,
            )
        except PatronAuthorizationFailedException as e:
            result = INVALID_CREDENTIALS
        except (PatronLoanLimitReached, PatronHoldLimitReached) as e:
            result = e.as_problem_detail_document().with_debug(str(e))
        except DeliveryMechanismError as e:
            result = BAD_DELIVERY_MECHANISM.with_debug(
                str(e), status_code=e.status_code
            )
        except OutstandingFines as e:
            result = OUTSTANDING_FINES.detailed(
                _(
                    "You must pay your $%(fine_amount).2f outstanding fines before you can borrow more books.",
                    fine_amount=patron.fines,
                )
            )
        except AuthorizationExpired as e:
            result = e.as_problem_detail_document(debug=False)
        except AuthorizationBlocked as e:
            result = e.as_problem_detail_document(debug=False)
        except CannotLoan as e:
            result = CHECKOUT_FAILED.with_debug(str(e))
        except CannotHold as e:
            result = HOLD_FAILED.with_debug(str(e))
        except CannotRenew as e:
            result = RENEW_FAILED.with_debug(str(e))
        except NotFoundOnRemote as e:
            result = NOT_FOUND_ON_REMOTE
        except CirculationException as e:
            # Generic circulation error.
            result = CHECKOUT_FAILED.with_debug(str(e))

        if result is None:
            # This shouldn't happen, but if it does, it means no exception
            # was raised but we just didn't get a loan or hold. Return a
            # generic circulation error.
            result = HOLD_FAILED
        return result, is_new

    def best_lendable_pool(
        self, library, patron, identifier_type, identifier, mechanism_id
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

        best = None
        mechanism = None
        problem_doc = None

        existing_loans = (
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
                mechanism = self.load_licensepooldelivery(pool, mechanism_id)
                if isinstance(mechanism, ProblemDetail):
                    problem_doc = mechanism
                    continue

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
        do_get: Any | None = None,
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
        do_get = do_get or Representation.simple_http_get

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
        library = flask.request.library  # type: ignore
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
            mechanism = self.load_licensepooldelivery(
                requested_license_pool, mechanism_id
            )
            if isinstance(mechanism, ProblemDetail):
                return mechanism

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
                return authentication_response

        if not mechanism:
            # See if the loan already has a mechanism set. We can use that.
            if loan and loan.fulfillment:
                mechanism = loan.fulfillment
            else:
                return BAD_DELIVERY_MECHANISM.detailed(
                    _("You must specify a delivery mechanism to fulfill this loan.")
                )

        try:
            fulfillment = self.circulation.fulfill(
                patron,
                credential,
                requested_license_pool,
                mechanism,
            )
        except DeliveryMechanismConflict as e:
            return DELIVERY_CONFLICT.detailed(str(e))
        except NoActiveLoan as e:
            return NO_ACTIVE_LOAN.detailed(
                _("Can't fulfill loan because you have no active loan for this book."),
                status_code=e.status_code,
            )
        except FormatNotAvailable as e:
            return NO_ACCEPTABLE_FORMAT.with_debug(str(e), status_code=e.status_code)
        except CannotFulfill as e:
            return CANNOT_FULFILL.with_debug(str(e), status_code=e.status_code)
        except DeliveryMechanismError as e:
            return BAD_DELIVERY_MECHANISM.with_debug(str(e), status_code=e.status_code)

        # A subclass of FulfillmentInfo may want to bypass the whole
        # response creation process.
        response = fulfillment.as_response
        if response is not None:
            return response

        headers = dict()
        encoding_header = dict()
        if (
            fulfillment.data_source_name == DataSource.ENKI
            and mechanism.delivery_mechanism.drm_scheme_media_type
            == DeliveryMechanism.NO_DRM
        ):
            encoding_header["Accept-Encoding"] = "deflate"

        if mechanism.delivery_mechanism.is_streaming:
            # If this is a streaming delivery mechanism, create an OPDS entry
            # with a fulfillment link to the streaming reader url.
            feed = OPDSAcquisitionFeed.single_entry_loans_feed(
                self.circulation, loan, fulfillment=fulfillment
            )
            if isinstance(feed, ProblemDetail):
                # This should typically never happen, since we've gone through the entire fulfill workflow
                # But for the sake of return-type completeness we are adding this here
                return feed
            if isinstance(feed, Response):
                return feed
            else:
                content = etree.tostring(feed)
            status_code = 200
            headers["Content-Type"] = OPDSFeed.ACQUISITION_FEED_TYPE
        elif fulfillment.content_link_redirect is True:
            # The fulfillment API has asked us to not be a proxy and instead redirect the client directly
            return redirect(fulfillment.content_link)
        else:
            content = fulfillment.content
            if fulfillment.content_link:
                # If we have a link to the content on a remote server, web clients may not
                # be able to access it if the remote server does not support CORS requests.

                # If the pool is open access though, the web client can link directly to the
                # file to download it, so it's safe to redirect.
                if requested_license_pool.open_access:
                    return redirect(fulfillment.content_link)

                # Otherwise, we need to fetch the content and return it instead
                # of redirecting to it, since it may be downloaded through an
                # indirect acquisition link.
                try:
                    status_code, headers, content = do_get(
                        fulfillment.content_link, headers=encoding_header
                    )
                    headers = dict(headers)
                except RemoteIntegrationException as e:
                    return e.as_problem_detail_document(debug=False)
            else:
                status_code = 200
            if fulfillment.content_type:
                headers["Content-Type"] = fulfillment.content_type

        return Response(response=content, status=status_code, headers=headers)

    def can_fulfill_without_loan(self, library, patron, pool, lpdm):
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

    def revoke(self, license_pool_id):
        patron = flask.request.patron
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

        header = self.authorization_header()
        credential = self.manager.auth.get_credential_from_header(header)
        if loan:
            try:
                self.circulation.revoke_loan(patron, credential, pool)
            except RemoteRefusedReturn as e:
                title = _(
                    "Loan deleted locally but remote refused. Loan is likely to show up again on next sync."
                )
                return COULD_NOT_MIRROR_TO_REMOTE.detailed(title, status_code=503)
            except CannotReturn as e:
                title = _("Loan deleted locally but remote failed.")
                return COULD_NOT_MIRROR_TO_REMOTE.detailed(title, 503).with_debug(
                    str(e)
                )
        elif hold:
            if not self.circulation.can_revoke_hold(pool, hold):
                title = _("Cannot release a hold once it enters reserved state.")
                return CANNOT_RELEASE_HOLD.detailed(title, 400)
            try:
                self.circulation.release_hold(patron, credential, pool)
            except CannotReleaseHold as e:
                title = _("Hold released locally but remote failed.")
                return CANNOT_RELEASE_HOLD.detailed(title, 503).with_debug(str(e))

        work = pool.work
        annotator = self.manager.annotator(None)
        return OPDSAcquisitionFeed.entry_as_response(
            OPDSAcquisitionFeed.single_entry(work, annotator)
        )

    def detail(self, identifier_type, identifier):
        if flask.request.method == "DELETE":
            return self.revoke_loan_or_hold(identifier_type, identifier)

        patron = flask.request.patron
        library = flask.request.library
        pools = self.load_licensepools(library, identifier_type, identifier)
        if isinstance(pools, ProblemDetail):
            return pools

        loan, pool = self.get_patron_loan(patron, pools)
        if loan:
            hold = None
        else:
            hold, pool = self.get_patron_hold(patron, pools)

        if not loan and not hold:
            return NO_ACTIVE_LOAN_OR_HOLD.detailed(
                _(
                    'You have no active loan or hold for "%(title)s".',
                    title=pool.work.title,
                ),
                status_code=404,
            )

        if flask.request.method == "GET":
            if loan:
                item = loan
            else:
                item = hold
            return OPDSAcquisitionFeed.single_entry_loans_feed(self.circulation, item)
