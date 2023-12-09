from __future__ import annotations

import email

import flask
import pytz
from flask import Response
from flask_babel import lazy_gettext as _
from sqlalchemy import select
from sqlalchemy.orm import Session, eagerload

from api.controller.base import BaseCirculationManagerController
from api.problem_details import (
    BAD_DELIVERY_MECHANISM,
    FORBIDDEN_BY_POLICY,
    NO_LICENSES,
    NO_SUCH_LANE,
    NOT_AGE_APPROPRIATE,
    REMOTE_INTEGRATION_FAILED,
)
from core.lane import Lane, WorkList
from core.model import (
    Collection,
    Hold,
    Identifier,
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
    LicensePool,
    LicensePoolDeliveryMechanism,
    Loan,
    get_one,
)
from core.problem_details import INVALID_INPUT
from core.util.problem_detail import ProblemDetail


class CirculationManagerController(BaseCirculationManagerController):
    def get_patron_circ_objects(self, object_class, patron, license_pools):
        if not patron:
            return []
        pool_ids = [pool.id for pool in license_pools]

        return (
            self._db.query(object_class)
            .filter(
                object_class.patron_id == patron.id,
                object_class.license_pool_id.in_(pool_ids),
            )
            .options(eagerload(object_class.license_pool))
            .all()
        )

    def get_patron_loan(self, patron, license_pools):
        loans = self.get_patron_circ_objects(Loan, patron, license_pools)
        if loans:
            loan = loans[0]
            return loan, loan.license_pool
        return None, None

    def get_patron_hold(self, patron, license_pools):
        holds = self.get_patron_circ_objects(Hold, patron, license_pools)
        if holds:
            hold = holds[0]
            return hold, hold.license_pool
        return None, None

    @property
    def circulation(self):
        """Return the appropriate CirculationAPI for the request Library."""
        library_id = flask.request.library.id
        return self.manager.circulation_apis[library_id]

    @property
    def search_engine(self):
        """Return the configured external search engine, or a
        ProblemDetail if none is configured.
        """
        search_engine = self.manager.external_search
        if not search_engine:
            return REMOTE_INTEGRATION_FAILED.detailed(
                _("The search index for this site is not properly configured.")
            )
        return search_engine

    def handle_conditional_request(self, last_modified=None):
        """Handle a conditional HTTP request.

        :param last_modified: A datetime representing the time this
           resource was last modified.

        :return: a Response, if the incoming request can be handled
            conditionally. Otherwise, None.
        """
        if not last_modified:
            return None

        # If-Modified-Since values have resolution of one second. If
        # last_modified has millisecond resolution, change its
        # resolution to one second.
        if last_modified.microsecond:
            last_modified = last_modified.replace(microsecond=0)

        if_modified_since = flask.request.headers.get("If-Modified-Since")
        if not if_modified_since:
            return None

        try:
            parsed_if_modified_since = email.utils.parsedate_to_datetime(
                if_modified_since
            )
        except TypeError:
            # Parse error <= Python 3.9
            return None
        except ValueError:
            # Parse error >= Python 3.10
            return None
        if not parsed_if_modified_since:
            return None

        # "[I]f the date is conforming to the RFCs it will represent a
        # time in UTC but with no indication of the actual source
        # timezone of the message the date comes from."
        if parsed_if_modified_since.tzinfo is None:
            parsed_if_modified_since = parsed_if_modified_since.replace(tzinfo=pytz.UTC)

        if parsed_if_modified_since >= last_modified:
            return Response(status=304)
        return None

    def load_lane(self, lane_identifier):
        """Turn user input into a Lane object."""
        library_id = flask.request.library.id

        lane = None
        if lane_identifier is None:
            # Return the top-level lane.
            lane = self.manager.top_level_lanes[library_id]
            if isinstance(lane, Lane):
                lane = self._db.merge(lane)
            elif isinstance(lane, WorkList):
                lane.children = [self._db.merge(child) for child in lane.children]
        else:
            try:
                lane_identifier = int(lane_identifier)
            except ValueError as e:
                pass

            if isinstance(lane_identifier, int):
                lane = get_one(
                    self._db, Lane, id=lane_identifier, library_id=library_id
                )

        if lane and not lane.accessible_to(self.request_patron):
            # The authenticated patron cannot access the lane they
            # requested. Act like the lane does not exist.
            lane = None

        if not lane:
            return NO_SUCH_LANE.detailed(
                _(
                    "Lane %(lane_identifier)s does not exist or is not associated with library %(library_id)s",
                    lane_identifier=lane_identifier,
                    library_id=library_id,
                )
            )

        return lane

    def load_work(self, library, identifier_type, identifier):
        pools = self.load_licensepools(library, identifier_type, identifier)
        if isinstance(pools, ProblemDetail):
            return pools

        # We know there is at least one LicensePool, and all LicensePools
        # for an Identifier have the same Work.
        work = pools[0].work

        if work and not work.age_appropriate_for_patron(self.request_patron):
            # This work is not age-appropriate for the authenticated
            # patron. Don't show it.
            work = NOT_AGE_APPROPRIATE
        return work

    def load_licensepools(self, library, identifier_type, identifier):
        """Turn user input into one or more LicensePool objects.

        :param library: The LicensePools must be associated with one of this
            Library's Collections.
        :param identifier_type: A type of identifier, e.g. "ISBN"
        :param identifier: An identifier string, used with `identifier_type`
            to look up an Identifier.
        """
        _db = Session.object_session(library)
        pools = (
            _db.scalars(
                select(LicensePool)
                .join(Collection, LicensePool.collection_id == Collection.id)
                .join(Identifier, LicensePool.identifier_id == Identifier.id)
                .join(
                    IntegrationConfiguration,
                    Collection.integration_configuration_id
                    == IntegrationConfiguration.id,
                )
                .join(
                    IntegrationLibraryConfiguration,
                    IntegrationConfiguration.id
                    == IntegrationLibraryConfiguration.parent_id,
                )
                .where(
                    Identifier.type == identifier_type,
                    Identifier.identifier == identifier,
                    IntegrationLibraryConfiguration.library_id == library.id,
                )
            )
            .unique()
            .all()
        )
        if not pools:
            return NO_LICENSES.detailed(
                _("The item you're asking about (%s/%s) isn't in this collection.")
                % (identifier_type, identifier)
            )
        return pools

    def load_licensepool(self, license_pool_id):
        """Turns user input into a LicensePool"""
        license_pool = get_one(self._db, LicensePool, id=license_pool_id)
        if not license_pool:
            return INVALID_INPUT.detailed(
                _("License Pool #%s does not exist.") % license_pool_id
            )

        return license_pool

    def load_licensepooldelivery(self, pool, mechanism_id):
        """Turn user input into a LicensePoolDeliveryMechanism object."""
        mechanism = get_one(
            self._db,
            LicensePoolDeliveryMechanism,
            data_source=pool.data_source,
            identifier=pool.identifier,
            delivery_mechanism_id=mechanism_id,
            on_multiple="interchangeable",
        )
        return mechanism or BAD_DELIVERY_MECHANISM

    def apply_borrowing_policy(self, patron, license_pool):
        """Apply the borrowing policy of the patron's library to the
        book they're trying to check out.

        This prevents a patron from borrowing an age-inappropriate book
        or from placing a hold in a library that prohibits holds.

        Generally speaking, both of these operations should be
        prevented before they get to this point; this is an extra
        layer of protection.

        :param patron: A `Patron`. It's okay if this turns out to be a
           `ProblemDetail` or `None` due to a problem earlier in the
           process.
        :param license_pool`: The `LicensePool` the patron is trying to act on.
        """
        if patron is None or isinstance(patron, ProblemDetail):
            # An earlier stage in the process failed to authenticate
            # the patron.
            return patron

        work = license_pool.work
        if work is not None and not work.age_appropriate_for_patron(patron):
            return NOT_AGE_APPROPRIATE

        if (
            not patron.library.settings.allow_holds
            and license_pool.licenses_available == 0
            and not license_pool.open_access
            and not license_pool.unlimited_access
        ):
            return FORBIDDEN_BY_POLICY.detailed(
                _("Library policy prohibits the placement of holds."), status_code=403
            )
        return None
