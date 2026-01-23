from __future__ import annotations

from flask_babel import lazy_gettext as _
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from palace.manager.api.circulation.dispatcher import CirculationApiDispatcher
from palace.manager.api.controller.base import BaseCirculationManagerController
from palace.manager.api.problem_details import (
    BAD_DELIVERY_MECHANISM,
    FORBIDDEN_BY_POLICY,
    NO_LICENSES,
    NO_SUCH_LANE,
    NOT_AGE_APPROPRIATE,
    NOT_FOUND_ON_REMOTE,
    REMOTE_INTEGRATION_FAILED,
)
from palace.manager.api.util.flask import get_request_library
from palace.manager.core.problem_details import INVALID_INPUT
from palace.manager.feed.worklist.base import WorkList
from palace.manager.search.external_search import ExternalSearchIndex
from palace.manager.service.redis.redis import Redis
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from palace.manager.sqlalchemy.model.lane import Lane
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import (
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.patron import Hold, Loan, Patron
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util import first_or_default
from palace.manager.util.problem_detail import ProblemDetail


class CirculationManagerController(BaseCirculationManagerController):
    def get_patron_circ_objects[
        T: (
            Loan,
            Hold,
        )
    ](
        self,
        object_class: type[T],
        patron: Patron | None,
        license_pools: list[LicensePool],
    ) -> list[T]:
        if not patron:
            return []
        pool_ids = [pool.id for pool in license_pools]

        return (  # type: ignore[no-any-return]
            self._db.query(object_class)
            .filter(
                object_class.patron_id == patron.id,
                object_class.license_pool_id.in_(pool_ids),
            )
            .options(joinedload(object_class.license_pool))
            .all()
        )

    def get_patron_loan(
        self, patron: Patron | None, license_pools: list[LicensePool]
    ) -> tuple[Loan, LicensePool] | tuple[None, None]:
        loans = self.get_patron_circ_objects(Loan, patron, license_pools)
        if loans:
            loan = loans[0]
            return loan, loan.license_pool
        return None, None

    def get_patron_hold(
        self, patron: Patron | None, license_pools: list[LicensePool]
    ) -> tuple[Hold, LicensePool] | tuple[None, None]:
        holds = self.get_patron_circ_objects(Hold, patron, license_pools)
        if holds:
            hold = holds[0]
            return hold, hold.license_pool
        return None, None

    @property
    def circulation(self) -> CirculationApiDispatcher:
        """Return the appropriate CirculationAPI for the request Library."""
        library_id = get_request_library().id
        return self.manager.circulation_apis[library_id]  # type: ignore[no-any-return]

    @property
    def search_engine(self) -> ExternalSearchIndex | ProblemDetail:
        """Return the configured external search engine, or a
        ProblemDetail if none is configured.
        """
        search_engine = self.manager.external_search
        if not search_engine:
            return REMOTE_INTEGRATION_FAILED.detailed(
                _("The search index for this site is not properly configured.")
            )
        return search_engine  # type: ignore[no-any-return]

    @property
    def redis_client(self) -> Redis:
        return self.manager.services.redis.client()  # type: ignore[no-any-return]

    def load_lane(self, lane_identifier: int | None) -> Lane | WorkList | ProblemDetail:
        """Turn user input into a Lane object."""
        library_id = get_request_library().id

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

    def load_work(
        self, library: Library, identifier_type: str, identifier: str
    ) -> Work | ProblemDetail:
        """Turn user input into a Work object.

        :param library: The Work must be associated with one of this Library's
            Collections.
        :param identifier_type: A type of identifier, e.g. "ISBN"
        :param identifier: An identifier string, used with `identifier_type`
            to look up an Identifier.
        """
        pools = self.load_licensepools(library, identifier_type, identifier)
        if isinstance(pools, ProblemDetail):
            return pools

        # We know there is at least one LicensePool. Find the first one with
        # a work set on it.
        work: Work | None = first_or_default([lp.work for lp in pools if lp.work])
        if work is None:
            # We have no work for this license pool. Return a ProblemDetail
            # that will give a 404 status code.
            self.log.warning(
                "No work found for license pool %r %s/%s",
                pools[0],
                identifier_type,
                identifier,
            )
            return NOT_FOUND_ON_REMOTE

        if work and not work.age_appropriate_for_patron(self.request_patron):
            # This work is not age-appropriate for the authenticated
            # patron. Don't show it.
            return NOT_AGE_APPROPRIATE
        return work

    def load_licensepools(
        self, library: Library, identifier_type: str, identifier: str
    ) -> list[LicensePool] | ProblemDetail:
        """Turn user input into one or more LicensePool objects.

        :param library: The LicensePools must be associated with one of this
            Library's Collections.
        :param identifier_type: A type of identifier, e.g. "ISBN"
        :param identifier: An identifier string, used with `identifier_type`
            to look up an Identifier.
        """
        _db = Session.object_session(library)
        identifier_obj, ignore = Identifier.for_foreign_id(
            _db, identifier_type, identifier, autocreate=False
        )
        pools = (
            _db.scalars(
                select(LicensePool)
                .join(Collection, LicensePool.collection_id == Collection.id)
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
                    LicensePool.identifier == identifier_obj,
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

    def load_licensepool(self, license_pool_id: int) -> LicensePool | ProblemDetail:
        """Turns user input into a LicensePool"""
        license_pool = get_one(self._db, LicensePool, id=license_pool_id)
        if not license_pool:
            return INVALID_INPUT.detailed(
                _("License Pool #%s does not exist.") % license_pool_id
            )

        return license_pool

    def load_licensepooldelivery(
        self, pool: LicensePool, mechanism_id: int
    ) -> LicensePoolDeliveryMechanism | ProblemDetail:
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

    def apply_borrowing_policy(
        self, patron: Patron | ProblemDetail | None, license_pool: LicensePool
    ) -> ProblemDetail | None:
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
            and license_pool.metered_or_equivalent_type
        ):
            return FORBIDDEN_BY_POLICY.detailed(
                _("Library policy prohibits the placement of holds."), status_code=403
            )
        return None
