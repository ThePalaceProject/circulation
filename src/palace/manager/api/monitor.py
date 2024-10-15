from collections.abc import Callable

from sqlalchemy import and_, or_

from palace.manager.api.opds_for_distributors import OPDSForDistributorsAPI
from palace.manager.core.monitor import ReaperMonitor
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.patron import (
    Annotation,
    Hold,
    Loan,
    LoanAndHoldMixin,
)
from palace.manager.util.datetime_helpers import utc_now


class LoanlikeReaperMonitor(ReaperMonitor):
    SOURCE_OF_TRUTH_PROTOCOLS = [
        OPDSForDistributorsAPI.label(),
    ]

    @property
    def where_clause(self):
        """We never want to automatically reap loans or holds for situations
        where the circulation manager is the source of truth. If we
        delete something we shouldn't have, we won't be able to get
        the 'real' information back.

        This means loans of open-access content and loans from
        collections based on a protocol found in
        SOURCE_OF_TRUTH_PROTOCOLS.

        Subclasses will append extra clauses to this filter.
        """
        source_of_truth = or_(
            LicensePool.open_access == True,
            IntegrationConfiguration.protocol.in_(self.SOURCE_OF_TRUTH_PROTOCOLS),
        )

        source_of_truth_subquery = (
            self._db.query(self.MODEL_CLASS.id)
            .join(self.MODEL_CLASS.license_pool)
            .join(LicensePool.collection)
            .join(
                IntegrationConfiguration,
                Collection.integration_configuration_id == IntegrationConfiguration.id,
            )
            .filter(source_of_truth)
        )
        return ~self.MODEL_CLASS.id.in_(source_of_truth_subquery)

    def post_delete_op(self, row) -> Callable:
        loan_like: LoanAndHoldMixin = row

        def post_delete():
            ce = CirculationEvent
            event_type = (
                ce.CM_HOLD_EXPIRED
                if isinstance(loan_like, Hold)
                else ce.CM_LOAN_EXPIRED
            )
            self.analytics.collect_event(
                library=loan_like.library,
                license_pool=loan_like.license_pool,
                event_type=event_type,
            )

        return post_delete


class LoanReaper(LoanlikeReaperMonitor):
    """Remove expired and abandoned loans from the database."""

    MODEL_CLASS: type[Loan] = Loan
    MAX_AGE = 90

    @property
    def where_clause(self):
        """Find loans that have either expired, or that were created a long
        time ago and have no definite end date.
        """
        start_field = self.MODEL_CLASS.start
        end_field = self.MODEL_CLASS.end
        superclause = super().where_clause
        now = utc_now()
        expired = end_field < now
        very_old_with_no_clear_end_date = and_(
            start_field < self.cutoff, end_field == None
        )
        not_unlimited_access = LicensePool.unlimited_access == False
        return and_(
            superclause,
            not_unlimited_access,
            or_(expired, very_old_with_no_clear_end_date),
        )

    def query(self):
        query = super().query()
        return query.join(self.MODEL_CLASS.license_pool)


ReaperMonitor.REGISTRY.append(LoanReaper)


class HoldReaper(LoanlikeReaperMonitor):
    """Remove seemingly abandoned holds from the database."""

    MODEL_CLASS = Hold
    MAX_AGE = 365

    @property
    def where_clause(self):
        """Find holds that were created a long time ago and either have
        no end date or have an end date in the past.

        The 'end date' for a hold is just an estimate, but if the estimate
        is in the future it's better to keep the hold around.
        """
        start_field = self.MODEL_CLASS.start
        end_field = self.MODEL_CLASS.end
        superclause = super().where_clause
        end_date_in_past = end_field < utc_now()
        probably_abandoned = and_(
            start_field < self.cutoff, or_(end_field == None, end_date_in_past)
        )
        return and_(superclause, probably_abandoned)


ReaperMonitor.REGISTRY.append(HoldReaper)


class IdlingAnnotationReaper(ReaperMonitor):
    """Remove idling annotations for inactive loans."""

    MODEL_CLASS = Annotation
    TIMESTAMP_FIELD = "timestamp"
    MAX_AGE = 60

    @property
    def where_clause(self):
        """The annotation must have motivation=IDLING, must be at least 60
        days old (meaning there has been no attempt to read the book
        for 60 days), and must not be associated with one of the
        patron's active loans or holds.
        """
        superclause = super().where_clause

        restrictions = []
        for t in Loan, Hold:
            active_subquery = (
                self._db.query(Annotation.id)
                .join(t, t.patron_id == Annotation.patron_id)
                .join(
                    LicensePool,
                    and_(
                        LicensePool.id == t.license_pool_id,
                        LicensePool.identifier_id == Annotation.identifier_id,
                    ),
                )
            )
            restrictions.append(~Annotation.id.in_(active_subquery))
        return and_(
            superclause, Annotation.motivation == Annotation.IDLING, *restrictions
        )


ReaperMonitor.REGISTRY.append(IdlingAnnotationReaper)
