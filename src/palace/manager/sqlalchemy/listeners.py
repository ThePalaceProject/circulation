from __future__ import annotations

import datetime
from threading import RLock
from typing import Any

from sqlalchemy import event, text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Mapper, Session

from palace.manager.core.config import Configuration
from palace.manager.core.query.coverage import EquivalencyCoverageQueries
from palace.manager.sqlalchemy.before_flush_decorator import Listener, ListenerState
from palace.manager.sqlalchemy.model.base import Base
from palace.manager.sqlalchemy.model.identifier import (
    Equivalency,
    Identifier,
    RecursiveEquivalencyCache,
)
from palace.manager.sqlalchemy.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from palace.manager.sqlalchemy.model.lane import Lane, LaneGenre
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.work import (
    Work,
    add_work_to_customlists_for_collection,
)
from palace.manager.util.datetime_helpers import utc_now

site_configuration_has_changed_lock = RLock()


def site_configuration_has_changed(_db, cooldown=1):
    """Call this whenever you want to indicate that the site configuration
    has changed and needs to be reloaded.

    This is automatically triggered on relevant changes to the data
    model, but you also should call it whenever you change an aspect
    of what you consider "site configuration", just to be safe.

    :param _db: Either a Session or (to save time in a common case) an
        ORM object that can turned into a Session.

    :param cooldown: Nothing will happen if it's been fewer than this
        number of seconds since the last site configuration change was
        recorded.
    """
    has_lock = site_configuration_has_changed_lock.acquire(blocking=False)
    if not has_lock:
        # Another thread is updating site configuration right now.
        # There is no need to do anything--the timestamp will still be
        # accurate.
        return

    try:
        _site_configuration_has_changed(_db, cooldown)
    finally:
        site_configuration_has_changed_lock.release()


def _site_configuration_has_changed(_db, cooldown=1):
    """Actually changes the timestamp on the site configuration."""
    now = utc_now()
    last_update = Configuration._site_configuration_last_update()

    if not last_update or (now - last_update).total_seconds() > cooldown:
        # The configuration last changed more than `cooldown` ago, which
        # means it's time to reset the Timestamp that says when the
        # configuration last changed.

        # Convert something that might not be a Connection object into
        # a Connection object.
        if isinstance(_db, Base):
            _db = Session.object_session(_db)

        # Update the timestamp.
        now = utc_now()
        earlier = now - datetime.timedelta(seconds=cooldown)

        # Using SKIP LOCKED here allows us avoid waiting for another process that is
        # (presumably) already updating the timestamp, only to immediately update it
        # again ourselves after the wait. It also avoids a possible deadlock for cases
        # in which a request results in another call into the app server while that
        # request is still active. For example, during library registration.
        #
        # During registration we make requests that look like this:
        # CM -- POST(register) --> Registry -- GET(authentication_document)--> CM
        sql = (
            "UPDATE timestamps SET finish=(:finish at time zone 'utc') WHERE "
            "id IN (select id from timestamps WHERE service=:service AND collection_id IS NULL "
            "AND finish<=(:earlier at time zone 'utc') FOR UPDATE SKIP LOCKED);"
        )
        _db.execute(
            text(sql),
            dict(
                service=Configuration.SITE_CONFIGURATION_CHANGED,
                finish=now,
                earlier=earlier,
            ),
        )

        # Update the Configuration's record of when the configuration
        # was updated. This will update our local record immediately
        # without requiring a trip to the database.
        Configuration.site_configuration_last_update(_db, known_value=now)


# Certain ORM events, however they occur, indicate that a work's
# external index needs updating.


@event.listens_for(Work.license_pools, "append")
@event.listens_for(Work.license_pools, "remove")
def licensepool_removed_from_work(target, value, initiator):
    """When a Work gains or loses a LicensePool, it needs to be reindexed."""
    if target:
        target.external_index_needs_updating()


@event.listens_for(Work.suppressed_for, "append")
@event.listens_for(Work.suppressed_for, "remove")
def work_suppressed_for_library(target, value, initiator):
    if target:
        target.external_index_needs_updating()


@Listener.before_flush(LicensePool, ListenerState.deleted)
def licensepool_deleted(session: Session, instance: LicensePool) -> None:
    """A LicensePool is deleted only when its collection is deleted.
    If this happens, we need to keep the Work's index up to date.
    """
    work = instance.work
    if work:
        work.external_index_needs_updating()


@event.listens_for(LicensePool.collection_id, "set")
def licensepool_collection_change(target, value, oldvalue, initiator):
    """A LicensePool should never change collections, but if it is,
    we need to keep the search index up to date.
    """
    work = target.work
    if not work:
        return
    if value == oldvalue:
        return
    work.external_index_needs_updating()


@event.listens_for(LicensePool.open_access, "set")
def licensepool_storage_status_change(target, value, oldvalue, initiator):
    """A Work may need to have its search document re-indexed if one of
    its LicensePools changes its open-access status.

    This shouldn't ever happen.
    """
    work = target.work
    if not work:
        return
    if value == oldvalue:
        return
    work.external_index_needs_updating()


@event.listens_for(Work.last_update_time, "set")
def last_update_time_change(target, value, oldvalue, initator):
    """A Work needs to have its search document re-indexed whenever its
    last_update_time changes.

    Among other things, this happens whenever the LicensePool's availability
    information changes.
    """
    if value == oldvalue:
        return
    target.external_index_needs_updating()


@Listener.before_flush(Equivalency, ListenerState.deleted)
def equivalency_coverage_reset_on_equivalency_delete(
    session: Session, target: Equivalency
) -> None:
    """On equivalency delete reset the coverage records of ANY ids touching
    the deleted identifiers
    """
    EquivalencyCoverageQueries.add_coverage_for_identifiers_chain(
        [target.input, target.output], _db=session
    )


@Listener.before_flush(Identifier, ListenerState.new)
def recursive_equivalence_on_identifier_create(
    session: Session, instance: Identifier
) -> None:
    """Whenever an Identifier is created we must atleast have the 'self, self'
    recursion available in the Recursives table else the queries will be incorrect"""
    session.add(
        RecursiveEquivalencyCache(parent_identifier=instance, identifier=instance)
    )


@Listener.before_flush((Work, LicensePool), ListenerState.new)
def add_work_to_customlists(session: Session, instance: Work | LicensePool) -> None:
    """Whenever a Work or LicensePool is created we must add it to the custom lists
    for its collection"""
    add_work_to_customlists_for_collection(instance)


@Listener.before_flush((Lane, LaneGenre), one_shot=True)
def configuration_relevant_lifecycle_event(session: Session):
    site_configuration_has_changed(session)


@event.listens_for(Lane.library_id, "set")
@event.listens_for(Lane.root_for_patron_type, "set")
def receive_modified(target, value, oldvalue, initiator):
    # Some elements of Lane configuration are stored in the
    # corresponding Library objects for performance reasons.

    # Remove this information whenever the Lane configuration
    # changes. This will force it to be recalculated.
    Library._has_root_lane_cache.clear()


# The following supports an optimization in `Library.active_collections`.
@event.listens_for(IntegrationConfiguration, "after_insert")
@event.listens_for(IntegrationConfiguration, "after_delete")
@event.listens_for(IntegrationConfiguration, "after_update")
@event.listens_for(IntegrationLibraryConfiguration, "after_insert")
@event.listens_for(IntegrationLibraryConfiguration, "after_delete")
@event.listens_for(IntegrationLibraryConfiguration, "after_update")
def handle_collection_change(_: Mapper, _connection: Connection, target: Base) -> None:
    Library._clear_active_collections_cache(target)


@event.listens_for(IntegrationConfiguration.library_configurations, "append")
@event.listens_for(IntegrationConfiguration.library_configurations, "remove")
@event.listens_for(IntegrationConfiguration.library_configurations, "set")
def handle_collection_library_relationship_change(target: Base, *_args: Any) -> None:
    Library._clear_active_collections_cache(target)
