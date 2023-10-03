from __future__ import annotations

import datetime
from threading import RLock
from typing import Union

from sqlalchemy import event, text
from sqlalchemy.orm import Session

from core.config import Configuration
from core.model import Base
from core.model.before_flush_decorator import Listener, ListenerState
from core.model.collection import Collection
from core.model.configuration import ConfigurationSetting, ExternalIntegration
from core.model.identifier import Equivalency, Identifier, RecursiveEquivalencyCache
from core.model.library import Library
from core.model.licensing import LicensePool
from core.model.work import Work, add_work_to_customlists_for_collection
from core.query.coverage import EquivalencyCoverageQueries
from core.util.datetime_helpers import utc_now

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


# Most of the time, we can know whether a change to the database is
# likely to require that the application reload the portion of the
# configuration it gets from the database. These hooks will call
# site_configuration_has_changed() whenever such a change happens.
#
# This is not supposed to be a comprehensive list of changes that
# should trigger a ConfigurationSetting reload -- that needs to be
# handled on the application level -- but it should be good enough to
# catch most that slip through the cracks.
@event.listens_for(Collection.children, "append")
@event.listens_for(Collection.children, "remove")
@event.listens_for(Collection.libraries, "append")
@event.listens_for(Collection.libraries, "remove")
@event.listens_for(ExternalIntegration.settings, "append")
@event.listens_for(ExternalIntegration.settings, "remove")
@event.listens_for(Library.integrations, "append")
@event.listens_for(Library.integrations, "remove")
def configuration_relevant_collection_change(target, value, initiator):
    site_configuration_has_changed(target)


@Listener.before_flush(
    (Library, ExternalIntegration, Collection, ConfigurationSetting), one_shot=True
)
def configuration_relevant_lifecycle_event(session: Session):
    site_configuration_has_changed(session)


# Certain ORM events, however they occur, indicate that a work's
# external index needs updating.


@event.listens_for(Work.license_pools, "append")
@event.listens_for(Work.license_pools, "remove")
def licensepool_removed_from_work(target, value, initiator):
    """When a Work gains or loses a LicensePool, it needs to be reindexed."""
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
def add_work_to_customlists(
    session: Session, instance: Union[Work, LicensePool]
) -> None:
    """Whenever a Work or LicensePool is created we must add it to the custom lists
    for its collection"""
    add_work_to_customlists_for_collection(instance)
