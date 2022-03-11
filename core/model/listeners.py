# encoding: utf-8

import datetime
from threading import RLock

from sqlalchemy import event, text
from sqlalchemy.orm import Session
from sqlalchemy.orm.base import NO_VALUE

from core.model.identifier import Equivalency, Identifier, RecursiveEquivalencyCache
from core.query.coverage import EquivalencyCoverageQueries

from ..config import Configuration
from ..util.datetime_helpers import utc_now
from . import Base
from .collection import Collection
from .configuration import ConfigurationSetting, ExternalIntegration
from .library import Library
from .licensing import LicensePool
from .work import Work

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


def directly_modified(obj):
    """Return True only if `obj` has itself been modified, as opposed to
    having an object added or removed to one of its associated
    collections.
    """
    return Session.object_session(obj).is_modified(obj, include_collections=False)


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
@event.listens_for(Library.settings, "append")
@event.listens_for(Library.settings, "remove")
def configuration_relevant_collection_change(target, value, initiator):
    site_configuration_has_changed(target)


@event.listens_for(Library, "after_insert")
@event.listens_for(Library, "after_delete")
@event.listens_for(ExternalIntegration, "after_insert")
@event.listens_for(ExternalIntegration, "after_delete")
@event.listens_for(Collection, "after_insert")
@event.listens_for(Collection, "after_delete")
@event.listens_for(ConfigurationSetting, "after_insert")
@event.listens_for(ConfigurationSetting, "after_delete")
def configuration_relevant_lifecycle_event(mapper, connection, target):
    site_configuration_has_changed(target)


@event.listens_for(Library, "after_update")
@event.listens_for(ExternalIntegration, "after_update")
@event.listens_for(Collection, "after_update")
@event.listens_for(ConfigurationSetting, "after_update")
def configuration_relevant_update(mapper, connection, target):
    if directly_modified(target):
        site_configuration_has_changed(target)


# When a pool gets a work and a presentation edition for the first time,
# the work should be added to any custom lists associated with the pool's
# collection.
# In some cases, the work may be generated before the presentation edition.
# Then we need to add it when the work gets a presentation edition.
@event.listens_for(LicensePool.work_id, "set")
@event.listens_for(Work.presentation_edition_id, "set")
def add_work_to_customlists_for_collection(pool_or_work, value, oldvalue, initiator):
    if isinstance(pool_or_work, LicensePool):
        work = pool_or_work.work
        pools = [pool_or_work]
    else:
        work = pool_or_work
        pools = work.license_pools

    if (
        (not oldvalue or oldvalue is NO_VALUE)
        and value
        and work
        and work.presentation_edition
    ):
        for pool in pools:
            if not pool.collection:
                # This shouldn't happen, but don't crash if it does --
                # the correct behavior is that the work not be added to
                # any CustomLists.
                continue
            for list in pool.collection.customlists:
                # Since the work was just created, we can assume that
                # there's already a pending registration for updating the
                # work's internal index, and decide not to create a
                # second one.
                list.add_entry(work, featured=True, update_external_index=False)


# Certain ORM events, however they occur, indicate that a work's
# external index needs updating.


@event.listens_for(Work.license_pools, "append")
@event.listens_for(Work.license_pools, "remove")
def licensepool_removed_from_work(target, value, initiator):
    """When a Work gains or loses a LicensePool, it needs to be reindexed."""
    if target:
        target.external_index_needs_updating()


@event.listens_for(LicensePool, "after_delete")
def licensepool_deleted(mapper, connection, target):
    """A LicensePool is deleted only when its collection is deleted.
    If this happens, we need to keep the Work's index up to date.
    """
    work = target.work
    if work:
        record = work.external_index_needs_updating()


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
@event.listens_for(LicensePool.self_hosted, "set")
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


@event.listens_for(Equivalency, "before_delete")
def equivalency_coverage_reset_on_equivalency_delete(mapper, _db, target: Equivalency):
    """On equivalency delete reset the coverage records of ANY ids touching
    the deleted identifiers
    TODO: This is a deprecated feature of listeners, we cannot write to the DB anymore
    However we are doing this until we have a solution, ala queues
    """

    session = Session(bind=_db)
    EquivalencyCoverageQueries.add_coverage_for_identifiers_chain(
        [target.input, target.output], _db=session
    )


@event.listens_for(Identifier, "after_insert")
def recursive_equivalence_on_identifier_create(mapper, connection, target: Identifier):
    """Whenever an Identifier is created we must atleast have the 'self, self'
    recursion available in the Recursives table else the queries will be incorrect"""
    session = Session(bind=connection)
    session.add(
        RecursiveEquivalencyCache(
            parent_identifier_id=target.id, identifier_id=target.id
        )
    )
    session.commit()
    session.close()
