from __future__ import annotations

import datetime
import sys
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from threading import RLock
from typing import Callable, Dict, List, Optional, Set, Tuple, Type, Union

from sqlalchemy import event, text
from sqlalchemy.orm import Session
from sqlalchemy.orm.unitofwork import UOWTransaction

from core.model.identifier import Equivalency, Identifier, RecursiveEquivalencyCache
from core.query.coverage import EquivalencyCoverageQueries

from ..config import Configuration
from ..util.datetime_helpers import utc_now
from . import Base
from .collection import Collection
from .configuration import ConfigurationSetting, ExternalIntegration
from .library import Library
from .licensing import LicensePool
from .work import Work, add_work_to_customlists_for_collection

# TODO: Remove this when we drop support for Python 3.9
if sys.version_info >= (3, 10):
    from typing import ParamSpec
else:
    from typing_extensions import ParamSpec


class State(Enum):
    NEW = "new"
    DELETED = "deleted"
    DIRTY = "dirty"
    ANY = "any"


_before_flush_hooks: Dict[
    Tuple[Type[Base], State], List[Callable[[Session, Base], None]]
] = defaultdict(list)
_before_flush_trigger_hooks: Dict[
    Callable[[Session], None], Set[Type[Base]]
] = defaultdict(set)


P = ParamSpec("P")


def before_flush(
    model: Type[Base], state: State
) -> Callable[[Callable[P, None]], Callable[P, None]]:
    """
    Decorator to register a function to be called before a flush.

    The decorated function will be called when a model of the given type is in the given state. The function
    will be called with two arguments: the session and the instance of the model that triggered the flush.
    """

    def decorator(func: Callable[P, None]) -> Callable[P, None]:
        _before_flush_hooks[(model, state)].append(func)  # type: ignore[arg-type]
        return func

    return decorator


def before_flush_trigger(
    *models: Type[Base],
) -> Callable[[Callable[P, None]], Callable[P, None]]:
    """
    Decorator to register a function to be triggered if any of the given models are added, deleted or modified.

    Each decorated function will be called with a single argument: the session. It will be called once for each
    flush, even if multiple models are added, deleted or modified.
    """

    def decorator(func: Callable[P, None]) -> Callable[P, None]:
        _before_flush_trigger_hooks[func].update(models)  # type: ignore[index]
        return func

    return decorator


def _fire_listeners(
    listening_for: State,
    session: Session,
    triggers: Dict[Tuple[Type[Base], ...], Triggers],
    instance_filter: Optional[Callable[[Base], bool]] = None,
) -> None:
    def default_instance_filter(_: Base) -> bool:
        return True

    if instance_filter is None:
        instance_filter = default_instance_filter

    hooks = {
        model: listeners
        for (model, state), listeners in _before_flush_hooks.items()
        if state == State.ANY or state == listening_for
    }
    instances = getattr(session, listening_for.value)
    for instance in instances:
        for model, listeners in hooks.items():
            if isinstance(instance, model) and instance_filter(instance):
                for listener in listeners:
                    listener(session, instance)
        for models, trigger in triggers.items():
            if (
                isinstance(instance, models)
                and not trigger.triggered
                and instance_filter(instance)
            ):
                trigger.triggered = True


@dataclass
class Triggers:
    hook: Callable[[Session], None]
    triggered: bool = False


def before_flush_event_listener(
    session: Session, flush_context: UOWTransaction, instances: Optional[List[object]]
) -> None:
    triggers = {
        tuple(models): Triggers(hook=hook)
        for hook, models in _before_flush_trigger_hooks.items()
    }

    _fire_listeners(State.NEW, session, triggers)
    _fire_listeners(State.DELETED, session, triggers)
    _fire_listeners(State.DIRTY, session, triggers, before_flush_dirty_filter)

    for models, trigger in triggers.items():
        if trigger.triggered:
            trigger.hook(session)


def before_flush_dirty_filter(instance: Base) -> bool:
    # Provide escape hatch for cases where we don't want to trigger the listener.
    supressed = getattr(instance, "_suppress_configuration_changes", False)
    if supressed:
        return False
    return directly_modified(instance)


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


@before_flush_trigger(Library, ExternalIntegration, Collection, ConfigurationSetting)
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


@before_flush(LicensePool, State.DELETED)
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


@before_flush(Equivalency, State.DELETED)
def equivalency_coverage_reset_on_equivalency_delete(
    session: Session, target: Equivalency
) -> None:
    """On equivalency delete reset the coverage records of ANY ids touching
    the deleted identifiers
    """
    EquivalencyCoverageQueries.add_coverage_for_identifiers_chain(
        [target.input, target.output], _db=session
    )


@before_flush(Identifier, State.NEW)
def recursive_equivalence_on_identifier_create(
    session: Session, instance: Identifier
) -> None:
    """Whenever an Identifier is created we must atleast have the 'self, self'
    recursion available in the Recursives table else the queries will be incorrect"""
    session.add(
        RecursiveEquivalencyCache(parent_identifier=instance, identifier=instance)
    )


@before_flush(Work, State.NEW)
@before_flush(LicensePool, State.NEW)
def add_work_to_customlists(
    session: Session, instance: Union[Work, LicensePool]
) -> None:
    """Whenever a Work or LicensePool is created we must add it to the custom lists
    for its collection"""
    add_work_to_customlists_for_collection(instance)
