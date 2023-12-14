from __future__ import annotations

from collections.abc import Callable
from copy import copy
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, ParamSpec

from sqlalchemy.orm import Session
from sqlalchemy.orm.unitofwork import UOWTransaction

if TYPE_CHECKING:
    from core.model import Base

P = ParamSpec("P")


class ListenerState(Enum):
    """These are the states for which we can register listeners."""

    new = "new"
    deleted = "deleted"
    dirty = "dirty"
    any = "any"


class BeforeFlushListener:
    @dataclass
    class Listeners:
        """
        Internal datastructure to keep track of the listener implementations that
        are registered with the BeforeFlushListener.
        """

        # Tuple of models that the listener is registered for.
        models: tuple[type[Base], ...]
        # State that the listener is registered for.
        state: ListenerState
        # If True, the listener will only be called once.
        one_shot: bool
        # The function to call when the listener is triggered.
        callback: Callable[..., None]
        # If True, the listener has been triggered once.
        one_shot_triggered: bool = False

    def __init__(self):
        self._listeners: list[BeforeFlushListener.Listeners] = []

    def before_flush(
        self,
        model: type[Base] | tuple[type[Base], ...],
        state: ListenerState = ListenerState.any,
        one_shot: bool = False,
    ) -> Callable[[Callable[P, None]], Callable[P, None]]:
        """
        Decorator to register a function to be called before a flush.

        The decorated function will be called when a model of the given type is in the given state. If the
        state is any, the function will be called for all states. If the model is a tuple, the function will be called
        for all models in the tuple.

        The listener func will be called with the session as the first argument. If one_shot is False, the listener
        func will be called with the instance as the second argument. If one_shot is True, the listener func will be
        called without the instance as the second argument.

        :param model: The model to register the listener for. Either a single model or a tuple of models.
        :param state: The state to register the listener for. This is a member of ListenerState. If the state is any,
        the function will be called for all states.
        :param one_shot: If True, the listener will only be called once.
        """

        def decorator(func: Callable[P, None]) -> Callable[P, None]:
            models = model if isinstance(model, tuple) else (model,)
            self._listeners.append(
                self.Listeners(
                    models=models, state=state, one_shot=one_shot, callback=func
                )
            )
            return func

        return decorator

    @classmethod
    def _invoke_listeners(
        cls,
        listening_for: ListenerState,
        session: Session,
        listeners: list[BeforeFlushListener.Listeners],
        instance_filter: Callable[[Session, Base], bool] | None = None,
    ) -> None:
        """
        Invoke the listeners for the given state.

        An instance can suppress the listener by setting the _suppress_before_flush_listeners attribute to True. This
        is used by Lanes to avoid triggering the listener when a Lane is modified by the update lane size script as
        this was causing performance issues.

        If instance_filter is provided, only invoke the listeners for instances where instance_filter(instance)
        returns True. This lets us filter out instances that have been indirectly modified when scanning for
        dirty instances.
        """
        if instance_filter is None:
            instance_filter = cls._filter_default

        instances = getattr(session, listening_for.value)

        for instance in instances:
            suppressed = getattr(instance, "_suppress_before_flush_listeners", False)
            if suppressed:
                continue
            for listener in listeners:
                if listener.one_shot and listener.one_shot_triggered:
                    continue

                if (
                    listener.state != ListenerState.any
                    and listening_for != listener.state
                ):
                    continue

                if isinstance(instance, listener.models) and instance_filter(
                    session, instance
                ):
                    if listener.one_shot:
                        listener.callback(session)
                        listener.one_shot_triggered = True
                    else:
                        listener.callback(session, instance)

    def before_flush_event_listener(
        self,
        session: Session,
        _flush_context: UOWTransaction | None = None,
        _instances: list[object] | None = None,
    ) -> None:
        """
        SQLAlchemy event listener that is called before a flush. This is where we invoke the listeners that have been
        registered with the BeforeFlushListener. This needs to be registered with SQLAlchemy using the 'before_flush'
        event, so that we can get access to the session and the instances that are about to be flushed.
        """
        # Create a copy of the listeners, so we can trigger the one-shot listeners without affecting the original list.
        listeners = [copy(listener) for listener in self._listeners]

        self._invoke_listeners(ListenerState.new, session, listeners)
        self._invoke_listeners(ListenerState.deleted, session, listeners)
        self._invoke_listeners(
            ListenerState.dirty, session, listeners, self._filter_directly_modified
        )

    @classmethod
    def _filter_default(cls, _session: Session, _instance: Base) -> bool:
        """
        Default filter for instances. This is used when no filter is provided to the listener decorator. It just
        returns True, so all instances will trigger the listener.
        """
        return True

    @staticmethod
    def _filter_directly_modified(session: Session, instance: Base):
        """Return True only if `obj` has itself been modified, as opposed to having an object added or removed to
        one of its associated collections.

        See the SQLAlchemy docs:
        https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session.is_modified
        """
        return session.is_modified(instance, include_collections=False)


Listener = BeforeFlushListener()
