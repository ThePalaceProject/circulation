from typing import Callable, List, Optional
from unittest.mock import MagicMock, PropertyMock, call

import pytest
from sqlalchemy.orm import Session

from core.model import Base
from core.model.before_flush_decorator import BeforeFlushListener, ListenerState


@pytest.fixture
def before_flush_decorator() -> BeforeFlushListener:
    """Return a BeforeFlushListener object."""
    return BeforeFlushListener()


@pytest.fixture
def create_session() -> Callable[..., Session]:
    def create(
        new: Optional[List[Base]] = None,
        deleted: Optional[List[Base]] = None,
        dirty: Optional[List[Base]] = None,
    ) -> Session:
        new = new or []
        deleted = deleted or []
        dirty = dirty or []
        session = MagicMock(spec=Session)
        type(session).new = PropertyMock(return_value=new)
        type(session).deleted = PropertyMock(return_value=deleted)
        type(session).dirty = PropertyMock(return_value=dirty)
        return session

    return create


def test_decorator_filters_on_state(
    before_flush_decorator: BeforeFlushListener,
    create_session: Callable[..., Session],
):
    mock = MagicMock()
    instance = MagicMock(spec=Base)
    before_flush_decorator.before_flush(model=Base, state=ListenerState.new)(mock)

    # Deleted instance, so the listener should not be called.
    session = create_session(deleted=[instance])
    before_flush_decorator.before_flush_event_listener(session)
    mock.assert_not_called()

    # Dirty instance, so the listener should not be called.
    session = create_session(dirty=[instance])
    before_flush_decorator.before_flush_event_listener(session)
    mock.assert_not_called()

    # New instance, so the listener should be called.
    session = create_session(new=[instance])
    before_flush_decorator.before_flush_event_listener(session)
    mock.assert_called_once_with(session, instance)


def test_decorator_called_for_each_instance(
    before_flush_decorator: BeforeFlushListener,
    create_session: Callable[..., Session],
):
    mock = MagicMock()
    instance1 = MagicMock(spec=Base)
    instance2 = MagicMock(spec=Base)

    before_flush_decorator.before_flush(model=Base, state=ListenerState.deleted)(mock)
    session = create_session(deleted=[instance1, instance2])
    before_flush_decorator.before_flush_event_listener(session)
    mock.assert_has_calls([call(session, instance1), call(session, instance2)])


def test_decorator_oneshot(
    before_flush_decorator: BeforeFlushListener,
    create_session: Callable[..., Session],
):
    mock = MagicMock()
    instance1 = MagicMock(spec=Base)
    instance2 = MagicMock(spec=Base)

    before_flush_decorator.before_flush(
        model=(Base,), state=ListenerState.deleted, one_shot=True
    )(mock)
    session = create_session(deleted=[instance1, instance2])
    before_flush_decorator.before_flush_event_listener(session)

    # The listener should only be called once, even though there are two instances.
    mock.assert_called_once_with(session)

    # However, the listener should be called again if the session is flushed again.
    before_flush_decorator.before_flush_event_listener(session)
    assert mock.call_count == 2


def test_dirty_instances_only_called_for_directly_modified(
    before_flush_decorator: BeforeFlushListener,
    create_session: Callable[..., Session],
):
    mock = MagicMock()
    instance1 = MagicMock(spec=Base)
    instance2 = MagicMock(spec=Base)

    session = create_session(dirty=[instance1, instance2])
    type(session).is_modified = MagicMock(  # type: ignore[method-assign]
        side_effect=lambda x, include_collections: x == instance2
    )
    before_flush_decorator.before_flush(model=Base, state=ListenerState.dirty)(mock)
    before_flush_decorator.before_flush_event_listener(session)

    # The listener should only be called for instance2, since instance1 is not directly modified.
    type(session).is_modified.assert_has_calls(  # type: ignore[attr-defined]
        [
            call(instance1, include_collections=False),
            call(instance2, include_collections=False),
        ]
    )
    mock.assert_called_once_with(session, instance2)


def test_filter_not_called_for_new_or_deleted(
    before_flush_decorator: BeforeFlushListener,
    create_session: Callable[..., Session],
):
    mock = MagicMock()
    instance1 = MagicMock(spec=Base)
    instance2 = MagicMock(spec=Base)
    instance3 = MagicMock(spec=Base)

    session = create_session(new=[instance1], deleted=[instance2], dirty=[instance3])
    type(session).is_modified = MagicMock()  # type: ignore[method-assign]

    before_flush_decorator.before_flush(model=Base, state=ListenerState.new)(mock)
    before_flush_decorator.before_flush(model=Base, state=ListenerState.deleted)(mock)
    before_flush_decorator.before_flush(model=Base, state=ListenerState.dirty)(mock)

    before_flush_decorator.before_flush_event_listener(session)

    type(session).is_modified.assert_called_once_with(  # type: ignore[attr-defined]
        instance3, include_collections=False
    )


def test_instances_can_suppress_listeners(
    before_flush_decorator: BeforeFlushListener,
    create_session: Callable[..., Session],
):
    mock = MagicMock()
    instance1 = MagicMock(spec=Base)
    type(instance1)._suppress_before_flush_listeners = PropertyMock(return_value=True)
    instance2 = MagicMock(spec=Base)

    session = create_session(new=[instance1, instance2])
    before_flush_decorator.before_flush(model=Base, state=ListenerState.new)(mock)
    before_flush_decorator.before_flush_event_listener(session)
    mock.assert_called_once_with(session, instance2)
