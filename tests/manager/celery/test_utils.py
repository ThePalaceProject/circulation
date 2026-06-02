from collections.abc import Generator
from contextlib import contextmanager

import pytest
from celery import Celery

from palace.util.exceptions import PalaceTypeError

from palace.manager.celery.task import Task
from palace.manager.celery.utils import (
    ModelNotFoundError,
    load_from_id,
    signature_with,
    validate_not_none,
)
from palace.manager.sqlalchemy.model.collection import Collection
from tests.fixtures.database import DatabaseTransactionFixture

# A standalone Celery app and bound tasks used only to exercise
# ``signature_with``. Using a local app (rather than the worker fixture)
# keeps these tests fast and isolated: we just need registered bound tasks and a
# simulated request, which we provide via ``push_request`` below.
_signature_app = Celery("test_signature_with", task_cls=Task)


@_signature_app.task(bind=True)
def _sample_task(
    task: Task, collection_id: int, batch_size: int = 100, after_id: int | None = None
) -> None:
    """A task whose parameters are all positional-or-keyword."""


@_signature_app.task(bind=True)
def _keyword_only_task(
    task: Task, collection_id: int, *, offset: int = 0, batch_size: int = 50
) -> None:
    """A task with keyword-only parameters following ``*``."""


@contextmanager
def _running(task: Task, *args: object, **kwargs: object) -> Generator[None]:
    """Simulate ``task`` running with the given args/kwargs in its request."""
    task.push_request(args=args, kwargs=kwargs)
    try:
        yield
    finally:
        task.pop_request()


class TestLoadFromId:
    def test_load(self, db: DatabaseTransactionFixture) -> None:
        collection = db.collection()
        loaded = load_from_id(db.session, Collection, collection.id)
        assert isinstance(loaded, Collection)
        assert loaded is collection

    def test_load_not_found(self, db: DatabaseTransactionFixture) -> None:
        collection = db.collection()
        collection_id = collection.id
        db.session.delete(collection)

        with pytest.raises(
            ModelNotFoundError, match=f"Collection with id '{collection_id}' not found."
        ):
            load_from_id(db.session, Collection, collection_id)


class TestValidateNotNone:
    def test_validate_not_none(self) -> None:
        assert validate_not_none(1, "Should not be None") == 1
        assert validate_not_none("test", "Should not be None") == "test"

        with pytest.raises(PalaceTypeError, match="Should not be None"):
            validate_not_none(None, "Should not be None")


class TestSignatureWith:
    def test_keyword_args_with_override(self) -> None:
        # Originally invoked with all arguments as keywords; overriding one
        # leaves the rest intact.
        with _running(_sample_task, collection_id=42, batch_size=100, after_id=7):
            signature = signature_with(_sample_task, after_id=1234)

        assert signature.task == _sample_task.name
        assert signature.args == ()
        assert signature.kwargs == {
            "collection_id": 42,
            "batch_size": 100,
            "after_id": 1234,
        }

    def test_positional_args_with_override(self) -> None:
        # Originally invoked positionally (as ``recalculate_hold_queue_collection``
        # does). The override must update the right parameter by name and must not
        # produce a duplicate positional + keyword for that parameter.
        with _running(_sample_task, 42, 100, 999):
            signature = signature_with(_sample_task, after_id=1234)

        assert signature.args == ()
        assert signature.kwargs == {
            "collection_id": 42,
            "batch_size": 100,
            "after_id": 1234,
        }

    def test_mixed_positional_and_keyword_args(self) -> None:
        with _running(_sample_task, 42, batch_size=50):
            signature = signature_with(_sample_task, after_id=1234)

        assert signature.args == ()
        assert signature.kwargs == {
            "collection_id": 42,
            "batch_size": 50,
            "after_id": 1234,
        }

    def test_no_overrides_reproduces_original_arguments(self) -> None:
        with _running(_sample_task, 42, after_id=7):
            signature = signature_with(_sample_task)

        assert signature.args == ()
        assert signature.kwargs == {"collection_id": 42, "after_id": 7}

    def test_override_parameter_that_used_its_default(self) -> None:
        # ``after_id`` was not supplied originally (it relied on its default);
        # an override for it still lands in the signature.
        with _running(_sample_task, collection_id=42):
            signature = signature_with(_sample_task, after_id=1234)

        assert signature.kwargs == {"collection_id": 42, "after_id": 1234}

    def test_keyword_only_parameters(self) -> None:
        # Keyword-only parameters never arrive via request.args, so they are
        # carried through request.kwargs and remain overridable by name.
        with _running(_keyword_only_task, 42, offset=10, batch_size=50):
            signature = signature_with(_keyword_only_task, offset=20)

        assert signature.args == ()
        assert signature.kwargs == {
            "collection_id": 42,
            "offset": 20,
            "batch_size": 50,
        }
