"""
Helper functions for use in Celery tasks
"""

import inspect
from typing import Any

import celery
from celery import Signature
from sqlalchemy.orm import Session

from palace.util.exceptions import PalaceTypeError, PalaceValueError

from palace.manager.sqlalchemy.util import get_one


class ModelNotFoundError(PalaceValueError):
    """
    Raised when a model instance is not found in the database.
    """


def load_from_id[T](db: Session, model: type[T], id: int) -> T:
    """
    Load an instance of a model from the database using its ID.

    Useful in Celery tasks, where we often pass IDs into tasks. Since
    these tasks are asynchronous, we need to load the instance from the database
    and its possible the instance has been deleted or modified in the meantime.

    This function will raise a ModelNotFoundError if the instance is not found.
    """

    try:
        instance = get_one(db, model, id=id)
        return validate_not_none(
            instance, f"{model.__name__} with id '{id}' not found."
        )
    except PalaceTypeError as e:
        raise ModelNotFoundError(e.message)


def validate_not_none[T](value: T | None, message: str) -> T:
    """
    Validate that a value is not None.

    Raises a PalaceTypeError if the value is None.
    """
    if value is None:
        raise PalaceTypeError(message)
    return value


def signature_with(task: celery.Task, **overrides: Any) -> Signature:
    """
    Build a signature to re-queue the currently-running task, reusing its
    original arguments and applying only the given overrides.

    Tasks that paginate or batch their work re-queue themselves via
    ``task.replace(...)``. Rather than re-listing every parameter in the new
    signature (and risking silently dropping one when the signature gains a
    parameter), this fills the unchanged parameters from the current invocation
    (``task.request.args`` / ``task.request.kwargs``) and overrides only what
    the caller passes.

    Positional arguments are normalized to keyword arguments using the task's
    own signature, so an override applies to the correct parameter regardless of
    whether it was originally passed positionally or by keyword. The returned
    signature is therefore always keyword-only.

    This requires a bound task (``bind=True``), so that ``task`` is the running
    task instance and exposes its request.

    :param task: The bound, currently-running task (the ``task`` parameter of a
        ``bind=True`` task).
    :param overrides: Parameters whose values should change for the next run.
    :return: A signature re-queuing the same task with the merged arguments.
    """
    # For a bound task, ``task.run`` is a bound method, so its signature already
    # omits the bound ``task`` parameter and begins at the first real argument.
    # These positional-capable names therefore map one-to-one onto request.args.
    positional_names = [
        name
        for name, parameter in inspect.signature(task.run).parameters.items()
        if parameter.kind
        in (parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD)
    ]
    merged: dict[str, Any] = dict(zip(positional_names, task.request.args))
    merged.update(task.request.kwargs)
    merged.update(overrides)
    return task.s(**merged)
