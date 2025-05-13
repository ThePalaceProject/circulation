"""
Helper functions for use in Celery tasks
"""

from typing import TypeVar

from sqlalchemy.orm import Session

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.sqlalchemy.util import get_one

T = TypeVar("T")


def load_from_id(db: Session, model: type[T], id: int) -> T:
    """
    Load an instance of a model from the database using its ID.

    Useful in Celery tasks, where we often pass IDs into tasks. Since
    these tasks are asynchronous, we need to load the instance from the database
    and its possible the instance has been deleted or modified in the meantime.

    This function will raise a PalaceValueError if the instance is not found.
    """

    instance = get_one(db, model, id=id)
    return validate_not_none(instance, f"{model.__name__} with id '{id}' not found.")


def validate_not_none(value: T | None, message: str) -> T:
    """
    Validate that a value is not None.

    Raises a PalaceValueError if the value is None.
    """
    if value is None:
        raise PalaceValueError(message)
    return value
