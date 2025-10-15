"""
Helper functions for use in Celery tasks
"""

from sqlalchemy.orm import Session

from palace.manager.core.exceptions import PalaceTypeError, PalaceValueError
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
