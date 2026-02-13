from __future__ import annotations

import logging
from collections.abc import Generator, Mapping
from contextlib import contextmanager, nullcontext
from typing import Any, Literal, Self

from frozendict import frozendict
from psycopg2._range import NumericRange
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext import mutable
from sqlalchemy.orm import Session

# This is the lock ID used to ensure that only one circulation manager
# initializes or migrates the database at a time.
LOCK_ID_DB_INIT = 1000000001


@contextmanager
def pg_advisory_lock(
    connectable: Engine | Connection, lock_id: int
) -> Generator[Connection]:
    """
    Application wide locking based on Lock IDs.

    :param connectable: An Engine or Connection to use for locking. If an Engine
        is provided, a new connection is created and managed by this context manager.
        If a Connection is provided, it is used directly.
    :param lock_id: The lock ID to acquire.
    """
    connection_ctx = (
        connectable.connect()
        if isinstance(connectable, Engine)
        else nullcontext(connectable)
    )
    with connection_ctx as connection:
        # Create the lock
        connection.execute(text(f"SELECT pg_advisory_lock({lock_id});"))
        try:
            yield connection
        except IntegrityError:
            # If there was an IntegrityError, and we are in a transaction,
            # we need to roll it back before we are able to release the lock.
            transaction = connection.get_transaction()
            if transaction is not None:
                transaction.rollback()
            raise
        finally:
            # Close the lock
            connection.execute(text(f"SELECT pg_advisory_unlock({lock_id});"))


def flush(db: Session) -> None:
    """Flush the database connection unless it's known to already be flushing."""
    is_flushing = False
    if hasattr(db, "_flushing"):
        # This is a regular database session.
        is_flushing = db._flushing
    elif hasattr(db, "registry"):
        # This is a flask_scoped_session scoped session.
        is_flushing = db.registry()._flushing
    else:
        logging.error("Unknown database connection type: %r", db)
    if not is_flushing:
        db.flush()


def create[T](
    db: Session,
    model: type[T],
    create_method: str = "",
    create_method_kwargs: Mapping[str, Any] | None = None,
    **kwargs: Any,
) -> tuple[T, Literal[True]]:
    kwargs.update(create_method_kwargs or {})
    created = getattr(model, create_method, model)(**kwargs)
    db.add(created)
    flush(db)
    return created, True


def get_one[T](
    db: Session,
    model: type[T],
    on_multiple: Literal["interchangeable"] | Literal["error"] = "error",
    constraint: Any = None,
    **kwargs: Any,
) -> T | None:
    """Gets an object from the database based on its attributes.

    :param constraint: A single clause that can be passed into
        `sqlalchemy.Query.filter` to limit the object that is returned.
    :return: object or None
    """
    q = db.query(model).filter_by(**kwargs)
    if constraint is not None:
        q = q.filter(constraint)

    if on_multiple == "interchangeable":
        # These records are interchangeable so we can use
        # whichever one we want.
        #
        # This may be a sign of a problem somewhere else. A
        # database-level constraint might be useful.
        q = q.limit(1)

    return q.one_or_none()


log = logging.getLogger(__name__)


def get_one_or_create[T](
    db: Session,
    model: type[T],
    create_method: str = "",
    create_method_kwargs: Mapping[str, Any] | None = None,
    **kwargs: Any,
) -> tuple[T, bool]:
    one = get_one(db, model, **kwargs)
    if one:
        return one, False
    else:
        try:
            with db.begin_nested():
                # These kwargs are supported by get_one() but not by create().
                get_one_keys = ["on_multiple", "constraint"]
                filtered_kwargs = {
                    k: v for k, v in kwargs.items() if k not in get_one_keys
                }
                obj = create(
                    db, model, create_method, create_method_kwargs, **filtered_kwargs
                )
                return obj
        except IntegrityError as e:
            log.debug(
                "INTEGRITY ERROR on %r %r, %r: %r",
                model,
                create_method_kwargs,
                kwargs,
                e,
            )
            one = get_one(db, model, **kwargs)
            if one is None:
                # If we still don't have an object, raise the error.
                log.error(
                    "Unable to retrieve with get_one after IntegrityError with %r %r",
                    model,
                    kwargs,
                )
                raise
            return one, False


def numericrange_to_string(r: NumericRange | None) -> str:
    """Helper method to convert a NumericRange to a human-readable string."""
    if not r:
        return ""
    lower = r.lower
    upper = r.upper
    if upper is None and lower is None:
        return ""
    if upper is None:
        return str(lower)
    if lower is None:
        return str(upper)
    # Currently this function only supports integer ranges, but NumericRange
    # supports floats as well, so we assert that the values are integers, so
    # this function fails if we ever start using floats.
    assert isinstance(lower, int) and isinstance(upper, int)
    if not r.upper_inc:
        upper -= 1
    if not r.lower_inc:
        lower += 1
    if upper == lower:
        return str(lower)
    return f"{lower}-{upper}"


NumericRangeTuple = tuple[float | None, float | None]


def numericrange_to_tuple(r: NumericRange | None) -> NumericRangeTuple:
    """Helper method to normalize NumericRange into a tuple."""
    if r is None:
        return (None, None)
    lower = r.lower
    upper = r.upper
    if lower and not r.lower_inc:
        lower += 1
    if upper and not r.upper_inc:
        upper -= 1
    return lower, upper


def tuple_to_numericrange(t: NumericRangeTuple | None) -> NumericRange | None:
    """Helper method to convert a tuple to an inclusive NumericRange."""
    if not t:
        return None
    return NumericRange(t[0], t[1], "[]")


class MutableDict(mutable.MutableDict):
    """
    A MutableDict that will accept a frozen dictionary as a value coercing
    it to a regular dictionary.
    """

    @classmethod
    def coerce(cls, key: str, value: Any) -> Self:
        """Convert frozendict to a dict and pass to super class."""
        if isinstance(value, frozendict):
            value = dict(value)

        return super().coerce(key, value)  # type: ignore[no-any-return]
