from __future__ import annotations

from typing import Any, List

import sqlalchemy as sa


def pg_update_enum(
    op: Any,
    table: str,
    column: str,
    enum_name: str,
    old_values: List[str],
    new_values: List[str],
) -> None:
    """
    Alembic migration helper function to update an enum type.

    Alembic performs its updates within a transaction, and Postgres does not allow
    the addition of new enum values within a transaction. In order to be able to
    update an enum within a tranaction this function creates a temporary enum type
    and uses it to update the column. It then drops the old enum type and creates
    the new enum type. Finally, it updates the column to use the new enum type and
    drops the temporary enum type.

    This is a cleaned up version of the code from:
    https://stackoverflow.com/questions/14845203/altering-an-enum-field-using-alembic/45615354#45615354
    """

    # Create SA Enum objects for the enums
    tmp_enum_name = f"_tmp_{enum_name}"
    tmp_enum = sa.Enum(*new_values, name=tmp_enum_name)
    old_enum = sa.Enum(*old_values, name=enum_name)
    new_enum = sa.Enum(*new_values, name=enum_name)

    # Create the tmp enum type
    tmp_enum.create(op.get_bind())

    # Alter the column to use the tmp enum type
    op.alter_column(
        table,
        column,
        type_=tmp_enum,
        postgresql_using=f"{column}::text::{tmp_enum_name}",
    )

    # Drop the old enum type
    old_enum.drop(op.get_bind())

    # Create the new enum type
    new_enum.create(op.get_bind())

    # Alter the column to use the new enum type
    op.alter_column(
        table, column, type_=new_enum, postgresql_using=f"{column}::text::{enum_name}"
    )

    # Drop the tmp enum type
    tmp_enum.drop(op.get_bind())


def drop_enum(op: Any, enum_name: str, checkfirst: bool = True) -> None:
    """
    Alembic migration helper function to drop an enum type.
    """
    sa.Enum(name=enum_name).drop(op.get_bind(), checkfirst=checkfirst)
