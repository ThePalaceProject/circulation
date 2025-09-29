"""Playtime tables have no dependencies.

Revision ID: 3e43ed59f256
Revises: 9d2dccb0d6ff
Create Date: 2024-03-18 01:34:28.381129+00:00

"""

from functools import cache

import sqlalchemy as sa
from alembic import op
from sqlalchemy.engine import Connection
from sqlalchemy.orm.session import Session

from palace.manager.sqlalchemy.model.identifier import Identifier, isbn_for_identifier
from palace.manager.sqlalchemy.model.time_tracking import _title_for_identifier
from palace.manager.sqlalchemy.util import get_one

# revision identifiers, used by Alembic.
revision = "3e43ed59f256"
down_revision = "9d2dccb0d6ff"
branch_labels = None
depends_on = None


def upgrade() -> None:
    session = Session(bind=op.get_bind())
    conn = session.connection()

    op.add_column(
        "playtime_entries", sa.Column("identifier_str", sa.String(), nullable=True)
    )
    op.add_column(
        "playtime_entries", sa.Column("collection_name", sa.String(), nullable=True)
    )
    op.add_column(
        "playtime_entries", sa.Column("library_name", sa.String(), nullable=True)
    )

    # Migrate the existing playtime records before we set the new columns to not nullable.
    update_playtime_entries(conn)

    op.alter_column(
        "playtime_entries", "identifier_str", existing_type=sa.String(), nullable=False
    )
    op.alter_column(
        "playtime_entries", "collection_name", existing_type=sa.String(), nullable=False
    )
    op.alter_column(
        "playtime_entries", "library_name", existing_type=sa.String(), nullable=False
    )

    op.alter_column(
        "playtime_entries", "identifier_id", existing_type=sa.INTEGER(), nullable=True
    )
    op.alter_column(
        "playtime_entries", "collection_id", existing_type=sa.INTEGER(), nullable=True
    )
    op.alter_column(
        "playtime_entries", "library_id", existing_type=sa.INTEGER(), nullable=True
    )
    op.drop_constraint(
        "playtime_entries_identifier_id_collection_id_library_id_tra_key",
        "playtime_entries",
        type_="unique",
    )
    op.drop_constraint(
        "playtime_entries_collection_id_fkey", "playtime_entries", type_="foreignkey"
    )
    op.drop_constraint(
        "playtime_entries_identifier_id_fkey", "playtime_entries", type_="foreignkey"
    )
    op.drop_constraint(
        "playtime_entries_library_id_fkey", "playtime_entries", type_="foreignkey"
    )

    op.create_unique_constraint(
        "unique_playtime_entry",
        "playtime_entries",
        ["tracking_id", "identifier_str", "collection_name", "library_name"],
    )
    op.create_foreign_key(
        "playtime_entries_identifier_id_fkey",
        "playtime_entries",
        "identifiers",
        ["identifier_id"],
        ["id"],
        onupdate="CASCADE",
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "playtime_entries_collection_id_fkey",
        "playtime_entries",
        "collections",
        ["collection_id"],
        ["id"],
        onupdate="CASCADE",
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "playtime_entries_library_id_fkey",
        "playtime_entries",
        "libraries",
        ["library_id"],
        ["id"],
        onupdate="CASCADE",
        ondelete="SET NULL",
    )

    op.add_column("playtime_summaries", sa.Column("title", sa.String(), nullable=True))
    op.add_column("playtime_summaries", sa.Column("isbn", sa.String(), nullable=True))
    op.alter_column(
        "playtime_summaries", "collection_id", existing_type=sa.INTEGER(), nullable=True
    )
    op.alter_column(
        "playtime_summaries", "library_id", existing_type=sa.INTEGER(), nullable=True
    )
    op.drop_constraint(
        "playtime_summaries_identifier_str_collection_name_library_n_key",
        "playtime_summaries",
        type_="unique",
    )
    op.create_unique_constraint(
        "unique_playtime_summary",
        "playtime_summaries",
        ["timestamp", "identifier_str", "collection_name", "library_name"],
    )

    # Update ISBN, where available, and title in summary table.
    update_summary_isbn_and_title(session)


def downgrade() -> None:
    op.drop_constraint("unique_playtime_summary", "playtime_summaries", type_="unique")
    op.create_unique_constraint(
        "playtime_summaries_identifier_str_collection_name_library_n_key",
        "playtime_summaries",
        ["identifier_str", "collection_name", "library_name", "timestamp"],
    )
    op.alter_column(
        "playtime_summaries",
        "collection_id",
        existing_type=sa.INTEGER(),
        nullable=False,
    )
    op.alter_column(
        "playtime_summaries", "library_id", existing_type=sa.INTEGER(), nullable=False
    )
    op.drop_column("playtime_summaries", "isbn")
    op.drop_column("playtime_summaries", "title")

    op.drop_constraint(
        "playtime_entries_identifier_id_fkey", "playtime_entries", type_="foreignkey"
    )
    op.drop_constraint(
        "playtime_entries_collection_id_fkey", "playtime_entries", type_="foreignkey"
    )
    op.drop_constraint(
        "playtime_entries_library_id_fkey", "playtime_entries", type_="foreignkey"
    )
    op.create_foreign_key(
        "playtime_entries_library_id_fkey",
        "playtime_entries",
        "libraries",
        ["library_id"],
        ["id"],
        onupdate="CASCADE",
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "playtime_entries_identifier_id_fkey",
        "playtime_entries",
        "identifiers",
        ["identifier_id"],
        ["id"],
        onupdate="CASCADE",
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "playtime_entries_collection_id_fkey",
        "playtime_entries",
        "collections",
        ["collection_id"],
        ["id"],
        onupdate="CASCADE",
        ondelete="CASCADE",
    )
    op.drop_constraint("unique_playtime_entry", "playtime_entries", type_="unique")
    op.create_unique_constraint(
        "playtime_entries_identifier_id_collection_id_library_id_tra_key",
        "playtime_entries",
        ["identifier_id", "collection_id", "library_id", "tracking_id"],
    )
    op.alter_column(
        "playtime_entries", "library_id", existing_type=sa.INTEGER(), nullable=False
    )
    op.alter_column(
        "playtime_entries", "collection_id", existing_type=sa.INTEGER(), nullable=False
    )
    op.alter_column(
        "playtime_entries", "identifier_id", existing_type=sa.INTEGER(), nullable=False
    )
    op.drop_column("playtime_entries", "library_name")
    op.drop_column("playtime_entries", "collection_name")
    op.drop_column("playtime_entries", "identifier_str")


def update_summary_isbn_and_title(session: Session) -> None:
    """Update existing playtime summary records in the database."""
    conn = session.connection()
    rows = conn.execute(
        sa.text("SELECT id, identifier_id FROM playtime_summaries")
    ).all()

    for row in rows:
        identifier = get_one(session, Identifier, id=row.identifier_id)
        isbn = cached_isbn_lookup(identifier)
        title = cached_title_lookup(identifier)
        conn.execute(
            sa.text(
                """
                UPDATE playtime_summaries
                SET isbn = %(isbn)s, title = %(title)s
                WHERE id = %(id)s
                """
            ),
            {"id": row.id, "isbn": isbn, "title": title},
        )


@cache
def cached_isbn_lookup(identifier: Identifier) -> str | None:
    """Given an identifier, return its ISBN."""
    return isbn_for_identifier(identifier)


@cache
def cached_title_lookup(identifier: Identifier) -> str | None:
    """Given an identifier, return its title."""
    return _title_for_identifier(identifier)


def update_playtime_entries(conn: Connection) -> None:
    """Update existing playtime entries in the database."""
    rows = conn.execute(
        sa.text(
            "SELECT id, identifier_id, collection_id, library_id FROM playtime_entries"
        )
    ).all()

    for row in rows:
        conn.execute(
            sa.text(
                """
                UPDATE playtime_entries
                SET identifier_str = %(urn)s, collection_name = %(collection_name)s, library_name = %(library_name)s
                WHERE id = %(id)s
                """
            ),
            {
                "id": row.id,
                "urn": get_identifier_urn(conn, row.identifier_id),
                "collection_name": get_collection_name(conn, row.collection_id),
                "library_name": get_library_name(conn, row.library_id),
            },
        )


@cache
def get_collection_name(conn: Connection, collection_id: int) -> str:
    """Given the id of a collection, return its name."""
    return conn.execute(
        sa.text(
            """
            SELECT ic.name
            FROM collections c
            JOIN integration_configurations ic on c.integration_configuration_id = ic.id
            WHERE c.id = %s
            """
        ),
        (collection_id,),
    ).scalar_one()


@cache
def get_identifier_urn(conn: Connection, identifier_id: int) -> str:
    """Given the id of an identifier id, return its urn."""
    row = conn.execute(
        sa.text(
            """
            SELECT type, identifier
            FROM identifiers
            WHERE id = %s
            """
        ),
        (identifier_id,),
    ).one()
    return Identifier._urn_from_type_and_value(row.type, row.identifier)


@cache
def get_library_name(conn: Connection, library_id: int) -> str:
    """Given the id of a library, return its name."""
    return conn.execute(
        sa.text("SELECT name FROM libraries WHERE id = %s"), (library_id,)
    ).scalar_one()
