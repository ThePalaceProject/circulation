"""Migrate announcements

Revision ID: 3d380776c1bf
Revises: c471f553249b
Create Date: 2023-07-11 17:22:56.596888+00:00

"""
import json
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Connection, Row

from alembic import op

# revision identifiers, used by Alembic.
revision = "3d380776c1bf"
down_revision = "c471f553249b"
branch_labels = None
depends_on = None


def create_announcement(
    connection: Connection, setting: Optional[Row], library_id: Optional[int] = None
) -> None:
    if setting and setting.value:
        announcements = json.loads(setting.value)
        for announcement in announcements:
            connection.execute(
                "insert into announcements (id, content, start, finish, library_id) values (%s, %s, %s, %s, %s)",
                (
                    announcement["id"],
                    announcement["content"],
                    announcement["start"],
                    announcement["finish"],
                    library_id,
                ),
            )


def upgrade() -> None:
    # Create table for announcements
    op.create_table(
        "announcements",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("content", sa.Unicode(), nullable=False),
        sa.Column("start", sa.Date(), nullable=False),
        sa.Column("finish", sa.Date(), nullable=False),
        sa.Column("library_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["library_id"],
            ["libraries.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_announcements_library_id"),
        "announcements",
        ["library_id"],
        unique=False,
    )

    # Migrate announcements from configuration settings
    connection = op.get_bind()
    libraries = connection.execute("select id, short_name from libraries")

    # Migrate library announcements
    for library in libraries:
        setting = connection.execute(
            "select cs.value from configurationsettings cs "
            "where cs.library_id = (%s) and cs.key = 'announcements' and cs.external_integration_id IS NULL",
            (library.id,),
        ).fetchone()
        create_announcement(connection, setting, library.id)

    # Migrate global announcements
    setting = connection.execute(
        "select cs.value from configurationsettings cs "
        "where cs.key = 'global_announcements' and cs.library_id IS NULL and cs.external_integration_id IS NULL",
    ).fetchone()
    create_announcement(connection, setting)


def downgrade() -> None:
    op.drop_index(op.f("ix_announcements_library_id"), table_name="announcements")
    op.drop_table("announcements")
