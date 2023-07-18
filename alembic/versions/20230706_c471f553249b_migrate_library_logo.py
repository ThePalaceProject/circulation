"""Migrate library logo

Revision ID: c471f553249b
Revises: 04bbd03bf9f1
Create Date: 2023-07-06 19:37:59.269231+00:00

"""
import logging

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "c471f553249b"
down_revision = "04bbd03bf9f1"
branch_labels = None
depends_on = None

log = logging.getLogger(f"palace.migration.{revision}")
log.setLevel(logging.INFO)
log.disabled = False


def upgrade() -> None:
    op.create_table(
        "libraries_logos",
        sa.Column("library_id", sa.Integer(), nullable=False),
        sa.Column("content", sa.LargeBinary(), nullable=False),
        sa.ForeignKeyConstraint(
            ["library_id"],
            ["libraries.id"],
        ),
        sa.PrimaryKeyConstraint("library_id"),
    )

    prefix = "data:image/png;base64,"
    connection = op.get_bind()
    libraries = connection.execute("select id, short_name from libraries")

    for library in libraries:
        setting = connection.execute(
            "select cs.value from configurationsettings cs "
            "where cs.library_id = (%s) and cs.key = 'logo'",
            (library.id,),
        ).first()
        if setting and setting.value:
            log.info(f"Library {library.short_name} has a logo, migrating it.")
            logo_str = setting.value

            # We stored the logo with a data:image prefix before, but we
            # don't need that anymore, so we remove it here.
            if logo_str.startswith(prefix):
                logo_str = logo_str[len(prefix) :]

            logo_bytes = logo_str.encode("utf-8")
            connection.execute(
                "insert into libraries_logos (library_id, content) values (%s, %s)",
                (library.id, logo_bytes),
            )
        else:
            log.info(f"Library {library.short_name} has no logo, skipping.")


def downgrade() -> None:
    op.drop_table("libraries_logos")
