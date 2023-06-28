"""Remove adobe vendor id tables

Revision ID: f08f9c6bded6
Revises: 28717fc6e50f
Create Date: 2023-06-28 19:07:27.735625+00:00

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "f08f9c6bded6"
down_revision = "28717fc6e50f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index(
        "ix_drmdeviceidentifiers_credential_id", table_name="drmdeviceidentifiers"
    )
    op.drop_index(
        "ix_drmdeviceidentifiers_device_identifier", table_name="drmdeviceidentifiers"
    )
    op.drop_table("drmdeviceidentifiers")
    op.drop_index(
        "ix_delegatedpatronidentifiers_library_uri",
        table_name="delegatedpatronidentifiers",
    )
    op.drop_index(
        "ix_delegatedpatronidentifiers_patron_identifier",
        table_name="delegatedpatronidentifiers",
    )
    op.drop_index(
        "ix_delegatedpatronidentifiers_type", table_name="delegatedpatronidentifiers"
    )
    op.drop_table("delegatedpatronidentifiers")


def downgrade() -> None:
    op.create_table(
        "delegatedpatronidentifiers",
        sa.Column("id", sa.INTEGER(), autoincrement=True, nullable=False),
        sa.Column("type", sa.VARCHAR(length=255), autoincrement=False, nullable=True),
        sa.Column(
            "library_uri", sa.VARCHAR(length=255), autoincrement=False, nullable=True
        ),
        sa.Column(
            "patron_identifier",
            sa.VARCHAR(length=255),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "delegated_identifier", sa.VARCHAR(), autoincrement=False, nullable=True
        ),
        sa.PrimaryKeyConstraint("id", name="delegatedpatronidentifiers_pkey"),
        sa.UniqueConstraint(
            "type",
            "library_uri",
            "patron_identifier",
            name="delegatedpatronidentifiers_type_library_uri_patron_identifi_key",
        ),
    )
    op.create_index(
        "ix_delegatedpatronidentifiers_type",
        "delegatedpatronidentifiers",
        ["type"],
        unique=False,
    )
    op.create_index(
        "ix_delegatedpatronidentifiers_patron_identifier",
        "delegatedpatronidentifiers",
        ["patron_identifier"],
        unique=False,
    )
    op.create_index(
        "ix_delegatedpatronidentifiers_library_uri",
        "delegatedpatronidentifiers",
        ["library_uri"],
        unique=False,
    )
    op.create_table(
        "drmdeviceidentifiers",
        sa.Column("id", sa.INTEGER(), autoincrement=True, nullable=False),
        sa.Column("credential_id", sa.INTEGER(), autoincrement=False, nullable=True),
        sa.Column(
            "device_identifier",
            sa.VARCHAR(length=255),
            autoincrement=False,
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["credential_id"],
            ["credentials.id"],
            name="drmdeviceidentifiers_credential_id_fkey",
        ),
        sa.PrimaryKeyConstraint("id", name="drmdeviceidentifiers_pkey"),
    )
    op.create_index(
        "ix_drmdeviceidentifiers_device_identifier",
        "drmdeviceidentifiers",
        ["device_identifier"],
        unique=False,
    )
    op.create_index(
        "ix_drmdeviceidentifiers_credential_id",
        "drmdeviceidentifiers",
        ["credential_id"],
        unique=False,
    )
