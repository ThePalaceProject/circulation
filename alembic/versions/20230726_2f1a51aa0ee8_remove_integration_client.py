"""Remove integration client

Revision ID: 2f1a51aa0ee8
Revises: 892c8e0c89f8
Create Date: 2023-07-26 13:34:02.924885+00:00

"""
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "2f1a51aa0ee8"
down_revision = "892c8e0c89f8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("ix_datasources_integration_client_id", table_name="datasources")
    op.drop_constraint(
        "datasources_integration_client_id_fkey", "datasources", type_="foreignkey"
    )
    op.drop_column("datasources", "integration_client_id")
    op.drop_index("ix_holds_integration_client_id", table_name="holds")
    op.drop_constraint("holds_integration_client_id_fkey", "holds", type_="foreignkey")
    op.drop_column("holds", "integration_client_id")
    op.drop_index("ix_loans_integration_client_id", table_name="loans")
    op.drop_constraint("loans_integration_client_id_fkey", "loans", type_="foreignkey")
    op.drop_column("loans", "integration_client_id")
    op.drop_index(
        "ix_integrationclients_shared_secret", table_name="integrationclients"
    )
    op.drop_table("integrationclients")


def downgrade() -> None:
    op.create_table(
        "integrationclients",
        sa.Column("id", sa.INTEGER(), autoincrement=True, nullable=False),
        sa.Column("url", sa.VARCHAR(), autoincrement=False, nullable=True),
        sa.Column("shared_secret", sa.VARCHAR(), autoincrement=False, nullable=True),
        sa.Column("enabled", sa.BOOLEAN(), autoincrement=False, nullable=True),
        sa.Column(
            "created",
            postgresql.TIMESTAMP(timezone=True),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "last_accessed",
            postgresql.TIMESTAMP(timezone=True),
            autoincrement=False,
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("id", name="integrationclients_pkey"),
        sa.UniqueConstraint("url", name="integrationclients_url_key"),
    )
    op.create_index(
        "ix_integrationclients_shared_secret",
        "integrationclients",
        ["shared_secret"],
        unique=False,
    )
    op.add_column(
        "loans",
        sa.Column(
            "integration_client_id", sa.INTEGER(), autoincrement=False, nullable=True
        ),
    )
    op.create_foreign_key(
        "loans_integration_client_id_fkey",
        "loans",
        "integrationclients",
        ["integration_client_id"],
        ["id"],
    )
    op.create_index(
        "ix_loans_integration_client_id",
        "loans",
        ["integration_client_id"],
        unique=False,
    )
    op.add_column(
        "holds",
        sa.Column(
            "integration_client_id", sa.INTEGER(), autoincrement=False, nullable=True
        ),
    )
    op.create_foreign_key(
        "holds_integration_client_id_fkey",
        "holds",
        "integrationclients",
        ["integration_client_id"],
        ["id"],
    )
    op.create_index(
        "ix_holds_integration_client_id",
        "holds",
        ["integration_client_id"],
        unique=False,
    )
    op.add_column(
        "datasources",
        sa.Column(
            "integration_client_id", sa.INTEGER(), autoincrement=False, nullable=True
        ),
    )
    op.create_foreign_key(
        "datasources_integration_client_id_fkey",
        "datasources",
        "integrationclients",
        ["integration_client_id"],
        ["id"],
    )
    op.create_index(
        "ix_datasources_integration_client_id",
        "datasources",
        ["integration_client_id"],
        unique=False,
    )
