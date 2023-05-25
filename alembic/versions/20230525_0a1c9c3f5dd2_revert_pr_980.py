"""revert pr 980

Revision ID: 0a1c9c3f5dd2
Revises: a9ed3f76d649
Create Date: 2023-05-25 19:07:04.474551+00:00

"""
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "0a1c9c3f5dd2"
down_revision = "a9ed3f76d649"
branch_labels = None
depends_on = None


status_enum = sa.Enum("green", "red", name="external_integration_status")


def upgrade() -> None:
    op.drop_table("externalintegrationerrors")
    op.drop_column("externalintegrations", "last_status_update")
    op.drop_column("externalintegrations", "status")
    status_enum.drop(op.get_bind())


def downgrade() -> None:
    status_enum.create(op.get_bind())
    op.add_column(
        "externalintegrations",
        sa.Column(
            "status",
            postgresql.ENUM("green", "red", name="external_integration_status"),
            server_default=sa.text("'green'::external_integration_status"),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "externalintegrations",
        sa.Column(
            "last_status_update",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.create_table(
        "externalintegrationerrors",
        sa.Column("id", sa.INTEGER(), autoincrement=True, nullable=False),
        sa.Column("time", postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
        sa.Column("error", sa.VARCHAR(), autoincrement=False, nullable=True),
        sa.Column(
            "external_integration_id", sa.INTEGER(), autoincrement=False, nullable=True
        ),
        sa.ForeignKeyConstraint(
            ["external_integration_id"],
            ["externalintegrations.id"],
            name="fk_error_externalintegrations_id",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="externalintegrationerrors_pkey"),
    )
