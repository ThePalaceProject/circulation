"""Drop coveragerecords and equivalentscoveragerecords tables

The CoverageProvider machinery that read and wrote ``coveragerecords`` was
retired in the previous release, and the equivalent-identifiers refresh that
wrote ``equivalentscoveragerecords`` moved to a Celery task before that. No
running code (current or N-1) references either table, so both are dropped here
along with their shared ``coverage_status`` enum type.

The ``timestamps`` table and its separate ``service_type`` enum are unaffected.

Revision ID: 5ca948100902
Revises: a6c85605404c
Create Date: 2026-06-24 22:47:03.130552+00:00

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "5ca948100902"
down_revision = "a6c85605404c"
branch_labels = None
depends_on = None

# The status enum shared by the two dropped tables. We manage the Postgres type
# explicitly (create_type=False) so it is created/dropped exactly once rather
# than once per table.
coverage_status = postgresql.ENUM(
    "success",
    "transient failure",
    "persistent failure",
    "registered",
    name="coverage_status",
    create_type=False,
)


def upgrade() -> None:
    op.drop_index(
        op.f("ix_equivalentscoveragerecords_equivalency_id"),
        table_name="equivalentscoveragerecords",
    )
    op.drop_index(
        op.f("ix_equivalentscoveragerecords_operation"),
        table_name="equivalentscoveragerecords",
    )
    op.drop_index(
        op.f("ix_equivalentscoveragerecords_status"),
        table_name="equivalentscoveragerecords",
    )
    op.drop_index(
        op.f("ix_equivalentscoveragerecords_timestamp"),
        table_name="equivalentscoveragerecords",
    )
    op.drop_table("equivalentscoveragerecords")

    op.drop_index(
        op.f("ix_coveragerecords_data_source_id_operation_identifier_id"),
        table_name="coveragerecords",
    )
    op.drop_index(op.f("ix_coveragerecords_exception"), table_name="coveragerecords")
    op.drop_index(
        op.f("ix_coveragerecords_identifier_id"), table_name="coveragerecords"
    )
    op.drop_index(op.f("ix_coveragerecords_status"), table_name="coveragerecords")
    op.drop_index(op.f("ix_coveragerecords_timestamp"), table_name="coveragerecords")
    op.drop_index(
        "ix_identifier_id_data_source_id_operation", table_name="coveragerecords"
    )
    op.drop_index(
        "ix_identifier_id_data_source_id_operation_collection_id",
        table_name="coveragerecords",
    )
    op.drop_table("coveragerecords")

    # The coverage_status enum was used only by the two tables just dropped.
    coverage_status.drop(op.get_bind(), checkfirst=False)


def downgrade() -> None:
    coverage_status.create(op.get_bind(), checkfirst=False)

    op.create_table(
        "coveragerecords",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("identifier_id", sa.Integer(), nullable=True),
        sa.Column("data_source_id", sa.Integer(), nullable=True),
        sa.Column("operation", sa.String(length=255), nullable=True),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", coverage_status, nullable=True),
        sa.Column("exception", sa.Unicode(), nullable=True),
        sa.Column("collection_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["collection_id"],
            ["collections.id"],
            name=op.f("coveragerecords_collection_id_fkey"),
        ),
        sa.ForeignKeyConstraint(
            ["data_source_id"],
            ["datasources.id"],
            name=op.f("coveragerecords_data_source_id_fkey"),
        ),
        sa.ForeignKeyConstraint(
            ["identifier_id"],
            ["identifiers.id"],
            name=op.f("coveragerecords_identifier_id_fkey"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("coveragerecords_pkey")),
    )
    op.create_index(
        "ix_identifier_id_data_source_id_operation_collection_id",
        "coveragerecords",
        ["identifier_id", "data_source_id", "operation", "collection_id"],
        unique=True,
    )
    op.create_index(
        "ix_identifier_id_data_source_id_operation",
        "coveragerecords",
        ["identifier_id", "data_source_id", "operation"],
        unique=True,
        postgresql_where=sa.text("collection_id IS NULL"),
    )
    op.create_index(
        op.f("ix_coveragerecords_timestamp"),
        "coveragerecords",
        ["timestamp"],
        unique=False,
    )
    op.create_index(
        op.f("ix_coveragerecords_status"), "coveragerecords", ["status"], unique=False
    )
    op.create_index(
        op.f("ix_coveragerecords_identifier_id"),
        "coveragerecords",
        ["identifier_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_coveragerecords_exception"),
        "coveragerecords",
        ["exception"],
        unique=False,
    )
    op.create_index(
        op.f("ix_coveragerecords_data_source_id_operation_identifier_id"),
        "coveragerecords",
        ["data_source_id", "operation", "identifier_id"],
        unique=False,
    )

    op.create_table(
        "equivalentscoveragerecords",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("equivalency_id", sa.Integer(), nullable=False),
        sa.Column("operation", sa.String(length=255), nullable=True),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", coverage_status, nullable=True),
        sa.Column("exception", sa.Unicode(), nullable=True),
        sa.ForeignKeyConstraint(
            ["equivalency_id"],
            ["equivalents.id"],
            name=op.f("equivalentscoveragerecords_equivalency_id_fkey"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("equivalentscoveragerecords_pkey")),
        sa.UniqueConstraint(
            "equivalency_id",
            "operation",
            name=op.f("equivalentscoveragerecords_equivalency_id_operation_key"),
        ),
    )
    op.create_index(
        op.f("ix_equivalentscoveragerecords_timestamp"),
        "equivalentscoveragerecords",
        ["timestamp"],
        unique=False,
    )
    op.create_index(
        op.f("ix_equivalentscoveragerecords_status"),
        "equivalentscoveragerecords",
        ["status"],
        unique=False,
    )
    op.create_index(
        op.f("ix_equivalentscoveragerecords_operation"),
        "equivalentscoveragerecords",
        ["operation"],
        unique=False,
    )
    op.create_index(
        op.f("ix_equivalentscoveragerecords_equivalency_id"),
        "equivalentscoveragerecords",
        ["equivalency_id"],
        unique=False,
    )
