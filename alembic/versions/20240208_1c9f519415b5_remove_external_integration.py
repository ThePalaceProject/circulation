"""Remove external integration

Revision ID: 1c9f519415b5
Revises: fc3c9ccf0ad8
Create Date: 2024-02-08 23:50:50.399968+00:00

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "1c9f519415b5"
down_revision = "fc3c9ccf0ad8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index(
        "ix_configurationsettings_external_integration_id",
        table_name="configurationsettings",
    )
    op.drop_index(
        "ix_configurationsettings_external_integration_id_key",
        table_name="configurationsettings",
        postgresql_where="(library_id IS NULL)",
    )
    op.drop_index(
        "ix_configurationsettings_external_integration_id_library_id_key",
        table_name="configurationsettings",
    )
    op.drop_index(
        "ix_configurationsettings_key",
        table_name="configurationsettings",
        postgresql_where="((external_integration_id IS NULL) AND (library_id IS NULL))",
    )
    op.drop_index(
        "ix_configurationsettings_library_id", table_name="configurationsettings"
    )
    op.drop_index(
        "ix_configurationsettings_library_id_key",
        table_name="configurationsettings",
        postgresql_where="(external_integration_id IS NULL)",
    )
    op.drop_table("configurationsettings")
    op.drop_index(
        "ix_externalintegrations_libraries_externalintegration_id",
        table_name="externalintegrations_libraries",
    )
    op.drop_index(
        "ix_externalintegrations_libraries_library_id",
        table_name="externalintegrations_libraries",
    )
    op.drop_table("externalintegrations_libraries")
    op.drop_table("externalintegrations")


def downgrade() -> None:
    op.create_table(
        "externalintegrations",
        sa.Column(
            "id",
            sa.INTEGER(),
            server_default=sa.text("nextval('externalintegrations_id_seq'::regclass)"),
            autoincrement=True,
            nullable=False,
        ),
        sa.Column("protocol", sa.VARCHAR(), autoincrement=False, nullable=False),
        sa.Column("goal", sa.VARCHAR(), autoincrement=False, nullable=True),
        sa.Column("name", sa.VARCHAR(), autoincrement=False, nullable=True),
        sa.PrimaryKeyConstraint("id", name="externalintegrations_pkey"),
        sa.UniqueConstraint("name", name="externalintegrations_name_key"),
        postgresql_ignore_search_path=False,
    )
    op.create_table(
        "externalintegrations_libraries",
        sa.Column(
            "externalintegration_id", sa.INTEGER(), autoincrement=False, nullable=False
        ),
        sa.Column("library_id", sa.INTEGER(), autoincrement=False, nullable=False),
        sa.ForeignKeyConstraint(
            ["externalintegration_id"],
            ["externalintegrations.id"],
            name="externalintegrations_libraries_externalintegration_id_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["library_id"],
            ["libraries.id"],
            name="externalintegrations_libraries_library_id_fkey",
        ),
        sa.UniqueConstraint(
            "externalintegration_id",
            "library_id",
            name="externalintegrations_librarie_externalintegration_id_librar_key",
        ),
    )
    op.create_index(
        "ix_externalintegrations_libraries_library_id",
        "externalintegrations_libraries",
        ["library_id"],
        unique=False,
    )
    op.create_index(
        "ix_externalintegrations_libraries_externalintegration_id",
        "externalintegrations_libraries",
        ["externalintegration_id"],
        unique=False,
    )
    op.create_table(
        "configurationsettings",
        sa.Column("id", sa.INTEGER(), autoincrement=True, nullable=False),
        sa.Column(
            "external_integration_id", sa.INTEGER(), autoincrement=False, nullable=True
        ),
        sa.Column("library_id", sa.INTEGER(), autoincrement=False, nullable=True),
        sa.Column("key", sa.VARCHAR(), autoincrement=False, nullable=True),
        sa.Column("value", sa.VARCHAR(), autoincrement=False, nullable=True),
        sa.ForeignKeyConstraint(
            ["external_integration_id"],
            ["externalintegrations.id"],
            name="configurationsettings_external_integration_id_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["library_id"],
            ["libraries.id"],
            name="configurationsettings_library_id_fkey",
        ),
        sa.PrimaryKeyConstraint("id", name="configurationsettings_pkey"),
    )
    op.create_index(
        "ix_configurationsettings_library_id_key",
        "configurationsettings",
        ["library_id", "key"],
        unique=True,
        postgresql_where="(external_integration_id IS NULL)",
    )
    op.create_index(
        "ix_configurationsettings_library_id",
        "configurationsettings",
        ["library_id"],
        unique=False,
    )
    op.create_index(
        "ix_configurationsettings_key",
        "configurationsettings",
        ["key"],
        unique=True,
        postgresql_where="((external_integration_id IS NULL) AND (library_id IS NULL))",
    )
    op.create_index(
        "ix_configurationsettings_external_integration_id_library_id_key",
        "configurationsettings",
        ["external_integration_id", "library_id", "key"],
        unique=True,
    )
    op.create_index(
        "ix_configurationsettings_external_integration_id_key",
        "configurationsettings",
        ["external_integration_id", "key"],
        unique=True,
        postgresql_where="(library_id IS NULL)",
    )
    op.create_index(
        "ix_configurationsettings_external_integration_id",
        "configurationsettings",
        ["external_integration_id"],
        unique=False,
    )
