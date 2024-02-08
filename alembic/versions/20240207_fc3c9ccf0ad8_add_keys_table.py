"""Add keys table

Revision ID: fc3c9ccf0ad8
Revises: 993729d4bf97
Create Date: 2024-02-07 17:51:44.823725+00:00

"""
import datetime
import uuid
from collections.abc import Callable

import sqlalchemy as sa
from jwcrypto import jwk
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Connection

from alembic import op
from core.migration.util import migration_logger
from core.util.datetime_helpers import utc_now
from core.util.string_helpers import random_key

# revision identifiers, used by Alembic.
revision = "fc3c9ccf0ad8"
down_revision = "993729d4bf97"
branch_labels = None
depends_on = None

log = migration_logger(revision)


def get_sitewide_config(connection: Connection, key: str) -> str | None:
    result = connection.execute(
        "SELECT value from configurationsettings where key = %s and library_id is null and external_integration_id is null",
        key,
    ).one_or_none()

    if result is None:
        return None

    return result.value


def insert_key(
    connection: Connection, key_type: str, value: str, created: datetime.datetime
) -> None:
    connection.execute(
        "INSERT INTO keys (id, created, value, type) VALUES (%s, %s, %s, %s)",
        (uuid.uuid4(), created, value, key_type),
    )


def migrate_configuration_setting(
    connection: Connection,
    key_type: str,
    setting_value: str | None,
    generate: Callable[[], str],
) -> None:
    unknown_creation_time = datetime.datetime(
        year=1970, month=1, day=1, tzinfo=datetime.timezone.utc
    )

    if setting_value:
        log.info(f"Migrating {key_type} to new keys table")
        insert_key(connection, key_type, setting_value, unknown_creation_time)
    else:
        log.warning(f"No {key_type} found. Generating a new one.")
        insert_key(connection, key_type, generate(), utc_now())


def upgrade() -> None:
    op.create_table(
        "keys",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("value", sa.Unicode(), nullable=False),
        sa.Column(
            "type",
            sa.Enum(
                "AUTH_TOKEN_JWE",
                "BEARER_TOKEN_SIGNING",
                "ADMIN_SECRET_KEY",
                name="keytype",
            ),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_keys_created"), "keys", ["created"], unique=False)
    op.create_index(op.f("ix_keys_type"), "keys", ["type"], unique=False)

    # Migrate in the data from the old table.
    connection = op.get_bind()

    admin_secret_key = get_sitewide_config(connection, "secret_key")
    bearer_token_signing_key = get_sitewide_config(
        connection, "bearer_token_signing_secret"
    )
    auth_token_jwe_key = get_sitewide_config(connection, "PATRON_JWE_KEY")

    migrate_configuration_setting(
        connection, "ADMIN_SECRET_KEY", admin_secret_key, lambda: random_key(48)
    )
    migrate_configuration_setting(
        connection,
        "BEARER_TOKEN_SIGNING",
        bearer_token_signing_key,
        lambda: random_key(48),
    )
    migrate_configuration_setting(
        connection,
        "AUTH_TOKEN_JWE",
        auth_token_jwe_key,
        lambda: jwk.JWK.generate(kty="oct", size=256).export(),
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_keys_type"), table_name="keys")
    op.drop_index(op.f("ix_keys_created"), table_name="keys")
    op.drop_table("keys")
    sa.Enum(name="keytype").drop(op.get_bind(), checkfirst=False)
