"""Migrate library key pair

Revision ID: 04bbd03bf9f1
Revises: f08f9c6bded6
Create Date: 2023-07-06 14:40:17.970603+00:00

"""
import json
import logging

import sqlalchemy as sa
from Crypto.PublicKey import RSA

from alembic import op

# revision identifiers, used by Alembic.
revision = "04bbd03bf9f1"
down_revision = "f08f9c6bded6"
branch_labels = None
depends_on = None

log = logging.getLogger(f"palace.migration.{revision}")
log.setLevel(logging.INFO)
log.disabled = False


def upgrade() -> None:
    # Add the new columns as nullable, add the values, then make them non-nullable
    op.add_column(
        "libraries",
        sa.Column("public_key", sa.Unicode(), nullable=True),
    )
    op.add_column(
        "libraries",
        sa.Column("private_key", sa.LargeBinary(), nullable=True),
    )

    # Now we update the value stored for the key pair
    connection = op.get_bind()
    libraries = connection.execute("select id, short_name from libraries")
    for library in libraries:
        setting = connection.execute(
            "select cs.value from configurationsettings cs "
            "where cs.library_id = (%s) and cs.key = 'key-pair' and cs.external_integration_id IS NULL",
            (library.id,),
        ).fetchone()
        if setting and setting.value:
            _, private_key_str = json.loads(setting.value)
            private_key = RSA.import_key(private_key_str)
        else:
            log.info(f"Library {library.short_name} has no key pair, generating one...")
            private_key = RSA.generate(2048)

        private_key_bytes = private_key.export_key("DER")
        public_key_str = private_key.publickey().export_key("PEM").decode("utf-8")

        connection.execute(
            "update libraries set public_key = (%s), private_key = (%s) where id = (%s)",
            (public_key_str, private_key_bytes, library.id),
        )

    # Then we make the columns non-nullable
    op.alter_column("libraries", "public_key", nullable=False)
    op.alter_column("libraries", "private_key", nullable=False)


def downgrade() -> None:
    op.drop_column("libraries", "private_key")
    op.drop_column("libraries", "public_key")
