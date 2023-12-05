"""MARC Export cleanup migration.

Revision ID: d3cdbea3d43b
Revises: 0039f3f12014
Create Date: 2023-12-04 17:23:26.396526+00:00

"""
from typing import Optional
from urllib.parse import urlparse

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op
from core.migration.util import migration_logger
from core.service.container import container_instance

# revision identifiers, used by Alembic.
revision = "d3cdbea3d43b"
down_revision = "0039f3f12014"
branch_labels = None
depends_on = None


def parse_key_from_url(url: str, bucket: str) -> Optional[str]:
    """Parse the key from a URL.

    :param url: The URL to parse.
    :return: The key, or None if the URL is not a valid S3 URL.
    """
    parsed_url = urlparse(url)

    if f"/{bucket}/" in parsed_url.path:
        return parsed_url.path.split(f"/{bucket}/", 1)[1]

    if bucket in parsed_url.netloc:
        return parsed_url.path.lstrip("/")

    return None


def upgrade() -> None:
    # Before removing the cachedmarcfiles table, we need to delete the cachedmarcfiles
    # from s3.
    services = container_instance()
    public_s3 = services.storage.public()
    log = migration_logger(revision)

    # Check if there are any cachedmarcfiles in s3
    connection = op.get_bind()
    cached_files = connection.execute(
        "SELECT r.mirror_url FROM cachedmarcfiles cmf JOIN representations r ON cmf.representation_id = r.id"
    ).all()
    if public_s3 is None and len(cached_files) > 0:
        raise RuntimeError(
            "There are cachedmarcfiles in the database, but no public s3 storage configured!"
        )

    keys_to_delete = []
    for cached_file in cached_files:
        url = cached_file.mirror_url
        bucket = public_s3.bucket
        key = parse_key_from_url(url, bucket)
        if key is None:
            raise RuntimeError(f"Unexpected URL format: {url} (bucket: {bucket})")
        generated_url = public_s3.generate_url(key)
        if generated_url != url:
            raise RuntimeError(f"URL mismatch: {url} != {generated_url}")
        keys_to_delete.append(key)

    for key in keys_to_delete:
        log.info(f"Deleting {key} from s3 bucket {public_s3.bucket}")
        public_s3.delete(key)

    # remove the coverage records for the cachedmarcfiles
    op.execute("DELETE FROM coveragerecords WHERE operation = 'generate-marc'")

    # Remove the foreign key constraint on the cachedmarcfiles table
    op.drop_constraint(
        "cachedmarcfiles_representation_id_fkey",
        "cachedmarcfiles",
        type_="foreignkey",
    )

    # Remove the representations for the cachedmarcfiles
    op.execute(
        "DELETE FROM representations WHERE id IN (SELECT representation_id FROM cachedmarcfiles)"
    )

    # Remove the cachedmarcfiles
    op.drop_index("ix_cachedmarcfiles_end_time", table_name="cachedmarcfiles")
    op.drop_index("ix_cachedmarcfiles_lane_id", table_name="cachedmarcfiles")
    op.drop_index("ix_cachedmarcfiles_library_id", table_name="cachedmarcfiles")
    op.drop_index("ix_cachedmarcfiles_start_time", table_name="cachedmarcfiles")
    op.drop_table("cachedmarcfiles")

    # Remove the unused marc_record column from the works table
    op.drop_column("works", "marc_record")


def downgrade() -> None:
    op.add_column(
        "works",
        sa.Column("marc_record", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
    op.create_table(
        "cachedmarcfiles",
        sa.Column("id", sa.INTEGER(), autoincrement=True, nullable=False),
        sa.Column("library_id", sa.INTEGER(), autoincrement=False, nullable=False),
        sa.Column("lane_id", sa.INTEGER(), autoincrement=False, nullable=True),
        sa.Column(
            "representation_id", sa.INTEGER(), autoincrement=False, nullable=False
        ),
        sa.Column(
            "start_time",
            postgresql.TIMESTAMP(timezone=True),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "end_time",
            postgresql.TIMESTAMP(timezone=True),
            autoincrement=False,
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["lane_id"], ["lanes.id"], name="cachedmarcfiles_lane_id_fkey"
        ),
        sa.ForeignKeyConstraint(
            ["library_id"], ["libraries.id"], name="cachedmarcfiles_library_id_fkey"
        ),
        sa.ForeignKeyConstraint(
            ["representation_id"],
            ["representations.id"],
            name="cachedmarcfiles_representation_id_fkey",
        ),
        sa.PrimaryKeyConstraint("id", name="cachedmarcfiles_pkey"),
    )
    op.create_index(
        "ix_cachedmarcfiles_start_time", "cachedmarcfiles", ["start_time"], unique=False
    )
    op.create_index(
        "ix_cachedmarcfiles_library_id", "cachedmarcfiles", ["library_id"], unique=False
    )
    op.create_index(
        "ix_cachedmarcfiles_lane_id", "cachedmarcfiles", ["lane_id"], unique=False
    )
    op.create_index(
        "ix_cachedmarcfiles_end_time", "cachedmarcfiles", ["end_time"], unique=False
    )
