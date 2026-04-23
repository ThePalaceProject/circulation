"""Add change tracking hash columns

Revision ID: f98e4049c87d
Revises: 23795e50c915
Create Date: 2026-04-03 03:05:58.742988+00:00

NOTE: First-import performance after this migration
---------------------------------------------------
All new columns are added as nullable with no default values, so every existing
Edition and LicensePool starts with ``updated_at_data_hash = NULL``. The change
detection logic in ``BaseMutableData.should_apply_to`` treats a NULL hash as
"never imported", which means **every record will be re-applied on the first
import run after this migration is deployed**. This is intentional – it
establishes the baseline hashes – but operators should expect that initial
import job to take longer than usual.

Subsequent imports will benefit from hash-based skipping and will be
significantly faster.
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f98e4049c87d"
down_revision = "23795e50c915"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add hash column to editions: stores the SHA-256 hash of the last
    # BibliographicData applied to this edition.
    op.add_column(
        "editions",
        sa.Column("updated_at_data_hash", sa.String(), nullable=True),
    )

    # Add hash column to licensepools: stores the SHA-256 hash of the last
    # CirculationData applied to this pool.
    op.add_column(
        "licensepools",
        sa.Column("updated_at_data_hash", sa.String(), nullable=True),
    )

    # Add created_at and updated_at to licensepools: track when CirculationData
    # was first and most recently imported for this pool. These complement the
    # existing availability_time (search/feed ordering) and last_checked
    # (event processing) timestamps.
    op.add_column(
        "licensepools",
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "licensepools",
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("licensepools", "updated_at")
    op.drop_column("licensepools", "created_at")
    op.drop_column("licensepools", "updated_at_data_hash")
    op.drop_column("editions", "updated_at_data_hash")
