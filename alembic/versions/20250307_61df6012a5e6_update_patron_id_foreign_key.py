"""Update patron_id foreign key

Revision ID: 61df6012a5e6
Revises: 63825d889633
Create Date: 2025-03-07 00:38:25.610733+00:00

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "61df6012a5e6"
down_revision = "63825d889633"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("annotations_patron_id_fkey", "annotations", type_="foreignkey")
    op.create_foreign_key(
        None, "annotations", "patrons", ["patron_id"], ["id"], ondelete="CASCADE"
    )
    op.drop_constraint("credentials_patron_id_fkey", "credentials", type_="foreignkey")
    op.create_foreign_key(
        None, "credentials", "patrons", ["patron_id"], ["id"], ondelete="CASCADE"
    )
    op.drop_constraint("holds_patron_id_fkey", "holds", type_="foreignkey")
    op.create_foreign_key(
        None, "holds", "patrons", ["patron_id"], ["id"], ondelete="CASCADE"
    )
    op.drop_constraint("loans_patron_id_fkey", "loans", type_="foreignkey")
    op.create_foreign_key(
        None, "loans", "patrons", ["patron_id"], ["id"], ondelete="CASCADE"
    )


def downgrade() -> None:
    op.drop_constraint(None, "loans", type_="foreignkey")
    op.create_foreign_key(
        "loans_patron_id_fkey", "loans", "patrons", ["patron_id"], ["id"]
    )
    op.drop_constraint(None, "holds", type_="foreignkey")
    op.create_foreign_key(
        "holds_patron_id_fkey", "holds", "patrons", ["patron_id"], ["id"]
    )
    op.drop_constraint(None, "credentials", type_="foreignkey")
    op.create_foreign_key(
        "credentials_patron_id_fkey", "credentials", "patrons", ["patron_id"], ["id"]
    )
    op.drop_constraint(None, "annotations", type_="foreignkey")
    op.create_foreign_key(
        "annotations_patron_id_fkey", "annotations", "patrons", ["patron_id"], ["id"]
    )
