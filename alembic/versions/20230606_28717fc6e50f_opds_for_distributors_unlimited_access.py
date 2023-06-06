"""opds for distributors unlimited access

Revision ID: 28717fc6e50f
Revises: a9ed3f76d649
Create Date: 2023-06-06 10:08:35.892018+00:00

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "28717fc6e50f"
down_revision = "a9ed3f76d649"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection = op.get_bind()
    connection.execute(
        """
  UPDATE
    licensepools AS lp
  SET
    licenses_owned     = -1,
    licenses_available = -1
  FROM
    collections c,
    externalintegrations e
  WHERE
    lp.licenses_owned             = 1
    and lp.licenses_available     = 1
    and lp.collection_id          = c.id
    and c.external_integration_id = e.id
    and e.protocol                = 'OPDS for Distributors'
    """
    )


def downgrade() -> None:
    connection = op.get_bind()
    connection.execute(
        """
  UPDATE
    licensepools AS lp
  SET
    licenses_owned     = 1,
    licenses_available = 1
  FROM
    collections c,
    externalintegrations e
  WHERE
    lp.licenses_owned             = -1
    and lp.licenses_available     = -1
    and lp.collection_id          = c.id
    and c.external_integration_id = e.id
    and e.protocol                = 'OPDS for Distributors'
    """
    )
