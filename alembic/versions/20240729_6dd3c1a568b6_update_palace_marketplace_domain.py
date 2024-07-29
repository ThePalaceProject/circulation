"""Update Palace Marketplace domain.

Revision ID: 6dd3c1a568b6
Revises: 7ba553f3f80d
Create Date: 2024-07-29 21:06:20.670391+00:00

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "6dd3c1a568b6"
down_revision = "7ba553f3f80d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    find_and_replace_within_external_account_id(
        "market.feedbooks.com", "market.thepalaceproject.org"
    )


def find_and_replace_within_external_account_id(find_value: str, replace_value: str):
    conn = op.get_bind()
    integration_configurations = conn.execute(
        f"SELECT id, settings->>'external_account_id' "
        f"FROM integration_configurations "
        f"WHERE settings->>'external_account_id' LIKE '%%{find_value}%%'"
    ).all()
    for integration_configuration in integration_configurations:
        external_account_id: str = integration_configuration.external_account_id
        new_external_account_id = external_account_id.replace(find_value, replace_value)
        conn.execute(
            f"""UPDATE integration_configurations
            SET settings = settings || '{"external_account_id": "{new_external_account_id}"}'
            WHERE id = {integration_configuration.id}"""
        )


def downgrade() -> None:
    find_and_replace_within_external_account_id(
        "market.thepalaceproject.org", "market.feedbooks.com"
    )
