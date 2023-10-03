"""Order last update facet per library

Revision ID: 5907102d8f99
Revises: 1c566151741f
Create Date: 2023-10-03 08:37:07.560101+00:00

"""
import json

from alembic import op

# revision identifiers, used by Alembic.
revision = "5907102d8f99"
down_revision = "1c566151741f"
branch_labels = None
depends_on = None

ORDER_KEY = "facets_enabled_order"


def upgrade() -> None:
    conn = op.get_bind()
    all_libraries = conn.execute("SELECT id, settings_dict from libraries").all()

    for (lib_id, settings) in all_libraries:
        order = settings.get(ORDER_KEY, [])
        if "last_update" not in order:
            order.append("last_update")
            settings[ORDER_KEY] = order
            conn.execute(
                "UPDATE libraries SET settings_dict=%s where id=%s",
                json.dumps(settings),
                lib_id,
            )


def downgrade() -> None:
    """Remove the last_update order facet from all libraries"""
    conn = op.get_bind()
    all_libraries = conn.execute("SELECT id, settings_dict from libraries").all()

    for (lib_id, settings) in all_libraries:
        order: list = settings.get(ORDER_KEY, [])
        if "last_update" in order:
            order.remove("last_update")
            settings[ORDER_KEY] = order
            conn.execute(
                "UPDATE libraries SET settings_dict=%s where id=%s",
                json.dumps(settings),
                lib_id,
            )
