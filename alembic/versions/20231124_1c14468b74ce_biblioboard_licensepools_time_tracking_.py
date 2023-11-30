"""Biblioboard licensepools time tracking flag

Revision ID: 1c14468b74ce
Revises: 6af9160a578e
Create Date: 2023-11-24 08:11:35.541207+00:00

"""
from alembic import op
from core.migration.util import migration_logger

# revision identifiers, used by Alembic.
revision = "1c14468b74ce"
down_revision = "6af9160a578e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection = op.get_bind()
    log = migration_logger(revision)

    collections = connection.execute(
        "select collections.id from integration_configurations \
        JOIN collections on collections.integration_configuration_id = integration_configurations.id \
        where integration_configurations.protocol = 'OPDS for Distributors'"
    ).all()

    log.warning(f"Will update licensepools for collections: {collections}")

    collection_ids = [cid.id for cid in collections]

    if len(collection_ids) == 0:
        log.info("No collections found to update!")
        return

    pool_ids = connection.execute(
        "select licensepools.id from licensepools \
        JOIN collections on collections.id = licensepools.collection_id \
        JOIN editions on editions.primary_identifier_id = licensepools.identifier_id \
        WHERE editions.medium = 'Audio' and licensepools.collection_id in %(collection_ids)s",
        collection_ids=tuple(collection_ids),
    ).all()

    pool_ids_list = [p.id for p in pool_ids]
    # update licensepools
    if len(pool_ids_list) == 0:
        log.info("No licensepools to update!")
        return

    connection.execute(
        "UPDATE licensepools SET should_track_playtime=true WHERE id in %(ids)s returning id",
        ids=tuple(pool_ids_list),
    ).all()


def downgrade() -> None:
    pass
