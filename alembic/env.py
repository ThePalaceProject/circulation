from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool

from alembic import context
from core.config import Configuration

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
from core.model import Base

target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


# Import the models not included in models/__init__.py
# This is required for autogenerate to work correctly
from api.saml.metadata.federations.model import (  # noqa: autoflake
    SAMLFederatedIdentityProvider,
    SAMLFederation,
)
from core.lane import Lane, LaneGenre  # noqa: autoflake

# Skip these tables from any kind of migration
SKIP_TABLES = ["complaints", "libraryalias"]


def include_name(name, type_, parent_names):
    if type_ == "table":
        return name not in SKIP_TABLES
    return True


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = Configuration.database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        include_name=include_name,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        **{"url": Configuration.database_url()}
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_name=include_name,
        )

        with context.begin_transaction():
            # Acquire an application lock to ensure multiple migrations are queued and not concurrent
            # See: https://github.com/sqlalchemy/alembic/issues/633
            connection.execute("SELECT pg_advisory_xact_lock(10000);")
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
