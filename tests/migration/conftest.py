from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Generator, Union

import pytest
import pytest_alembic
from pytest_alembic.config import Config

if TYPE_CHECKING:
    from pytest_alembic import MigrationContext
    from sqlalchemy.engine import Engine

    import alembic.config
    from tests.fixtures.database import ApplicationFixture, DatabaseFixture

pytest_plugins = [
    "tests.fixtures.api_config",
    "tests.fixtures.database",
]


@pytest.fixture
def alembic_engine(
    application: ApplicationFixture, database: DatabaseFixture
) -> Engine:
    """
    Override this fixture to provide pytest-alembic powered tests with a database handle.
    """
    return database._engine


@pytest.fixture
def alembic_runner(
    alembic_config: Union[Dict[str, Any], alembic.config.Config, Config],
    alembic_engine: Engine,
) -> Generator[MigrationContext, None, None]:
    """
    Override this fixture to make sure that we stamp head. Since this is how out database
    is initialized. The normal fixtures assume you start from an empty database.
    """
    config = Config.from_raw_config(alembic_config)
    with pytest_alembic.runner(config=config, engine=alembic_engine) as runner:
        runner.command_executor.stamp("head")
        yield runner
