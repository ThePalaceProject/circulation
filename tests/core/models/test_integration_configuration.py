from unittest.mock import MagicMock

from core.integration.goals import Goals
from core.model import Library, create
from core.model.integration import IntegrationConfiguration
from tests.fixtures.database import DatabaseTransactionFixture


class TestIntegrationConfigurations:
    def test_for_library(seslf, db: DatabaseTransactionFixture):
        config, _ = create(
            db.session,
            IntegrationConfiguration,
            goal=Goals.LICENSE_GOAL,
            protocol="protocol",
            name="Config Name",
        )
        library = db.default_library()
        assert library.id is not None

        # No library ID provided
        assert config.for_library(None) is None

        # Library has no ID
        mock_library = MagicMock(spec=Library)
        mock_library.id = None
        assert config.for_library(mock_library) is None

        # No library config exists
        assert config.for_library(library.id) is None

        config.libraries.append(library)

        # Library config exists
        libconfig = config.for_library(library.id)

        # The same config is returned for the same library
        assert config.for_library(library) is libconfig
