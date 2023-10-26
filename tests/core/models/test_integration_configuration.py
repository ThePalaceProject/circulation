from core.integration.goals import Goals
from core.model import create
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

        # No library config exists
        assert config.for_library(library.id) is None

        # This should create a new config
        libconfig = config.for_library(library.id, create=True)
        assert libconfig is not None
        assert libconfig.library == library
        assert libconfig.parent == config
        assert libconfig.settings_dict == {}

        # The same config is returned henceforth
        assert config.for_library(library.id) == libconfig
        assert config.for_library(library.id, create=True) == libconfig
