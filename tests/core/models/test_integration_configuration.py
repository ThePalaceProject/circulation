import pytest

from core.integration.goals import Goals
from core.model import create
from core.model.integration import IntegrationConfiguration
from tests.fixtures.database import DatabaseTransactionFixture


class TestSettingsModel:
    def test_get_items(self, db: DatabaseTransactionFixture):
        config, _ = create(
            db.session,
            IntegrationConfiguration,
            goal=Goals.LICENSE_GOAL,
            protocol="protocol",
            name="Config Name",
        )
        config.settings = dict(key="key", value="value")
        db.session.commit()

        # Basic retrieval
        assert config.get("key") == "key"
        assert config["value"] == "value"
        # Non existant keys
        assert config.get("no-such-key") == None
        assert config.get("no-such-key", "n/a") == "n/a"
        assert pytest.raises(KeyError, lambda: config["no-such-key"]).value is not None

    def test_set_items(self, db: DatabaseTransactionFixture):
        config, _ = create(
            db.session,
            IntegrationConfiguration,
            goal=Goals.LICENSE_GOAL,
            protocol="protocol",
            name="Config Name",
        )

        assert config.settings == {}

        config["key"] = "key"
        config.set("value", "value")
        db.session.commit()
        db.session.refresh(config)

        assert config.settings == dict(key="key", value="value")


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

        # No library config exists
        assert config.for_library(library.id) == None

        # This should create a new config
        libconfig = config.for_library(library.id, create=True)
        assert libconfig is not None
        assert libconfig.library == library
        assert libconfig.parent == config
        assert libconfig.settings == {}

        # The same config is returned henceforth
        assert config.for_library(library.id) == libconfig
        assert config.for_library(library.id, create=True) == libconfig


class TestIntegrationLibraryConfiguration:
    def test_get_item(self, db: DatabaseTransactionFixture):
        """Test the inheritance based value fetching"""
        config, _ = create(
            db.session,
            IntegrationConfiguration,
            goal=Goals.LICENSE_GOAL,
            protocol="protocol",
            name="Config Name",
        )
        library = db.default_library()
        assert library.id is not None

        config["key"] = "parent-key"
        config["value"] = "parent-value"

        libconfig = config.for_library(library.id, create=True)
        libconfig["key"] = "child-key"

        # Child owned key is the childs value
        assert libconfig["key"] == "child-key"
        # Parent owned key comes from the parent
        assert libconfig["value"] == "parent-value"
        # Parents key is not overwritten by the child
        assert config["key"] == "parent-key"

        # The lib config is only its own key
        assert libconfig.settings == dict(key="child-key")
