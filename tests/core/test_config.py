import os
from typing import Iterable

import pytest
from sqlalchemy.orm.session import Session

from core.config import Configuration as BaseConfiguration
from core.model import ExternalIntegration
from tests.fixtures.database import DatabaseTransactionFixture


# Create a configuration object that the tests can run against without
# impacting the real configuration object.
class MockConfiguration(BaseConfiguration):
    instance: dict = dict()


class ConfigurationTestFixture:
    def __init__(self, database_transaction: DatabaseTransactionFixture):
        self.Conf = MockConfiguration
        self.root_dir = os.path.join(os.path.split(__file__)[0], "..", "..")
        self.VERSION_FILENAME = os.path.join(self.root_dir, self.Conf.VERSION_FILENAME)
        self.transaction = database_transaction

    def close(self):
        if os.path.exists(self.VERSION_FILENAME):
            os.remove(self.VERSION_FILENAME)


@pytest.fixture()
def configuration_test_fixture(
    db: DatabaseTransactionFixture,
) -> Iterable[ConfigurationTestFixture]:
    fix = ConfigurationTestFixture(db)
    yield fix
    fix.close()


class TestConfiguration:
    def test_app_version(self, configuration_test_fixture: ConfigurationTestFixture):
        data = configuration_test_fixture
        data.Conf.instance = dict()

        def create_version_file(content):
            with open(data.VERSION_FILENAME, "w") as f:
                f.write(content)

        # Without a .version file, the key is set to a null object.
        result = data.Conf.app_version()
        assert data.Conf.APP_VERSION in data.Conf.instance
        assert data.Conf.NO_APP_VERSION_FOUND == result
        assert data.Conf.NO_APP_VERSION_FOUND == data.Conf.get(data.Conf.APP_VERSION)

        # An empty .version file yields the same results.
        data.Conf.instance = dict()
        create_version_file(" \n")
        result = data.Conf.app_version()
        assert data.Conf.NO_APP_VERSION_FOUND == result
        assert data.Conf.NO_APP_VERSION_FOUND == data.Conf.get(data.Conf.APP_VERSION)

        # A .version file with content loads the content.
        data.Conf.instance = dict()
        create_version_file("ba.na.na")
        result = data.Conf.app_version()
        assert "ba.na.na" == result
        assert "ba.na.na" == data.Conf.get(data.Conf.APP_VERSION)

    def test_load_cdns(self, configuration_test_fixture: ConfigurationTestFixture):
        """Test our ability to load CDN configuration from the database."""
        data = configuration_test_fixture

        data.transaction.external_integration(
            protocol=ExternalIntegration.CDN,
            goal=ExternalIntegration.CDN_GOAL,
            settings={
                data.Conf.CDN_MIRRORED_DOMAIN_KEY: "site.com",
                ExternalIntegration.URL: "http://cdn/",
            },
        )

        data.Conf.load_cdns(data.transaction.session)

        integrations = data.Conf.instance[data.Conf.INTEGRATIONS]
        assert {"site.com": "http://cdn/"} == integrations[ExternalIntegration.CDN]
        assert True == data.Conf.instance[data.Conf.CDNS_LOADED_FROM_DATABASE]

    @pytest.mark.skip(
        reason="This test _may_ be incompatible with the use of a database fixture."
    )
    def test_cdns_loaded_dynamically(
        self, configuration_test_fixture: ConfigurationTestFixture
    ):
        # When you call cdns() on a Configuration object that was
        # never initialized, it creates a new database connection and
        # loads CDN configuration from the database. This lets
        # us avoid having to have a database connection handy to pass into
        # cdns().
        #
        # We can't do an end-to-end test, because any changes we
        # commit won't show up in the new connection (this test is
        # running inside a transaction that will be rolled back).
        #
        # But we can verify that load_cdns is called with a new
        # database connection.

        data = configuration_test_fixture

        class Mock(MockConfiguration):
            @classmethod
            def load_cdns(cls, _db, config_instance=None):
                cls.called_with = (_db, config_instance)

        cdns = Mock.cdns()
        assert {} == cdns

        new_db, none = Mock.called_with
        assert new_db != data.transaction.session
        assert isinstance(new_db, Session)
        assert None == none
