import json
from enum import Enum
from unittest.mock import MagicMock, create_autospec

import pytest
import sqlalchemy
from flask_babel import lazy_gettext as _
from sqlalchemy.exc import IntegrityError

from core.config import CannotLoadConfiguration, Configuration
from core.model import create, get_one
from core.model.collection import Collection
from core.model.configuration import (
    ConfigurationAttribute,
    ConfigurationAttributeType,
    ConfigurationGrouping,
    ConfigurationMetadata,
    ConfigurationOption,
    ConfigurationSetting,
    ConfigurationStorage,
    ExternalIntegration,
    ExternalIntegrationLink,
    HasExternalIntegration,
)
from core.model.datasource import DataSource
from tests.fixtures.database import DatabaseTransactionFixture


class TestConfigurationSetting:
    def test_is_secret(self, db: DatabaseTransactionFixture):
        """Some configuration settings are considered secrets,
        and some are not.
        """
        m = ConfigurationSetting._is_secret
        assert True == m("secret")
        assert True == m("password")
        assert True == m("its_a_secret_to_everybody")
        assert True == m("the_password")
        assert True == m("password_for_the_account")
        assert False == m("public_information")

        assert True == ConfigurationSetting.sitewide(db.session, "secret_key").is_secret
        assert (
            False == ConfigurationSetting.sitewide(db.session, "public_key").is_secret
        )

    def test_value_or_default(self, db: DatabaseTransactionFixture):
        integration, ignore = create(
            db.session,
            ExternalIntegration,
            goal=db.fresh_str(),
            protocol=db.fresh_str(),
        )
        setting = integration.setting("key")
        assert None == setting.value

        # If the setting has no value, value_or_default sets the value to
        # the default, and returns the default.
        assert "default value" == setting.value_or_default("default value")
        assert "default value" == setting.value

        # Once the value is set, value_or_default returns the value.
        assert "default value" == setting.value_or_default("new default")

        # If the setting has any value at all, even the empty string,
        # it's returned instead of the default.
        setting.value = ""
        assert "" == setting.value_or_default("default")

    def test_value_inheritance(self, db: DatabaseTransactionFixture):
        key = "SomeKey"

        # Here's a sitewide configuration setting.
        sitewide_conf = ConfigurationSetting.sitewide(db.session, key)

        # Its value is not set.
        assert None == sitewide_conf.value

        # Set it.
        sitewide_conf.value = "Sitewide value"
        assert "Sitewide value" == sitewide_conf.value

        # Here's an integration, let's say the SIP2 authentication mechanism
        sip, ignore = create(
            db.session,
            ExternalIntegration,
            goal=ExternalIntegration.PATRON_AUTH_GOAL,
            protocol="SIP2",
        )

        # It happens to a ConfigurationSetting for the same key used
        # in the sitewide configuration.
        sip_conf = ConfigurationSetting.for_externalintegration(key, sip)

        # But because the meaning of a configuration key differ so
        # widely across integrations, the SIP2 integration does not
        # inherit the sitewide value for the key.
        assert None == sip_conf.value
        sip_conf.value = "SIP2 value"

        # Here's a library which has a ConfigurationSetting for the same
        # key used in the sitewide configuration.
        library = db.default_library()
        library_conf = ConfigurationSetting.for_library(key, library)

        # Since all libraries use a given ConfigurationSetting to mean
        # the same thing, a library _does_ inherit the sitewide value
        # for a configuration setting.
        assert "Sitewide value" == library_conf.value

        # Change the site-wide configuration, and the default also changes.
        sitewide_conf.value = "New site-wide value"
        assert "New site-wide value" == library_conf.value

        # The per-library value takes precedence over the site-wide
        # value.
        library_conf.value = "Per-library value"
        assert "Per-library value" == library_conf.value

        # Now let's consider a setting like the patron identifier
        # prefix.  This is set on the combination of a library and a
        # SIP2 integration.
        key = "patron_identifier_prefix"
        library_patron_prefix_conf = (
            ConfigurationSetting.for_library_and_externalintegration(
                db.session, key, library, sip
            )
        )
        assert None == library_patron_prefix_conf.value

        # If the SIP2 integration has a value set for this
        # ConfigurationSetting, that value is inherited for every
        # individual library that uses the integration.
        generic_patron_prefix_conf = ConfigurationSetting.for_externalintegration(
            key, sip
        )
        assert None == generic_patron_prefix_conf.value
        generic_patron_prefix_conf.value = "Integration-specific value"
        assert "Integration-specific value" == library_patron_prefix_conf.value

        # Change the value on the integration, and the default changes
        # for each individual library.
        generic_patron_prefix_conf.value = "New integration-specific value"
        assert "New integration-specific value" == library_patron_prefix_conf.value

        # The library+integration setting takes precedence over the
        # integration setting.
        library_patron_prefix_conf.value = "Library-specific value"
        assert "Library-specific value" == library_patron_prefix_conf.value

    def test_duplicate(self, db: DatabaseTransactionFixture):
        """You can't have two ConfigurationSettings for the same key,
        library, and external integration.

        (test_relationships shows that you can have two settings for the same
        key as long as library or integration is different.)
        """
        key = db.fresh_str()
        integration, ignore = create(
            db.session,
            ExternalIntegration,
            goal=db.fresh_str(),
            protocol=db.fresh_str(),
        )
        library = db.default_library()
        setting = ConfigurationSetting.for_library_and_externalintegration(
            db.session, key, library, integration
        )
        setting2 = ConfigurationSetting.for_library_and_externalintegration(
            db.session, key, library, integration
        )
        assert setting.id == setting2.id
        pytest.raises(
            IntegrityError,
            create,
            db.session,
            ConfigurationSetting,
            key=key,
            library=library,
            external_integration=integration,
        )

    def test_relationships(self, db: DatabaseTransactionFixture):
        integration, ignore = create(
            db.session,
            ExternalIntegration,
            goal=db.fresh_str(),
            protocol=db.fresh_str(),
        )
        assert [] == integration.settings

        library = db.default_library()
        assert [] == library.settings

        # Create four different ConfigurationSettings with the same key.
        cs = ConfigurationSetting
        key = db.fresh_str()

        for_neither = cs.sitewide(db.session, key)
        assert None == for_neither.library
        assert None == for_neither.external_integration

        for_library = cs.for_library(key, library)
        assert library == for_library.library
        assert None == for_library.external_integration

        for_integration = cs.for_externalintegration(key, integration)
        assert None == for_integration.library
        assert integration == for_integration.external_integration

        for_both = cs.for_library_and_externalintegration(
            db.session, key, library, integration
        )
        assert library == for_both.library
        assert integration == for_both.external_integration

        # We got four distinct objects with the same key.
        objs = [for_neither, for_library, for_integration, for_both]
        assert 4 == len(set(objs))
        for o in objs:
            assert o.key == key

        assert [for_library, for_both] == library.settings
        assert [for_integration, for_both] == integration.settings
        assert library == for_both.library
        assert integration == for_both.external_integration

        # If we delete the integration, all configuration settings
        # associated with it are deleted, even the one that's also
        # associated with the library.
        db.session.delete(integration)
        db.session.commit()
        assert [for_library.id] == [x.id for x in library.settings]

    def test_no_orphan_delete_cascade(self, db: DatabaseTransactionFixture):
        # Disconnecting a ConfigurationSetting from a Library or
        # ExternalIntegration doesn't delete it, because it's fine for
        # a ConfigurationSetting to have no associated Library or
        # ExternalIntegration.
        library = db.default_library()
        for_library = ConfigurationSetting.for_library(db.fresh_str(), library)

        integration = db.external_integration(db.fresh_str())
        for_integration = ConfigurationSetting.for_externalintegration(
            db.fresh_str(), integration
        )

        # Remove library and external_integration.
        for_library.library = None
        for_integration.external_integration = None
        db.session.commit()

        # That was a weird thing to do, but the ConfigurationSettings
        # are still in the database.
        for cs in for_library, for_integration:
            assert cs == get_one(db.session, ConfigurationSetting, id=cs.id)

    @pytest.mark.parametrize(
        "set_to,expect",
        [(None, None), (1, "1"), ("snowman", "snowman"), ("☃".encode(), "☃")],
    )
    def test_setter_parameterized(self, db, set_to, expect):
        # Values are converted into Unicode strings on the way in to
        # the 'value' setter.
        setting = ConfigurationSetting.sitewide(db.session, "setting")
        setting.value = set_to
        assert setting.value == expect

    def test_stored_bytes_value(self, db: DatabaseTransactionFixture):
        bytes_setting = ConfigurationSetting.sitewide(db.session, "bytes_setting")
        assert bytes_setting.value is None

        bytes_setting.value = "1234 ☃".encode()
        assert "1234 ☃" == bytes_setting.value

        with pytest.raises(UnicodeDecodeError):
            bytes_setting.value = b"\x80"

    def test_int_value(self, db: DatabaseTransactionFixture):
        number = ConfigurationSetting.sitewide(db.session, "number")
        assert None == number.int_value

        number.value = "1234"
        assert 1234 == number.int_value

        number.value = "tra la la"
        pytest.raises(ValueError, lambda: number.int_value)

    def test_float_value(self, db: DatabaseTransactionFixture):
        number = ConfigurationSetting.sitewide(db.session, "number")
        assert None == number.int_value

        number.value = "1234.5"
        assert 1234.5 == number.float_value

        number.value = "tra la la"
        pytest.raises(ValueError, lambda: number.float_value)

    def test_json_value(self, db: DatabaseTransactionFixture):
        jsondata = ConfigurationSetting.sitewide(db.session, "json")
        assert None == jsondata.int_value

        jsondata.value = "[1,2]"
        assert [1, 2] == jsondata.json_value

        jsondata.value = "tra la la"
        pytest.raises(ValueError, lambda: jsondata.json_value)

    def test_excluded_audio_data_sources(self, db: DatabaseTransactionFixture):
        # Get a handle on the underlying ConfigurationSetting
        setting = ConfigurationSetting.sitewide(
            db.session, Configuration.EXCLUDED_AUDIO_DATA_SOURCES
        )
        m = ConfigurationSetting.excluded_audio_data_sources
        # When no explicit value is set for the ConfigurationSetting,
        # the return value of the method is AUDIO_EXCLUSIONS -- whatever
        # the default is for the current version of the circulation manager.
        assert None == setting.value
        assert ConfigurationSetting.EXCLUDED_AUDIO_DATA_SOURCES_DEFAULT == m(db.session)
        # When an explicit value for the ConfigurationSetting, is set, that
        # value is interpreted as JSON and returned.
        setting.value = "[]"
        assert [] == m(db.session)

    def test_explain(self, db: DatabaseTransactionFixture):
        """Test that ConfigurationSetting.explain gives information
        about all site-wide configuration settings.
        """
        ConfigurationSetting.sitewide(db.session, "a_secret").value = "1"
        ConfigurationSetting.sitewide(db.session, "nonsecret_setting").value = "2"

        integration = db.external_integration("a protocol", "a goal")

        actual = ConfigurationSetting.explain(db.session, include_secrets=True)
        expect = """Site-wide configuration settings:
---------------------------------
a_secret='1'
nonsecret_setting='2'"""
        assert expect == "\n".join(actual)

        without_secrets = "\n".join(
            ConfigurationSetting.explain(db.session, include_secrets=False)
        )
        assert "a_secret" not in without_secrets
        assert "nonsecret_setting" in without_secrets


class TestUniquenessConstraints:
    def test_duplicate_sitewide_setting(self, db: DatabaseTransactionFixture):
        # You can't create two sitewide settings with the same key.
        c1 = ConfigurationSetting(key="key", value="value1")
        db.session.add(c1)
        db.session.flush()
        c2 = ConfigurationSetting(key="key", value="value2")
        db.session.add(c2)
        pytest.raises(IntegrityError, db.session.flush)

    def test_duplicate_library_setting(self, db: DatabaseTransactionFixture):
        # A library can't have two settings with the same key.
        c1 = ConfigurationSetting(
            key="key", value="value1", library=db.default_library()
        )
        db.session.add(c1)
        db.session.flush()
        c2 = ConfigurationSetting(
            key="key", value="value2", library=db.default_library()
        )
        db.session.add(c2)
        pytest.raises(IntegrityError, db.session.flush)

    def test_duplicate_integration_setting(self, db: DatabaseTransactionFixture):
        # An external integration can't have two settings with the
        # same key.
        integration = db.external_integration(db.fresh_str())
        c1 = ConfigurationSetting(
            key="key", value="value1", external_integration=integration
        )
        db.session.add(c1)
        db.session.flush()
        c2 = ConfigurationSetting(
            key="key", value="value1", external_integration=integration
        )
        db.session.add(c2)
        pytest.raises(IntegrityError, db.session.flush)

    def test_duplicate_library_integration_setting(
        self, db: DatabaseTransactionFixture
    ):
        # A library can't configure an external integration two
        # different ways for the same key.
        integration = db.external_integration(db.fresh_str())
        c1 = ConfigurationSetting(
            key="key",
            value="value1",
            library=db.default_library(),
            external_integration=integration,
        )
        db.session.add(c1)
        db.session.flush()
        c2 = ConfigurationSetting(
            key="key",
            value="value1",
            library=db.default_library(),
            external_integration=integration,
        )
        db.session.add(c2)
        pytest.raises(IntegrityError, db.session.flush)


class TestExternalIntegrationLink:
    def test_collection_mirror_settings(self):
        settings = ExternalIntegrationLink.COLLECTION_MIRROR_SETTINGS

        assert settings[0]["key"] == ExternalIntegrationLink.COVERS_KEY
        assert settings[0]["label"] == "Covers Mirror"
        assert (
            settings[0]["options"][0]["key"]
            == ExternalIntegrationLink.NO_MIRROR_INTEGRATION
        )
        assert settings[0]["options"][0]["label"] == _(
            "None - Do not mirror cover images"
        )

        assert settings[1]["key"] == ExternalIntegrationLink.OPEN_ACCESS_BOOKS_KEY
        assert settings[1]["label"] == "Open Access Books Mirror"
        assert (
            settings[1]["options"][0]["key"]
            == ExternalIntegrationLink.NO_MIRROR_INTEGRATION
        )
        assert settings[1]["options"][0]["label"] == _(
            "None - Do not mirror free books"
        )

        assert settings[2]["key"] == ExternalIntegrationLink.PROTECTED_ACCESS_BOOKS_KEY
        assert settings[2]["label"] == "Protected Access Books Mirror"
        assert (
            settings[2]["options"][0]["key"]
            == ExternalIntegrationLink.NO_MIRROR_INTEGRATION
        )
        assert settings[2]["options"][0]["label"] == _(
            "None - Do not mirror self-hosted, commercially licensed books"
        )

    def test_relationships(self, db: DatabaseTransactionFixture):
        # Create a collection with two storage external integrations.
        collection = db.collection(
            name="Collection",
            protocol=ExternalIntegration.OVERDRIVE,
        )

        storage1 = db.external_integration(
            name="integration1",
            protocol=ExternalIntegration.S3,
        )
        storage2 = db.external_integration(
            name="integration2",
            protocol=ExternalIntegration.S3,
            goal=ExternalIntegration.STORAGE_GOAL,
            username="username",
            password="password",
        )

        # Two external integration links need to be created to associate
        # the collection's external integration with the two storage
        # external integrations.
        s1_external_integration_link = db.external_integration_link(
            integration=collection.external_integration,
            other_integration=storage1,
            purpose="covers_mirror",
        )
        s2_external_integration_link = db.external_integration_link(
            integration=collection.external_integration,
            other_integration=storage2,
            purpose="books_mirror",
        )

        qu = db.session.query(ExternalIntegrationLink).order_by(
            ExternalIntegrationLink.other_integration_id
        )
        external_integration_links = qu.all()

        assert len(external_integration_links) == 2
        assert external_integration_links[0].other_integration_id == storage1.id
        assert external_integration_links[1].other_integration_id == storage2.id

        # When a storage integration is deleted, the related external
        # integration link row is deleted, and the relationship with the
        # collection is removed.
        db.session.delete(storage1)

        qu = db.session.query(ExternalIntegrationLink)
        external_integration_links = qu.all()

        assert len(external_integration_links) == 1
        assert external_integration_links[0].other_integration_id == storage2.id


class ExampleExternalIntegrationFixture:
    external_integration: ExternalIntegration
    database_fixture: DatabaseTransactionFixture

    def __init__(
        self,
        external_integration: ExternalIntegration,
        database_transaction: DatabaseTransactionFixture,
    ):
        self.external_integration = external_integration
        self.database_fixture = database_transaction


@pytest.fixture()
def example_externalintegration_fixture(
    db,
) -> ExampleExternalIntegrationFixture:
    e = db.external_integration(goal=db.fresh_str(), protocol=db.fresh_str())
    return ExampleExternalIntegrationFixture(e, db)


class TestExternalIntegration:
    def test_for_library_and_goal(
        self, example_externalintegration_fixture: ExampleExternalIntegrationFixture
    ):
        db = example_externalintegration_fixture.database_fixture
        external_integration = example_externalintegration_fixture.external_integration

        goal = external_integration.goal
        qu = ExternalIntegration.for_library_and_goal(
            db.session, db.default_library(), goal
        )

        # This matches nothing because the ExternalIntegration is not
        # associated with the Library.
        assert [] == qu.all()
        get_one = ExternalIntegration.one_for_library_and_goal
        assert None == get_one(db.session, db.default_library(), goal)

        # Associate the library with the ExternalIntegration and
        # the query starts matching it. one_for_library_and_goal
        # also starts returning it.
        external_integration.libraries.append(db.default_library())
        assert [external_integration] == qu.all()
        assert external_integration == get_one(db.session, db.default_library(), goal)

        # Create another, similar ExternalIntegration. By itself, this
        # has no effect.
        integration2, ignore = create(
            db.session, ExternalIntegration, goal=goal, protocol=db.fresh_str()
        )
        assert [external_integration] == qu.all()
        assert external_integration == get_one(db.session, db.default_library(), goal)

        # Associate that ExternalIntegration with the library, and
        # the query starts picking it up, and one_for_library_and_goal
        # starts raising an exception.
        integration2.libraries.append(db.default_library())
        assert {external_integration, integration2} == set(qu.all())
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            get_one(db.session, db.default_library(), goal)
        assert "Library {} defines multiple integrations with goal {}".format(
            db.default_library().name, goal
        ) in str(excinfo.value)

    def test_for_collection_and_purpose(
        self, example_externalintegration_fixture: ExampleExternalIntegrationFixture
    ):
        db = example_externalintegration_fixture.database_fixture
        wrong_purpose = "isbn"
        collection = db.collection()

        with pytest.raises(CannotLoadConfiguration) as excinfo:
            ExternalIntegration.for_collection_and_purpose(
                db.session, collection, wrong_purpose
            )
        assert (
            "No storage integration for collection '%s' and purpose '%s' is configured"
            % (collection.name, wrong_purpose)
            in str(excinfo.value)
        )

        external_integration = db.external_integration("some protocol")
        collection.external_integration_id = external_integration.id
        purpose = "covers_mirror"
        db.external_integration_link(integration=external_integration, purpose=purpose)

        integration = ExternalIntegration.for_collection_and_purpose(
            db.session, collection=collection, purpose=purpose
        )
        assert isinstance(integration, ExternalIntegration)

    def test_with_setting_value(
        self, example_externalintegration_fixture: ExampleExternalIntegrationFixture
    ):
        db = example_externalintegration_fixture.database_fixture

        def results():
            # Run the query and return all results.
            return ExternalIntegration.with_setting_value(
                db.session, "protocol", "goal", "key", "value"
            ).all()

        # We start off with no results.
        assert [] == results()

        # This ExternalIntegration will not match the result,
        # even though protocol and goal match, because it
        # doesn't have the 'key' ConfigurationSetting set.
        integration = db.external_integration("protocol", "goal")
        assert [] == results()

        # Now 'key' is set, but set to the wrong value.
        setting = integration.setting("key")
        setting.value = "wrong"
        assert [] == results()

        # Now it's set to the right value, so we get a result.
        setting.value = "value"
        assert [integration] == results()

        # Create another, identical integration.
        integration2, is_new = create(
            db.session, ExternalIntegration, protocol="protocol", goal="goal"
        )
        assert integration2 != integration
        integration2.setting("key").value = "value"

        # Both integrations show up.
        assert {integration, integration2} == set(results())

        # If the integration's goal doesn't match, it doesn't show up.
        integration2.goal = "wrong"
        assert [integration] == results()

        # If the integration's protocol doesn't match, it doesn't show up.
        integration.protocol = "wrong"
        assert [] == results()

    def test_data_source(
        self, example_externalintegration_fixture: ExampleExternalIntegrationFixture
    ):
        db = example_externalintegration_fixture.database_fixture

        # For most collections, the protocol determines the
        # data source.
        collection = db.collection(protocol=ExternalIntegration.OVERDRIVE)
        assert DataSource.OVERDRIVE == collection.data_source.name

        # For OPDS Import collections, data source is a setting which
        # might not be present.
        assert None == db.default_collection().data_source

        # data source will be automatically created if necessary.
        DatabaseTransactionFixture.set_settings(
            db.default_collection().integration_configuration,
            **{Collection.DATA_SOURCE_NAME_SETTING: "New Data Source"}
        )
        assert "New Data Source" == db.default_collection().data_source.name

    def test_set_key_value_pair(
        self, example_externalintegration_fixture: ExampleExternalIntegrationFixture
    ):
        """Test the ability to associate extra key-value pairs with
        an ExternalIntegration.
        """
        integration = example_externalintegration_fixture.external_integration
        assert [] == integration.settings

        setting = integration.set_setting("website_id", "id1")
        assert "website_id" == setting.key
        assert "id1" == setting.value

        # Calling set() again updates the key-value pair.
        assert [setting.id] == [x.id for x in integration.settings]
        setting2 = integration.set_setting("website_id", "id2")
        assert setting.id == setting2.id
        assert "id2" == setting2.value

        assert setting2 == integration.setting("website_id")

    def test_explain(
        self, example_externalintegration_fixture: ExampleExternalIntegrationFixture
    ):
        db = example_externalintegration_fixture.database_fixture
        integration = db.external_integration("protocol", "goal")
        integration.name = "The Integration"
        integration.url = "http://url/"
        integration.username = "someuser"
        integration.password = "somepass"
        integration.setting("somesetting").value = "somevalue"

        # Two different libraries have slightly different
        # configurations for this integration.
        db.default_library().name = "First Library"
        db.default_library().integrations.append(integration)
        ConfigurationSetting.for_library_and_externalintegration(
            db.session, "library-specific", db.default_library(), integration
        ).value = "value1"

        library2 = db.library()
        library2.name = "Second Library"
        library2.integrations.append(integration)
        ConfigurationSetting.for_library_and_externalintegration(
            db.session, "library-specific", library2, integration
        ).value = "value2"

        # If we decline to pass in a library, we get information about how
        # each library in the system configures this integration.

        expect = (
            """ID: %s
Name: The Integration
Protocol/Goal: protocol/goal
library-specific='value1' (applies only to First Library)
library-specific='value2' (applies only to Second Library)
somesetting='somevalue'
url='http://url/'
username='someuser'"""
            % integration.id
        )
        actual = integration.explain()
        assert expect == "\n".join(actual)

        # If we pass in a library, we only get information about
        # how that specific library configures the integration.
        for_library_2 = "\n".join(integration.explain(library=library2))
        assert "applies only to First Library" not in for_library_2
        assert "applies only to Second Library" in for_library_2

        # If we pass in True for include_secrets, we see the passwords.
        with_secrets = integration.explain(include_secrets=True)
        assert "password='somepass'" in with_secrets

    def test_custom_accept_header(
        self, example_externalintegration_fixture: ExampleExternalIntegrationFixture
    ):
        db = example_externalintegration_fixture.database_fixture

        integration = db.external_integration("protocol", "goal")
        # Must be empty if not set
        assert integration.custom_accept_header == None

        # Must be the same value if set
        integration.custom_accept_header = "custom header"
        assert integration.custom_accept_header == "custom header"

    def test_delete(
        self, example_externalintegration_fixture: ExampleExternalIntegrationFixture
    ):
        """Ensure that ExternalIntegration.delete clears all orphan ExternalIntegrationLinks."""
        session = example_externalintegration_fixture.database_fixture.session
        db = example_externalintegration_fixture.database_fixture

        integration1 = db.external_integration(
            ExternalIntegration.MANUAL,
            ExternalIntegration.LICENSE_GOAL,
            libraries=[db.default_library()],
        )
        integration2 = db.external_integration(
            ExternalIntegration.S3,
            ExternalIntegration.STORAGE_GOAL,
            libraries=[db.default_library()],
        )

        # Set up a a link associating integration2 with integration1.
        link1 = db.external_integration_link(
            integration1,
            db.default_library(),
            integration2,
            ExternalIntegrationLink.PROTECTED_ACCESS_BOOKS,
        )
        link2 = db.external_integration_link(
            integration1,
            db.default_library(),
            integration2,
            ExternalIntegrationLink.COVERS,
        )

        # Delete integration1.
        session.delete(integration1)

        # Ensure that there are no orphan links.
        links = session.query(ExternalIntegrationLink).all()
        for link in (link1, link2):
            assert link not in links

        # Ensure that the first integration was successfully removed.
        external_integrations = session.query(ExternalIntegration).all()
        assert integration1 not in external_integrations
        assert integration2 in external_integrations


SETTING1_KEY = "setting1"
SETTING1_LABEL = "Setting 1's label"
SETTING1_DESCRIPTION = "Setting 1's description"
SETTING1_TYPE = ConfigurationAttributeType.TEXT
SETTING1_REQUIRED = False
SETTING1_DEFAULT = "12345"
SETTING1_CATEGORY = "Settings"

SETTING2_KEY = "setting2"
SETTING2_LABEL = "Setting 2's label"
SETTING2_DESCRIPTION = "Setting 2's description"
SETTING2_TYPE = ConfigurationAttributeType.SELECT
SETTING2_REQUIRED = False
SETTING2_DEFAULT = "value1"
SETTING2_OPTIONS = [
    ConfigurationOption("key1", "value1"),
    ConfigurationOption("key2", "value2"),
    ConfigurationOption("key3", "value3"),
]
SETTING2_CATEGORY = "Settings"

SETTING3_KEY = "setting3"
SETTING3_LABEL = "Setting 3's label"
SETTING3_DESCRIPTION = "Setting 3's description"
SETTING3_TYPE = ConfigurationAttributeType.MENU
SETTING3_REQUIRED = False
SETTING3_OPTIONS = [
    ConfigurationOption("key1", "value1"),
    ConfigurationOption("key2", "value2"),
    ConfigurationOption("key3", "value3"),
]
SETTING3_DEFAULT = [SETTING3_OPTIONS[0].key, SETTING3_OPTIONS[1].key]
SETTING3_CATEGORY = "Settings"


SETTING4_KEY = "setting4"
SETTING4_LABEL = "Setting 4's label"
SETTING4_DESCRIPTION = "Setting 4's description"
SETTING4_TYPE = ConfigurationAttributeType.LIST
SETTING4_REQUIRED = False
SETTING4_OPTIONS = None
SETTING4_DEFAULT = None
SETTING4_CATEGORY = "Settings"

SETTING5_KEY = "setting5"
SETTING5_LABEL = "Setting 5's label"
SETTING5_DESCRIPTION = "Setting 5's description"
SETTING5_TYPE = ConfigurationAttributeType.NUMBER
SETTING5_REQUIRED = False
SETTING5_DEFAULT = 12345
SETTING5_CATEGORY = "Settings"


class MockConfiguration(ConfigurationGrouping):
    setting1 = ConfigurationMetadata(
        key=SETTING1_KEY,
        label=SETTING1_LABEL,
        description=SETTING1_DESCRIPTION,
        type=SETTING1_TYPE,
        required=SETTING1_REQUIRED,
        default=SETTING1_DEFAULT,
        category=SETTING1_CATEGORY,
    )

    setting2 = ConfigurationMetadata(
        key=SETTING2_KEY,
        label=SETTING2_LABEL,
        description=SETTING2_DESCRIPTION,
        type=SETTING2_TYPE,
        required=SETTING2_REQUIRED,
        default=SETTING2_DEFAULT,
        options=SETTING2_OPTIONS,
        category=SETTING2_CATEGORY,
    )

    setting3 = ConfigurationMetadata(
        key=SETTING3_KEY,
        label=SETTING3_LABEL,
        description=SETTING3_DESCRIPTION,
        type=SETTING3_TYPE,
        required=SETTING3_REQUIRED,
        default=SETTING3_DEFAULT,
        options=SETTING3_OPTIONS,
        category=SETTING3_CATEGORY,
    )

    setting4 = ConfigurationMetadata(
        key=SETTING4_KEY,
        label=SETTING4_LABEL,
        description=SETTING4_DESCRIPTION,
        type=SETTING4_TYPE,
        required=SETTING4_REQUIRED,
        default=SETTING4_DEFAULT,
        options=SETTING4_OPTIONS,
        category=SETTING4_CATEGORY,
    )

    setting5 = ConfigurationMetadata(
        key=SETTING5_KEY,
        label=SETTING5_LABEL,
        description=SETTING5_DESCRIPTION,
        type=SETTING5_TYPE,
        required=SETTING5_REQUIRED,
        default=SETTING5_DEFAULT,
        category=SETTING5_CATEGORY,
    )


class ConfigurationWithBooleanProperty(ConfigurationGrouping):
    boolean_setting = ConfigurationMetadata(
        key="boolean_setting",
        label="Boolean Setting",
        description="Boolean Setting",
        type=ConfigurationAttributeType.SELECT,
        required=True,
        default="true",
        options=[
            ConfigurationOption("true", "True"),
            ConfigurationOption("false", "False"),
        ],
    )


class MockConfiguration2(ConfigurationGrouping):
    setting1 = ConfigurationMetadata(
        key="setting1",
        label=SETTING1_LABEL,
        description=SETTING1_DESCRIPTION,
        type=SETTING1_TYPE,
        required=SETTING1_REQUIRED,
        default=SETTING1_DEFAULT,
        category=SETTING1_CATEGORY,
        index=1,
    )

    setting2 = ConfigurationMetadata(
        key="setting2",
        label=SETTING2_LABEL,
        description=SETTING2_DESCRIPTION,
        type=SETTING2_TYPE,
        required=SETTING2_REQUIRED,
        default=SETTING2_DEFAULT,
        options=SETTING2_OPTIONS,
        category=SETTING2_CATEGORY,
        index=0,
    )


class TestConfigurationOption:
    def test_to_settings(self):
        # Arrange
        option = ConfigurationOption("key1", "value1")
        expected_result = {"key": "key1", "label": "value1"}

        # Act
        result = option.to_settings()

        # Assert
        assert result == expected_result

    def test_from_enum(self):
        # Arrange
        class TestEnum(Enum):
            LABEL1 = "KEY1"
            LABEL2 = "KEY2"

        expected_result = [
            ConfigurationOption("KEY1", "LABEL1"),
            ConfigurationOption("KEY2", "LABEL2"),
        ]

        # Act
        result = ConfigurationOption.from_enum(TestEnum)

        # Assert
        assert result == expected_result


class TestConfigurationGrouping:
    @pytest.mark.parametrize(
        "_,setting_name,expected_value",
        [("setting1", "setting1", 12345), ("setting2", "setting2", "12345")],
    )
    def test_getters(self, _, setting_name, expected_value):
        # Arrange
        configuration_storage = create_autospec(spec=ConfigurationStorage)
        configuration_storage.load = MagicMock(return_value=expected_value)
        db = create_autospec(spec=sqlalchemy.orm.session.Session)
        configuration = MockConfiguration(configuration_storage, db)

        # Act
        setting_value = getattr(configuration, setting_name)

        # Assert
        assert setting_value == expected_value
        configuration_storage.load.assert_called_once_with(db, setting_name)

    @pytest.mark.parametrize(
        "_,setting_name,db_value,expected_value",
        [
            (
                "default_menu_value",
                MockConfiguration.setting3.key,
                None,
                MockConfiguration.setting3.default,
            ),
            (
                "menu_value",
                MockConfiguration.setting3.key,
                json.dumps(
                    [
                        MockConfiguration.setting3.options[0].key,
                        MockConfiguration.setting3.options[1].key,
                    ]
                ),
                [
                    MockConfiguration.setting3.options[0].key,
                    MockConfiguration.setting3.options[1].key,
                ],
            ),
            (
                "default_list_value",
                MockConfiguration.setting4.key,
                None,
                MockConfiguration.setting4.default,
            ),
            (
                "menu_value",
                MockConfiguration.setting4.key,
                json.dumps(["value1", "value2"]),
                ["value1", "value2"],
            ),
        ],
    )
    def test_menu_and_list_getters(self, _, setting_name, db_value, expected_value):
        # Arrange
        configuration_storage = create_autospec(spec=ConfigurationStorage)
        configuration_storage.load = MagicMock(return_value=db_value)
        db = create_autospec(spec=sqlalchemy.orm.session.Session)
        configuration = MockConfiguration(configuration_storage, db)

        # Act
        setting_value = getattr(configuration, setting_name)

        # Assert
        assert setting_value == expected_value
        configuration_storage.load.assert_called_once_with(db, setting_name)

    def test_getter_return_default_value(self):
        # Arrange
        configuration_storage = create_autospec(spec=ConfigurationStorage)
        configuration_storage.load = MagicMock(return_value=None)
        db = create_autospec(spec=sqlalchemy.orm.session.Session)
        configuration = MockConfiguration(configuration_storage, db)

        # Act
        setting1_value = configuration.setting1
        setting5_value = configuration.setting5

        # Assert
        assert SETTING1_DEFAULT == setting1_value
        assert SETTING5_DEFAULT == setting5_value

    @pytest.mark.parametrize(
        "_,setting_name,expected_value",
        [("setting1", "setting1", 12345), ("setting2", "setting2", "12345")],
    )
    def test_setters(self, _, setting_name, expected_value):
        # Arrange
        configuration_storage = create_autospec(spec=ConfigurationStorage)
        configuration_storage.save = MagicMock(return_value=expected_value)
        db = create_autospec(spec=sqlalchemy.orm.session.Session)
        configuration = MockConfiguration(configuration_storage, db)

        # Act
        setattr(configuration, setting_name, expected_value)

        # Assert
        configuration_storage.save.assert_called_once_with(
            db, setting_name, expected_value
        )

    def test_to_settings_considers_default_indices(self):
        # Act
        settings = MockConfiguration.to_settings()

        # Assert
        assert len(settings) == 5

        assert settings[0][ConfigurationAttribute.KEY.value] == SETTING1_KEY
        assert settings[0][ConfigurationAttribute.LABEL.value] == SETTING1_LABEL
        assert (
            settings[0][ConfigurationAttribute.DESCRIPTION.value]
            == SETTING1_DESCRIPTION
        )
        assert settings[0][ConfigurationAttribute.TYPE.value] == None
        assert settings[0][ConfigurationAttribute.REQUIRED.value] == SETTING1_REQUIRED
        assert settings[0][ConfigurationAttribute.DEFAULT.value] == SETTING1_DEFAULT
        assert settings[0][ConfigurationAttribute.CATEGORY.value] == SETTING1_CATEGORY

        assert settings[1][ConfigurationAttribute.KEY.value] == SETTING2_KEY
        assert settings[1][ConfigurationAttribute.LABEL.value] == SETTING2_LABEL
        assert (
            settings[1][ConfigurationAttribute.DESCRIPTION.value]
            == SETTING2_DESCRIPTION
        )
        assert settings[1][ConfigurationAttribute.TYPE.value] == SETTING2_TYPE.value
        assert settings[1][ConfigurationAttribute.REQUIRED.value] == SETTING2_REQUIRED
        assert settings[1][ConfigurationAttribute.DEFAULT.value] == SETTING2_DEFAULT
        assert settings[1][ConfigurationAttribute.OPTIONS.value] == [
            option.to_settings() for option in SETTING2_OPTIONS
        ]
        assert settings[1][ConfigurationAttribute.CATEGORY.value] == SETTING2_CATEGORY

        assert settings[2][ConfigurationAttribute.KEY.value] == SETTING3_KEY
        assert settings[2][ConfigurationAttribute.LABEL.value] == SETTING3_LABEL
        assert (
            settings[2][ConfigurationAttribute.DESCRIPTION.value]
            == SETTING3_DESCRIPTION
        )
        assert settings[2][ConfigurationAttribute.TYPE.value] == SETTING3_TYPE.value
        assert settings[2][ConfigurationAttribute.REQUIRED.value] == SETTING3_REQUIRED
        assert settings[2][ConfigurationAttribute.DEFAULT.value] == SETTING3_DEFAULT
        assert settings[2][ConfigurationAttribute.OPTIONS.value] == [
            option.to_settings() for option in SETTING3_OPTIONS
        ]
        assert settings[2][ConfigurationAttribute.CATEGORY.value] == SETTING3_CATEGORY

        assert settings[3][ConfigurationAttribute.KEY.value] == SETTING4_KEY
        assert settings[3][ConfigurationAttribute.LABEL.value] == SETTING4_LABEL
        assert (
            settings[3][ConfigurationAttribute.DESCRIPTION.value]
            == SETTING4_DESCRIPTION
        )
        assert settings[3][ConfigurationAttribute.TYPE.value] == SETTING4_TYPE.value
        assert settings[3][ConfigurationAttribute.REQUIRED.value] == SETTING4_REQUIRED
        assert settings[3][ConfigurationAttribute.DEFAULT.value] == SETTING4_DEFAULT
        assert settings[3][ConfigurationAttribute.CATEGORY.value] == SETTING4_CATEGORY

    def test_to_settings_considers_explicit_indices(self):
        # Act
        settings = MockConfiguration2.to_settings()

        # Assert
        assert len(settings) == 2

        assert settings[0][ConfigurationAttribute.KEY.value] == SETTING2_KEY
        assert settings[0][ConfigurationAttribute.LABEL.value] == SETTING2_LABEL
        assert (
            settings[0][ConfigurationAttribute.DESCRIPTION.value]
            == SETTING2_DESCRIPTION
        )
        assert settings[0][ConfigurationAttribute.TYPE.value] == SETTING2_TYPE.value
        assert settings[0][ConfigurationAttribute.REQUIRED.value] == SETTING2_REQUIRED
        assert settings[0][ConfigurationAttribute.DEFAULT.value] == SETTING2_DEFAULT
        assert settings[0][ConfigurationAttribute.OPTIONS.value] == [
            option.to_settings() for option in SETTING2_OPTIONS
        ]
        assert settings[0][ConfigurationAttribute.CATEGORY.value] == SETTING2_CATEGORY

        assert settings[1][ConfigurationAttribute.KEY.value] == SETTING1_KEY
        assert settings[1][ConfigurationAttribute.LABEL.value] == SETTING1_LABEL
        assert (
            settings[1][ConfigurationAttribute.DESCRIPTION.value]
            == SETTING1_DESCRIPTION
        )
        assert settings[1][ConfigurationAttribute.TYPE.value] == None
        assert settings[1][ConfigurationAttribute.REQUIRED.value] == SETTING1_REQUIRED
        assert settings[1][ConfigurationAttribute.DEFAULT.value] == SETTING1_DEFAULT
        assert settings[1][ConfigurationAttribute.CATEGORY.value] == SETTING1_CATEGORY


class TestNumberConfigurationMetadata:
    def test_number_type_getter(self, db: DatabaseTransactionFixture):
        # Arrange
        external_integration = db.external_integration("test")
        external_integration_association = create_autospec(spec=HasExternalIntegration)
        external_integration_association.external_integration = MagicMock(
            return_value=external_integration
        )
        configuration_storage = ConfigurationStorage(external_integration_association)
        configuration = MockConfiguration(configuration_storage, db.session)

        configuration.setting5 = "abc"
        with pytest.raises(CannotLoadConfiguration):
            configuration.setting5

        configuration.setting5 = "123"
        assert configuration.setting5 == 123.0

        configuration.setting5 = ""
        assert configuration.setting5 == SETTING5_DEFAULT


class TestBooleanConfigurationMetadata:
    @pytest.mark.parametrize(
        "provided,expected",
        [
            ("true", True),
            ("t", True),
            ("yes", True),
            ("y", True),
            (1, False),
            ("false", False),
        ],
    )
    def test_configuration_metadata_correctly_cast_bool_values(
        self, db: DatabaseTransactionFixture, provided, expected
    ):
        """Ensure that ConfigurationMetadata.to_bool correctly translates different values into boolean (True/False)."""
        # Arrange
        external_integration = db.external_integration("test")

        external_integration_association = create_autospec(spec=HasExternalIntegration)
        external_integration_association.external_integration = MagicMock(
            return_value=external_integration
        )

        configuration_storage = ConfigurationStorage(external_integration_association)

        configuration = ConfigurationWithBooleanProperty(
            configuration_storage, db.session
        )

        # We set a new value using ConfigurationMetadata.__set__
        configuration.boolean_setting = provided

        # Act
        # We read the existing value using ConfigurationMetadata.__get__
        result = ConfigurationMetadata.to_bool(configuration.boolean_setting)

        # Assert
        assert expected == result
