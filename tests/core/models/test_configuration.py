import pytest
from sqlalchemy.exc import IntegrityError

from core.config import CannotLoadConfiguration, Configuration
from core.model import create, get_one
from core.model.configuration import ConfigurationSetting, ExternalIntegration
from core.model.datasource import DataSource
from core.opds_import import OPDSAPI
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
        library_conf = ConfigurationSetting.for_library_and_externalintegration(
            db.session, key, library, None
        )

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

        # Create four different ConfigurationSettings with the same key.
        cs = ConfigurationSetting
        key = db.fresh_str()

        for_neither = cs.sitewide(db.session, key)
        assert None == for_neither.library
        assert None == for_neither.external_integration

        for_library = cs.for_library_and_externalintegration(
            db.session, key, library, None
        )
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

        assert [for_library, for_both] == library.external_integration_settings
        assert [for_integration, for_both] == integration.settings
        assert library == for_both.library
        assert integration == for_both.external_integration

        # If we delete the integration, all configuration settings
        # associated with it are deleted, even the one that's also
        # associated with the library.
        db.session.delete(integration)
        db.session.commit()
        assert [for_library.id] == [x.id for x in library.external_integration_settings]

    def test_no_orphan_delete_cascade(self, db: DatabaseTransactionFixture):
        # Disconnecting a ConfigurationSetting from a Library or
        # ExternalIntegration doesn't delete it, because it's fine for
        # a ConfigurationSetting to have no associated Library or
        # ExternalIntegration.
        library = db.default_library()
        for_library = ConfigurationSetting.for_library_and_externalintegration(
            db.session, db.fresh_str(), library, None
        )

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
        c1 = ConfigurationSetting(key="key", _value="value1")
        db.session.add(c1)
        db.session.flush()
        c2 = ConfigurationSetting(key="key", _value="value2")
        db.session.add(c2)
        pytest.raises(IntegrityError, db.session.flush)

    def test_duplicate_library_setting(self, db: DatabaseTransactionFixture):
        # A library can't have two settings with the same key.
        c1 = ConfigurationSetting(
            key="key", _value="value1", library=db.default_library()
        )
        db.session.add(c1)
        db.session.flush()
        c2 = ConfigurationSetting(
            key="key", _value="value2", library=db.default_library()
        )
        db.session.add(c2)
        pytest.raises(IntegrityError, db.session.flush)

    def test_duplicate_integration_setting(self, db: DatabaseTransactionFixture):
        # An external integration can't have two settings with the
        # same key.
        integration = db.external_integration(db.fresh_str())
        c1 = ConfigurationSetting(
            key="key", _value="value1", external_integration=integration
        )
        db.session.add(c1)
        db.session.flush()
        c2 = ConfigurationSetting(
            key="key", _value="value1", external_integration=integration
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
            _value="value1",
            library=db.default_library(),
            external_integration=integration,
        )
        db.session.add(c1)
        db.session.flush()
        c2 = ConfigurationSetting(
            key="key",
            _value="value1",
            library=db.default_library(),
            external_integration=integration,
        )
        db.session.add(c2)
        pytest.raises(IntegrityError, db.session.flush)


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
        assert collection.data_source is not None
        assert collection.data_source.name == DataSource.OVERDRIVE

        # For OPDS Import collections, data source is a setting which
        # might not be present.
        opds_collection = db.collection(protocol=ExternalIntegration.OPDS_IMPORT)
        assert opds_collection.data_source is None

        # data source will be automatically created if necessary.
        settings = OPDSAPI.settings_class()(
            external_account_id="http://url.com/feed", data_source="New Data Source"
        )
        OPDSAPI.settings_update(opds_collection.integration_configuration, settings)
        assert isinstance(opds_collection.data_source, DataSource)
        assert opds_collection.data_source.name == "New Data Source"  # type: ignore[unreachable]

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
