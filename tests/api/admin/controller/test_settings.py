from typing import Any

import pytest

from api.admin.controller.settings import SettingsController
from api.admin.problem_details import (
    DUPLICATE_INTEGRATION,
    INTEGRATION_URL_ALREADY_IN_USE,
    NO_PROTOCOL_FOR_NEW_SERVICE,
    PROTOCOL_DOES_NOT_SUPPORT_PARENTS,
    UNKNOWN_PROTOCOL,
)
from core.integration.base import (
    HasChildIntegrationConfiguration,
    HasLibraryIntegrationConfiguration,
)
from core.integration.goals import Goals
from core.integration.registry import IntegrationRegistry
from core.integration.settings import BaseSettings, ConfigurationFormItem, FormField
from core.model import ExternalIntegration
from core.util.problem_detail import ProblemError
from tests.fixtures.api_admin import AdminControllerFixture, SettingsControllerFixture


class TestSettingsController:
    def test_get_integration_protocols(
        self, admin_ctrl_fixture: AdminControllerFixture
    ):
        """Test the _get_integration_protocols helper method."""

        class Protocol(HasChildIntegrationConfiguration):
            SITEWIDE = True
            LIBRARY_SETTINGS = [6]
            CARDINALITY = 1

            class ChildSettings(BaseSettings):
                key: int = FormField(form=ConfigurationFormItem("key"))

            @classmethod
            def child_settings_class(cls):
                return cls.ChildSettings

            @classmethod
            def settings_class(cls):
                return BaseSettings

            @classmethod
            def label(cls):
                return "my label"

            @classmethod
            def description(cls):
                return "my description"

        [result] = SettingsController(
            admin_ctrl_fixture.manager
        )._get_integration_protocols([Protocol])
        expect = dict(
            sitewide=True,
            description="my description",
            settings=[],
            library_settings=[6],
            child_settings=[{"label": "key", "key": "key", "required": True}],
            label="my label",
            cardinality=1,
            name="my label",
        )
        assert expect == result

    def test_get_integration_info(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        """Test the _get_integration_info helper method."""
        m = settings_ctrl_fixture.controller._get_integration_info

        # Test the case where there are integrations in the database
        # with the given goal, but none of them match the
        # configuration.
        goal = settings_ctrl_fixture.ctrl.db.fresh_str()
        integration = settings_ctrl_fixture.ctrl.db.external_integration(
            protocol="a protocol", goal=goal
        )
        assert [] == m(goal, [dict(name="some other protocol")])

    def test_create_integration(self, settings_ctrl_fixture: SettingsControllerFixture):
        """Test the _create_integration helper method."""

        m = settings_ctrl_fixture.controller._create_integration

        protocol_definitions = [
            dict(name="allow many"),
            dict(name="allow one", cardinality=1),
        ]
        goal = "some goal"

        # You get an error if you don't pass in a protocol.
        assert (NO_PROTOCOL_FOR_NEW_SERVICE, False) == m(
            protocol_definitions, None, goal
        )

        # You get an error if you do provide a protocol but no definition
        # for it can be found.
        assert (UNKNOWN_PROTOCOL, False) == m(
            protocol_definitions, "no definition", goal
        )

        # If the protocol has multiple cardinality you can create as many
        # integrations using that protocol as you want.
        i1, is_new1 = m(protocol_definitions, "allow many", goal)
        assert True == is_new1

        i2, is_new2 = m(protocol_definitions, "allow many", goal)
        assert True == is_new2

        assert i1 != i2
        for i in [i1, i2]:
            assert "allow many" == i.protocol
            assert goal == i.goal

        # If the protocol has single cardinality, you can only create one
        # integration using that protocol before you start getting errors.
        i1, is_new1 = m(protocol_definitions, "allow one", goal)
        assert True == is_new1

        i2, is_new2 = m(protocol_definitions, "allow one", goal)
        assert False == is_new2
        assert DUPLICATE_INTEGRATION == i2

    def test_check_url_unique(self, settings_ctrl_fixture: SettingsControllerFixture):
        # Verify our ability to catch duplicate integrations for a
        # given URL.
        m = settings_ctrl_fixture.controller.check_url_unique

        # Here's an ExternalIntegration.
        original = settings_ctrl_fixture.ctrl.db.external_integration(
            url="http://service/", protocol="a protocol", goal="a goal"
        )
        assert isinstance(original, ExternalIntegration)
        protocol = original.protocol
        goal = original.goal

        # Here's another ExternalIntegration that might or might not
        # be about to become a duplicate of the original.
        new = settings_ctrl_fixture.ctrl.db.external_integration(
            protocol=protocol, goal="new goal"
        )
        new.goal = original.goal
        assert new != original

        # We're going to call this helper function multiple times to check if
        # different scenarios trip the "duplicate" logic.
        def is_dupe(url, protocol, goal):
            result = m(new, url, protocol, goal)
            if result is None:
                return False
            elif result is INTEGRATION_URL_ALREADY_IN_USE:
                return True
            else:
                raise Exception(
                    "check_url_unique must return either the problem detail or None"
                )

        # The original ExternalIntegration is not a duplicate of itself.
        assert None == m(original, original.url, protocol, goal)

        # However, any other ExternalIntegration with the same URL,
        # protocol, and goal is considered a duplicate.
        assert True == is_dupe(original.url, protocol, goal)

        # Minor URL differences are ignored when considering duplicates
        # -- this is with help from url_variants().
        assert True == is_dupe("https://service/", protocol, goal)
        assert True == is_dupe("https://service", protocol, goal)

        # Not all variants are handled in this way
        assert False == is_dupe("https://service/#fragment", protocol, goal)

        # If any of URL, protocol, and goal are different, then the
        # integration is not considered a duplicate.
        assert False == is_dupe("different url", protocol, goal)
        assert False == is_dupe(original.url, "different protocol", goal)
        assert False == is_dupe(original.url, protocol, "different goal")

        # If you're not considering a URL at all, we assume no
        # duplicate.
        assert False == is_dupe(None, protocol, goal)

    def test_url_variants(self):
        # Test the helper method that generates slight variants of
        # any given URL.
        def m(url):
            return list(SettingsController.url_variants(url))

        # No URL, no variants.
        assert [] == m(None)
        assert [] == m("not a url")

        # Variants of an HTTP URL with a trailing slash.
        assert ["http://url/", "http://url", "https://url/", "https://url"] == m(
            "http://url/"
        )

        # Variants of an HTTPS URL with a trailing slash.
        assert ["https://url/", "https://url", "http://url/", "http://url"] == m(
            "https://url/"
        )

        # Variants of a URL with no trailing slash.
        assert ["https://url", "https://url/", "http://url", "http://url/"] == m(
            "https://url"
        )

    def test__get_protocol_class(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        _get_protocol_class = settings_ctrl_fixture.controller._get_settings_class
        registry = IntegrationRegistry[Any](Goals.LICENSE_GOAL)

        class P1Settings(BaseSettings):
            pass

        class Protocol1:
            @classmethod
            def settings_class(cls):
                return P1Settings

        class P2Settings(BaseSettings):
            pass

        class P2ChildSettings(BaseSettings):
            pass

        class Protocol2(HasChildIntegrationConfiguration):
            @classmethod
            def settings_class(cls):
                return P2Settings

            @classmethod
            def child_settings_class(cls):
                return P2ChildSettings

        registry.register(Protocol1, canonical="1")
        registry.register(Protocol2, canonical="2")

        assert _get_protocol_class(registry, "3") == None
        assert _get_protocol_class(registry, "1") == P1Settings
        assert _get_protocol_class(registry, "2") == P2Settings
        assert _get_protocol_class(registry, "2", is_child=True) == P2ChildSettings
        assert (
            _get_protocol_class(registry, "1", is_child=True)
            == PROTOCOL_DOES_NOT_SUPPORT_PARENTS
        )

    def test__set_configuration_library(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        db = settings_ctrl_fixture.ctrl.db
        config = db.default_collection().integration_configuration
        _set_configuration_library = (
            settings_ctrl_fixture.controller._set_configuration_library
        )
        library = db.library(short_name="short-name")

        class P1LibrarySettings(BaseSettings):
            key: str
            value: str

        class Protocol1(
            HasLibraryIntegrationConfiguration[BaseSettings, P1LibrarySettings]
        ):
            @classmethod
            def library_settings_class(cls):
                return P1LibrarySettings

            @classmethod
            def label(cls):
                pass

            @classmethod
            def description(cls):
                pass

            @classmethod
            def settings_class(cls):
                pass

        with pytest.raises(RuntimeError) as runtime_error_raised:
            _set_configuration_library(
                config, dict(short_name="not-short-name"), Protocol1
            )
        assert (
            str(runtime_error_raised.value)
            == "Could not find the configuration library"
        )

        with pytest.raises(ProblemError) as problem_error_raised:
            _set_configuration_library(config, dict(short_name="short-name"), Protocol1)
        assert (
            problem_error_raised.value.problem_detail.detail
            == "Required field 'key' is missing."
        )

        result = _set_configuration_library(
            config, dict(short_name="short-name", key="key", value="value"), Protocol1
        )
        assert result.library == library
        assert result.settings_dict == dict(key="key", value="value")
