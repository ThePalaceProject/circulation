import flask
import pytest
from flask import url_for
from werkzeug.datastructures import MultiDict

from api.admin.exceptions import *
from api.odl import SharedODLAPI
from api.registration.registry import Registration
from core.model import AdminRole, ConfigurationSetting, Library, create
from core.util.http import HTTP
from tests.fixtures.api_admin import SettingsControllerFixture


class TestCollectionRegistration:
    """Test the process of registering a specific collection with
    a RemoteRegistry.
    """

    def test_collection_library_registrations_get(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        db = settings_ctrl_fixture.ctrl.db.session

        collection = settings_ctrl_fixture.ctrl.db.default_collection()
        succeeded, ignore = create(
            db,
            Library,
            name="Library 1",
            short_name="L1",
        )
        ConfigurationSetting.for_library_and_externalintegration(
            db,
            "library-registration-status",
            succeeded,
            collection.external_integration,
        ).value = "success"
        failed, ignore = create(
            db,
            Library,
            name="Library 2",
            short_name="L2",
        )
        ConfigurationSetting.for_library_and_externalintegration(
            db,
            "library-registration-status",
            failed,
            collection.external_integration,
        ).value = "failure"
        unregistered, ignore = create(
            db,
            Library,
            name="Library 3",
            short_name="L3",
        )
        collection.libraries = [succeeded, failed, unregistered]

        with settings_ctrl_fixture.request_context_with_admin("/", method="GET"):
            response = (
                settings_ctrl_fixture.manager.admin_collection_library_registrations_controller.process_collection_library_registrations()
            )

            serviceInfo = response.get("library_registrations")
            assert 1 == len(serviceInfo)
            assert collection.id == serviceInfo[0].get("id")

            libraryInfo = serviceInfo[0].get("libraries")
            expected = [
                dict(short_name=succeeded.short_name, status="success"),
                dict(short_name=failed.short_name, status="failure"),
            ]
            assert expected == libraryInfo

            settings_ctrl_fixture.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            db.flush()
            pytest.raises(
                AdminNotAuthorized,
                settings_ctrl_fixture.manager.admin_collection_library_registrations_controller.process_collection_library_registrations,
            )

    def test_collection_library_registrations_post(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        """Test what might happen POSTing to collection_library_registrations."""
        # First test the failure cases.

        m = (
            settings_ctrl_fixture.manager.admin_collection_library_registrations_controller.process_collection_library_registrations
        )

        # Here, the user doesn't have permission to start the
        # registration process.
        settings_ctrl_fixture.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            pytest.raises(AdminNotAuthorized, m)
        settings_ctrl_fixture.admin.add_role(AdminRole.SYSTEM_ADMIN)

        # The collection ID doesn't correspond to any real collection.
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([("collection_id", "1234")])
            response = m()
            assert MISSING_COLLECTION == response

        # Pass in a collection ID so that doesn't happen again.
        collection = settings_ctrl_fixture.ctrl.db.collection()
        collection.external_account_id = "collection url"

        # Oops, the collection doesn't actually support registration.
        form = MultiDict(
            [
                ("collection_id", collection.id),
                ("library_short_name", "not-a-library"),
            ]
        )
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = form
            response = m()
            assert COLLECTION_DOES_NOT_SUPPORT_REGISTRATION == response

        # Change the protocol to one that supports registration.
        collection.protocol = SharedODLAPI.NAME

        # Now the problem is the library doesn't correspond to a real
        # library.
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = form
            response = m()
            assert NO_SUCH_LIBRARY == response

        # The push() implementation might return a ProblemDetail for any
        # number of reasons.
        library = settings_ctrl_fixture.ctrl.db.default_library()
        form = MultiDict(
            [
                ("collection_id", collection.id),
                ("library_short_name", library.short_name),
            ]
        )

        class Mock(Registration):
            def push(self, *args, **kwargs):
                return REMOTE_INTEGRATION_FAILED

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = form
            assert REMOTE_INTEGRATION_FAILED == m(registration_class=Mock)

        # But if that doesn't happen, success!
        class Mock(Registration):
            """When asked to push a registration, do nothing and say it
            worked.
            """

            called_with = None

            def push(self, *args, **kwargs):
                Mock.called_with = (args, kwargs)
                return True

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = form
            result = m(registration_class=Mock)
            assert 200 == result.status_code

            # push() was called with the arguments we would expect.
            args, kwargs = Mock.called_with
            assert (Registration.PRODUCTION_STAGE, url_for) == args

            # We would have made real HTTP requests.
            assert HTTP.debuggable_post == kwargs.pop("do_post")
            assert HTTP.debuggable_get == kwargs.pop("do_get")
            # And passed the collection URL over to the shared collection.
            assert collection.external_account_id == kwargs.pop("catalog_url")
            # No other weird keyword arguments were passed in.
            assert {} == kwargs
