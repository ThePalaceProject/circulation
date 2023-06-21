import pytest
from flask import url_for

from api.adobe_vendor_id import AuthdataUtility
from api.problem_details import INVALID_CREDENTIALS
from core.model import DataSource
from core.util.problem_detail import ProblemDetail
from tests.fixtures.api_controller import ControllerFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.vendor_id import VendorIDFixture


class DeviceManagementFixture(ControllerFixture):
    def __init__(
        self, db: DatabaseTransactionFixture, vendor_id_fixture: VendorIDFixture
    ):
        super().__init__(db, vendor_id_fixture, setup_cm=True)
        vendor_id_fixture.initialize_adobe(self.library, self.libraries)
        self.auth = dict(Authorization=self.valid_auth)

        # Since our library doesn't have its Adobe configuration
        # enabled, the Device Management Protocol controller has not
        # been enabled.
        assert None == self.manager.adobe_device_management

        # Set up the Adobe configuration for this library and
        # reload the CirculationManager configuration.
        self.manager.setup_adobe_vendor_id(db.session, self.library)
        self.manager.load_settings()

        # Now the controller is enabled and we can use it in this
        # test.
        self.controller = self.manager.adobe_device_management


@pytest.fixture(scope="function")
def device_fixture(db: DatabaseTransactionFixture, vendor_id_fixture: VendorIDFixture):
    return DeviceManagementFixture(db, vendor_id_fixture)


class TestDeviceManagementProtocolController:
    def _create_credential(self, device_fixture: DeviceManagementFixture):
        """Associate a credential with the default patron which
        can have Adobe device identifiers associated with it,
        """
        return device_fixture.db.credential(
            DataSource.INTERNAL_PROCESSING,
            AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
            device_fixture.default_patron,
        )

    def test_link_template_header(self, device_fixture: DeviceManagementFixture):
        """Test the value of the Link-Template header used in
        device_id_list_handler.
        """
        with device_fixture.request_context_with_library("/"):
            headers = device_fixture.controller.link_template_header
            assert 1 == len(headers)
            template = headers["Link-Template"]
            expected_url = url_for(
                "adobe_drm_device",
                library_short_name=device_fixture.library.short_name,
                device_id="{id}",
                _external=True,
            )
            expected_url = expected_url.replace("%7Bid%7D", "{id}")
            assert '<%s>; rel="item"' % expected_url == template

    def test__request_handler_failure(self, device_fixture: DeviceManagementFixture):
        """You cannot create a DeviceManagementRequestHandler
        without providing a patron.
        """
        result = device_fixture.controller._request_handler(None)

        assert isinstance(result, ProblemDetail)
        assert INVALID_CREDENTIALS.uri == result.uri
        assert "No authenticated patron" == result.detail

    def test_device_id_list_handler_post_success(
        self, device_fixture: DeviceManagementFixture
    ):
        # The patron has no credentials, and thus no registered devices.
        assert [] == device_fixture.default_patron.credentials
        headers = dict(device_fixture.auth)
        headers["Content-Type"] = device_fixture.controller.DEVICE_ID_LIST_MEDIA_TYPE
        with device_fixture.request_context_with_library(
            "/", method="POST", headers=headers, data="device"
        ):
            device_fixture.controller.authenticated_patron_from_request()
            response = device_fixture.controller.device_id_list_handler()
            assert 200 == response.status_code

            # We just registered a new device with the patron. This
            # automatically created an appropriate Credential for
            # them.
            [credential] = device_fixture.default_patron.credentials  # type: ignore
            assert DataSource.INTERNAL_PROCESSING == credential.data_source.name  # type: ignore
            assert AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER == credential.type  # type: ignore

            assert ["device"] == [
                x.device_identifier for x in credential.drm_device_identifiers  # type: ignore
            ]

    def test_device_id_list_handler_get_success(
        self, device_fixture: DeviceManagementFixture
    ):
        credential = self._create_credential(device_fixture)
        credential.register_drm_device_identifier("device1")
        credential.register_drm_device_identifier("device2")
        with device_fixture.request_context_with_library(
            "/", headers=device_fixture.auth
        ):
            device_fixture.controller.authenticated_patron_from_request()
            response = device_fixture.controller.device_id_list_handler()
            assert 200 == response.status_code

            # We got a list of device IDs.
            assert (
                device_fixture.controller.DEVICE_ID_LIST_MEDIA_TYPE
                == response.headers["Content-Type"]
            )
            assert "device1\ndevice2" == response.get_data(as_text=True)

            # We got a URL Template (see test_link_template_header())
            # that explains how to address any particular device ID.
            expect = device_fixture.controller.link_template_header
            for k, v in list(expect.items()):
                assert response.headers[k] == v

    def device_id_list_handler_bad_auth(self, device_fixture: DeviceManagementFixture):
        with device_fixture.request_context_with_library("/"):
            device_fixture.controller.authenticated_patron_from_request()
            response = device_fixture.manager.adobe_vendor_id.device_id_list_handler()
            assert isinstance(response, ProblemDetail)
            assert 401 == response.status_code

    def device_id_list_handler_bad_method(
        self, device_fixture: DeviceManagementFixture
    ):
        with device_fixture.request_context_with_library(
            "/", method="DELETE", headers=device_fixture.auth
        ):
            device_fixture.controller.authenticated_patron_from_request()
            response = device_fixture.controller.device_id_list_handler()
            assert isinstance(response, ProblemDetail)
            assert 405 == response.status_code

    def test_device_id_list_handler_too_many_simultaneous_registrations(
        self, device_fixture: DeviceManagementFixture
    ):
        # We only allow registration of one device ID at a time.
        headers = dict(device_fixture.auth)
        headers["Content-Type"] = device_fixture.controller.DEVICE_ID_LIST_MEDIA_TYPE
        with device_fixture.request_context_with_library(
            "/", method="POST", headers=headers, data="device1\ndevice2"
        ):
            device_fixture.controller.authenticated_patron_from_request()
            response = device_fixture.controller.device_id_list_handler()
            assert 413 == response.status_code
            assert "You may only register one device ID at a time." == response.detail

    def test_device_id_list_handler_wrong_media_type(
        self, device_fixture: DeviceManagementFixture
    ):
        headers = dict(device_fixture.auth)
        headers["Content-Type"] = "text/plain"
        with device_fixture.request_context_with_library(
            "/", method="POST", headers=headers, data="device1\ndevice2"
        ):
            device_fixture.controller.authenticated_patron_from_request()
            response = device_fixture.controller.device_id_list_handler()
            assert 415 == response.status_code
            assert (
                "Expected vnd.librarysimplified/drm-device-id-list document."
                == response.detail
            )

    def test_device_id_handler_success(self, device_fixture: DeviceManagementFixture):
        credential = self._create_credential(device_fixture)
        credential.register_drm_device_identifier("device")

        with device_fixture.request_context_with_library(
            "/", method="DELETE", headers=device_fixture.auth
        ):
            patron = device_fixture.controller.authenticated_patron_from_request()
            response = device_fixture.controller.device_id_handler("device")
            assert 200 == response.status_code

    def test_device_id_handler_bad_auth(self, device_fixture: DeviceManagementFixture):
        with device_fixture.request_context_with_library("/", method="DELETE"):
            patron = device_fixture.controller.authenticated_patron_from_request()
            response = device_fixture.controller.device_id_handler("device")
            assert isinstance(response, ProblemDetail)
            assert 401 == response.status_code

    def test_device_id_handler_bad_method(
        self, device_fixture: DeviceManagementFixture
    ):
        with device_fixture.request_context_with_library(
            "/", method="POST", headers=device_fixture.auth
        ):
            patron = device_fixture.controller.authenticated_patron_from_request()
            response = device_fixture.controller.device_id_handler("device")
            assert isinstance(response, ProblemDetail)
            assert 405 == response.status_code
            assert "Only DELETE is supported." == response.detail
