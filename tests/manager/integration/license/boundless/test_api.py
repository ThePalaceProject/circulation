from __future__ import annotations

import copy
import datetime
import json
from functools import partial
from unittest.mock import MagicMock, create_autospec, patch

import pytest
from freezegun import freeze_time

from palace.manager.api.circulation.data import HoldInfo, LoanInfo
from palace.manager.api.circulation.exceptions import (
    CannotFulfill,
    DeliveryMechanismError,
    FormatNotAvailable,
    InvalidInputException,
    NoActiveLoan,
    NotFoundOnRemote,
    RemoteInitiatedServerError,
)
from palace.manager.api.circulation.fulfillment import DirectFulfillment
from palace.manager.celery.tasks import boundless as boundless_tasks
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.format import FormatData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.feed.annotator.circulation import CirculationManagerAnnotator
from palace.manager.integration.license.boundless.api import BoundlessApi
from palace.manager.integration.license.boundless.constants import BoundlessFormat
from palace.manager.integration.license.boundless.exception import (
    BoundlessValidationError,
)
from palace.manager.integration.license.boundless.fulfillment import (
    BoundlessAcsFulfillment,
)
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.resource import Representation
from palace.manager.util.datetime_helpers import datetime_utc, utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.library import LibraryFixture
from tests.fixtures.services import ServicesFixture
from tests.manager.integration.license.boundless.conftest import BoundlessFixture


class TestBoundlessApi:
    def test__run_self_tests(
        self,
        db: DatabaseTransactionFixture,
        boundless: BoundlessFixture,
    ):
        # Verify that BoundlessApi._run_self_tests() calls the right
        # methods.
        api = boundless.api

        mock_refresh_bearer_token = create_autospec(
            api.api_requests.refresh_bearer_token,
            return_value=MagicMock(access_token="the new token"),
        )
        api.api_requests.refresh_bearer_token = mock_refresh_bearer_token
        mock_availability = create_autospec(
            api.api_requests.availability,
            return_value=MagicMock(titles=[(1, "a"), (2, "b"), (3, "c")]),
        )
        api.api_requests.availability = mock_availability
        mock_patron_activity = create_autospec(
            api.patron_activity, return_value=["loan", "hold"]
        )
        api.patron_activity = mock_patron_activity

        # Now let's make sure two Libraries have access to this
        # Collection -- one library with a default patron and one
        # without.
        no_default_patron = db.library()
        boundless.collection.associated_libraries.append(no_default_patron)

        with_default_patron = db.default_library()
        db.simple_auth_integration(with_default_patron)

        # Now that everything is set up, run the self-test.
        time_run = utc_now()
        with freeze_time(time_run):
            [
                refresh_bearer_token,
                recent_circulation_events,
                patron_activity,
                no_patron_credential,
                pools_without_delivery,
            ] = list(api._run_self_tests(db.session))
        assert refresh_bearer_token.name == "Refreshing bearer token"
        assert refresh_bearer_token.success is True
        assert refresh_bearer_token.result == "the new token"
        mock_refresh_bearer_token.assert_called_once()

        assert (
            no_patron_credential.name
            == "Acquiring test patron credentials for library %s"
            % no_default_patron.name
        )
        assert no_patron_credential.success is False
        assert (
            str(no_patron_credential.exception)
            == "Library has no test patron configured."
        )

        assert (
            recent_circulation_events.name
            == "Asking for circulation events for the last five minutes"
        )
        assert recent_circulation_events.success is True
        assert recent_circulation_events.result == "Found 3 event(s)"
        mock_availability.assert_called_once_with(
            since=time_run - datetime.timedelta(minutes=5)
        )

        assert (
            patron_activity.name
            == "Checking activity for test patron for library %s"
            % with_default_patron.name
        )
        assert patron_activity.success is True
        assert patron_activity.result == "Found 2 loans/holds"
        mock_patron_activity.assert_called_once()
        patron, pin = mock_patron_activity.call_args.args
        assert patron.authorization_identifier == "username1"
        assert pin == "password1"

        assert (
            pools_without_delivery.name
            == "Checking for titles that have no delivery mechanisms."
        )
        assert pools_without_delivery.success is True
        assert (
            pools_without_delivery.result
            == "All titles in this collection have delivery mechanisms."
        )

    def test__run_self_tests_short_circuit(self, boundless: BoundlessFixture):
        # If we can't refresh the bearer token, the rest of the
        # self-tests aren't even run.

        api = boundless.api
        api.api_requests.refresh_bearer_token = MagicMock(
            side_effect=Exception("no way")
        )

        # Now that everything is set up, run the self-test. Only one
        # test will be run.
        [failure] = api._run_self_tests(boundless.db.session)
        assert failure.name == "Refreshing bearer token"
        assert failure.success is False
        assert failure.exception is not None
        assert failure.exception.args[0] == "no way"

    def test_create_identifier_strings(self, boundless: BoundlessFixture):
        identifier = boundless.db.identifier()
        values = BoundlessApi.create_identifier_strings(["foo", identifier])
        assert ["foo", identifier.identifier] == values

    def test_update_availability(self, boundless: BoundlessFixture):
        # Test the Boundless implementation of the update_availability method
        # defined by the CirculationAPI interface.

        # Create a LicensePool that needs updating.
        edition, pool = boundless.db.edition(
            identifier_type=Identifier.AXIS_360_ID,
            data_source_name=DataSource.BOUNDLESS,
            with_license_pool=True,
            collection=boundless.collection,
        )

        # We have never checked the circulation information for this
        # LicensePool. Put some random junk in the pool to verify
        # that it gets changed.
        pool.licenses_owned = 10
        pool.licenses_available = 5
        pool.patrons_in_hold_queue = 3
        assert None == pool.last_checked

        # Prepare availability information.
        data = boundless.files.sample_data("availability_with_loans.xml")

        # Modify the data so that it appears to be talking about the
        # book we just created.
        new_identifier = pool.identifier.identifier
        data = data.replace(b"0012533119", new_identifier.encode("utf8"))

        boundless.http_client.queue_response(200, content=data)

        boundless.api.update_availability(pool)

        # The availability information has been udpated, as has the
        # date the availability information was last checked.
        assert 2 == pool.licenses_owned
        assert 1 == pool.licenses_available
        assert 0 == pool.patrons_in_hold_queue
        assert pool.last_checked is not None

    def test_checkin_success(self, boundless: BoundlessFixture):
        # Verify that we can make a request to the EarlyCheckInTitle
        # endpoint and get a good response.
        edition, pool = boundless.db.edition(
            identifier_type=Identifier.AXIS_360_ID,
            data_source_name=DataSource.BOUNDLESS,
            with_license_pool=True,
        )
        data = boundless.files.sample_data("checkin_success.xml")
        boundless.http_client.queue_response(200, content=data)
        patron = boundless.db.patron()
        barcode = boundless.db.fresh_str()
        patron.authorization_identifier = barcode
        boundless.api.checkin(patron, "pin", pool)

        # Verify the format of the HTTP request that was made.
        assert boundless.http_client.requests_methods[1] == "GET"
        assert "/EarlyCheckInTitle/v3" in boundless.http_client.requests[1]
        assert boundless.http_client.requests_args[1]["params"] == {
            "itemID": pool.identifier.identifier,
            "patronID": barcode,
        }

    def test_checkin_failure(self, boundless: BoundlessFixture):
        # Verify that we correctly handle failure conditions sent from
        # the EarlyCheckInTitle endpoint.
        edition, pool = boundless.db.edition(
            identifier_type=Identifier.AXIS_360_ID,
            data_source_name=DataSource.BOUNDLESS,
            with_license_pool=True,
        )
        data = boundless.files.sample_data("checkin_failure.xml")
        boundless.http_client.queue_response(200, content=data)
        patron = boundless.db.patron()
        patron.authorization_identifier = boundless.db.fresh_str()
        with pytest.raises(NotFoundOnRemote):
            boundless.api.checkin(patron, "pin", pool)

    def test_place_hold(
        self, boundless: BoundlessFixture, library_fixture: LibraryFixture
    ):
        edition, pool = boundless.db.edition(
            identifier_type=Identifier.AXIS_360_ID,
            data_source_name=DataSource.BOUNDLESS,
            with_license_pool=True,
        )
        data = boundless.files.sample_data("place_hold_success.xml")
        boundless.http_client.queue_response(200, content=data)
        library = library_fixture.library()
        library_settings = library_fixture.settings(library)
        patron = boundless.db.patron(library=library)
        library_settings.default_notification_email_address = (
            "notifications@example.com"
        )

        response = boundless.api.place_hold(patron, "pin", pool, None)
        assert response.hold_position == 1
        assert response.identifier_type == pool.identifier.type
        assert response.identifier == pool.identifier.identifier
        params = boundless.http_client.requests_args[1]["params"]
        assert params is not None
        assert params["email"] == "notifications@example.com"

    def test_release_hold(self, boundless: BoundlessFixture):
        edition, pool = boundless.db.edition(
            identifier_type=Identifier.AXIS_360_ID,
            data_source_name=DataSource.BOUNDLESS,
            with_license_pool=True,
        )
        data = boundless.files.sample_data("release_hold_success.xml")
        boundless.http_client.queue_response(200, content=data)
        patron = boundless.db.patron()
        barcode = boundless.db.fresh_str()
        patron.authorization_identifier = barcode

        boundless.api.release_hold(patron, "pin", pool)

        # Verify the format of the HTTP request that was made.
        assert boundless.http_client.requests_methods[1] == "GET"
        assert "/removeHold/v2" in boundless.http_client.requests[1]
        assert boundless.http_client.requests_args[1]["params"] == {
            "titleId": pool.identifier.identifier,
            "patronId": barcode,
        }

    def test_checkout(self, boundless: BoundlessFixture):
        # Verify that we can make a request to the CheckoutTitle
        # endpoint and get a good response.
        edition, pool = boundless.db.edition(
            identifier_type=Identifier.AXIS_360_ID,
            data_source_name=DataSource.BOUNDLESS,
            with_license_pool=True,
        )
        data = boundless.files.sample_data("checkout_success.xml")
        boundless.http_client.queue_response(200, content=data)
        patron = boundless.db.patron()
        barcode = boundless.db.fresh_str()
        patron.authorization_identifier = barcode
        delivery_mechanism = pool.delivery_mechanisms[0]

        boundless.api.checkout(patron, "pin", pool, delivery_mechanism)

        # Verify the format of the HTTP request that was made.
        assert boundless.http_client.requests_methods[1] == "POST"
        assert "/checkout/v2" in boundless.http_client.requests[1]
        assert boundless.http_client.requests_args[1]["params"] == {
            "titleId": pool.identifier.identifier,
            "patronId": barcode,
            "format": "ePub",
        }

    def test_fulfill_errors(self, boundless: BoundlessFixture):
        # Test our ability to fulfill a Boundless title.
        edition, pool = boundless.db.edition(
            identifier_type=Identifier.AXIS_360_ID,
            data_source_name=DataSource.BOUNDLESS,
            with_license_pool=True,
        )

        patron = boundless.db.patron()
        patron.authorization_identifier = "a barcode"
        delivery_mechanism = pool.delivery_mechanisms[0]

        fulfill = partial(
            boundless.api.fulfill,
            patron,
            "pin",
            licensepool=pool,
            delivery_mechanism=delivery_mechanism,
        )

        # If Boundless says a patron does not have a title checked out,
        # an attempt to fulfill that title will fail with NoActiveLoan.
        data = boundless.files.sample_data("availability_with_ebook_fulfillment.xml")
        boundless.http_client.queue_response(200, content=data)
        with pytest.raises(NoActiveLoan):
            fulfill()

        # If the title is checked out but Boundless provides no download link,
        # the exception is CannotFulfill.
        pool.identifier.identifier = "0015176429"
        data = boundless.files.sample_data("availability_without_fulfillment.xml")
        boundless.http_client.queue_response(200, content=data)
        with pytest.raises(CannotFulfill):
            fulfill()

        # If Boundless shows the title as checked out, but in a format that we did
        # not request, we get a FormatNotAvailable exception.
        delivery_mechanism.delivery_mechanism.content_type = (
            Representation.EPUB_MEDIA_TYPE
        )
        delivery_mechanism.delivery_mechanism.drm_scheme = (
            DeliveryMechanism.BAKER_TAYLOR_KDRM_DRM
        )
        pool.identifier.identifier = "0016820953"
        data = boundless.files.sample_data("availability_with_ebook_fulfillment.xml")
        boundless.http_client.queue_response(200, content=data)
        with pytest.raises(FormatNotAvailable):
            fulfill()

        # Test errors with additional API calls made during fulfillment.
        pool.identifier.identifier = "0012244222"
        delivery_mechanism.delivery_mechanism.content_type = None
        delivery_mechanism.delivery_mechanism.drm_scheme = (
            DeliveryMechanism.FINDAWAY_DRM
        )
        data_dict = json.loads(
            boundless.files.sample_data("audiobook_fulfillment_info.json")
        )

        # Now strategically remove required information from the
        # document and verify that extraction fails.
        for field in (
            "FNDLicenseID",
            "FNDSessionKey",
            "ExpirationDate",
        ):
            missing_field = data_dict.copy()
            del missing_field[field]
            boundless.http_client.queue_response(
                200,
                content=(
                    boundless.files.sample_data(
                        "availability_with_audiobook_fulfillment.xml"
                    )
                ),
            )
            boundless.http_client.queue_response(200, content=json.dumps(missing_field))
            with pytest.raises(BoundlessValidationError):
                fulfill()

        # Try with a bad expiration date.
        bad_date = data_dict.copy()
        bad_date["ExpirationDate"] = "not-a-date"
        boundless.http_client.queue_response(
            200,
            content=(
                boundless.files.sample_data(
                    "availability_with_audiobook_fulfillment.xml"
                )
            ),
        )
        boundless.http_client.queue_response(200, content=json.dumps(bad_date))
        with pytest.raises(BoundlessValidationError):
            fulfill()

        # Try with an expired session key.
        expired_session_key = data_dict.copy()
        expired_session_key["FNDSessionKey"] = "Expired"
        boundless.http_client.queue_response(
            200,
            content=(
                boundless.files.sample_data(
                    "availability_with_audiobook_fulfillment.xml"
                )
            ),
        )
        boundless.http_client.queue_response(
            200, content=json.dumps(expired_session_key)
        )
        with pytest.raises(
            RemoteInitiatedServerError, match="Expired findaway session key"
        ):
            fulfill()

    def test_fulfill_acs(self, boundless: BoundlessFixture):
        # Test our ability to fulfill a Boundless title.
        edition, pool = boundless.db.edition(
            identifier_type=Identifier.AXIS_360_ID,
            identifier_id="0015176429",
            data_source_name=DataSource.BOUNDLESS,
            with_license_pool=True,
        )

        patron = boundless.db.patron()
        patron.authorization_identifier = "a barcode"
        delivery_mechanism = pool.delivery_mechanisms[0]

        fulfill = partial(
            boundless.api.fulfill,
            patron,
            "pin",
            licensepool=pool,
            delivery_mechanism=delivery_mechanism,
        )

        # If an ebook is checked out and we're asking for it to be
        # fulfilled through Adobe DRM, we get a Boundless360AcsFulfillment
        # object with a content link.
        data = boundless.files.sample_data("availability_with_loan_and_hold.xml")
        boundless.http_client.queue_response(200, content=data)
        fulfillment = fulfill()
        assert isinstance(fulfillment, BoundlessAcsFulfillment)
        assert fulfillment.content_type == DeliveryMechanism.ADOBE_DRM
        assert fulfillment.content_link == "http://fulfillment/"

        data = boundless.files.sample_data("availability_with_ebook_fulfillment.xml")
        delivery_mechanism.delivery_mechanism.content_type = (
            Representation.EPUB_MEDIA_TYPE
        )
        delivery_mechanism.delivery_mechanism.drm_scheme = DeliveryMechanism.ADOBE_DRM
        data = data.replace(b"0016820953", pool.identifier.identifier.encode("utf8"))
        boundless.http_client.queue_response(200, content=data)
        fulfillment = fulfill()
        assert isinstance(fulfillment, BoundlessAcsFulfillment)
        assert (
            fulfillment.content_link
            == "http://adobe.acsm/?src=library&transactionId=2a34598b-12af-41e4-a926-af5e42da7fe5&isbn=9780763654573&format=F2"
        )

    def test_fulfill_baker_taylor_kdrm(self, boundless: BoundlessFixture):
        # Test our ability to fulfill an AxisNow ebook.
        edition, pool = boundless.db.edition(
            identifier_type=Identifier.AXIS_360_ID,
            identifier_id="0016820953",
            data_source_name=DataSource.BOUNDLESS,
            with_license_pool=True,
        )

        patron = boundless.db.patron()
        patron.authorization_identifier = "a barcode"
        lpdm = pool.delivery_mechanisms[0]
        delivery_mechanism = lpdm.delivery_mechanism
        delivery_mechanism.content_type = Representation.EPUB_MEDIA_TYPE
        delivery_mechanism.drm_scheme = DeliveryMechanism.BAKER_TAYLOR_KDRM_DRM

        fulfill = partial(
            boundless.api.fulfill,
            patron,
            "pin",
            licensepool=pool,
            delivery_mechanism=lpdm,
        )

        # If we ask for AxisNow format, we start the Baker & Taylor KDRM fulfillment workflow which
        # makes several api requests, and then returns a DirectFulfillment with the correct
        # content type and content link.

        # This fulfillment requires additional parameters to be passed to the fulfill call. If they
        # are not provided we raise a InvalidInputException.
        boundless.http_client.queue_response(
            200,
            content=boundless.files.sample_data(
                "availability_with_axisnow_fulfillment.xml"
            ),
        )
        boundless.http_client.queue_response(
            200, content=boundless.files.sample_data("ebook_fulfillment_info.json")
        )
        with pytest.raises(
            InvalidInputException, match="Missing required URL parameters"
        ):
            fulfill()

        # Test a successful fulfillment with the required parameters.
        boundless.http_client.queue_response(
            200,
            content=boundless.files.sample_data(
                "availability_with_axisnow_fulfillment.xml"
            ),
        )
        boundless.http_client.queue_response(
            200, content=boundless.files.sample_data("ebook_fulfillment_info.json")
        )
        license_data = boundless.files.sample_data("license.json")
        boundless.http_client.queue_response(200, content=license_data)
        fulfillment = fulfill(
            client_ip="2.2.2.2",
            device_id="device-id",
            modulus="a" * 342,
            exponent="abcd",
        )

        assert isinstance(fulfillment, DirectFulfillment)
        assert fulfillment.content_type == DeliveryMechanism.BAKER_TAYLOR_KDRM_DRM
        assert json.loads(fulfillment.content) == {
            "licenseDocument": json.loads(license_data),
            "links": [
                {
                    "href": "https://frontdoor.axisnow.com/content/download/9780547351551",
                    "rel": "publication",
                    "type": "application/epub+zip",
                }
            ],
        }

    def test_fulfill_findaway(self, boundless: BoundlessFixture):
        # Test our ability to fulfill a Boundless audio title.
        edition, pool = boundless.db.edition(
            identifier_type=Identifier.AXIS_360_ID,
            identifier_id="0015176429",
            data_source_name=DataSource.BOUNDLESS,
            with_license_pool=True,
        )

        patron = boundless.db.patron()
        patron.authorization_identifier = "a barcode"
        delivery_mechanism = pool.delivery_mechanisms[0]

        fulfill = partial(
            boundless.api.fulfill,
            patron,
            "pin",
            licensepool=pool,
            delivery_mechanism=delivery_mechanism,
        )

        # If we ask for a findaway audiobook, we start the findaway fulfillment workflow which
        # makes several api requests, and then returns an DirectFulfillment with the correct
        # content type and content.
        data = boundless.files.sample_data(
            "availability_with_audiobook_fulfillment.xml"
        )
        data = data.replace(b"0012244222", pool.identifier.identifier.encode("utf8"))
        boundless.http_client.queue_response(200, content=data)
        boundless.http_client.queue_response(
            200, content=boundless.files.sample_data("audiobook_fulfillment_info.json")
        )
        boundless.http_client.queue_response(
            200, content=boundless.files.sample_data("audiobook_metadata.json")
        )
        delivery_mechanism.delivery_mechanism.content_type = None
        delivery_mechanism.delivery_mechanism.drm_scheme = (
            DeliveryMechanism.FINDAWAY_DRM
        )
        fulfillment = fulfill()
        assert isinstance(fulfillment, DirectFulfillment)
        assert fulfillment.content_type == DeliveryMechanism.FINDAWAY_DRM
        assert isinstance(fulfillment.content, str)
        # The manifest document combines information from the
        # fulfillment document and the metadata document.
        for required in (
            '"findaway:sessionKey": "0f547af1-38c1-4b1c-8a1a-169d353065d0"',
            '"duration": 8150.87',
        ):
            assert required in fulfillment.content

        # We make the correct series of requests
        [_, _, fulfillment_args, metadata_args] = boundless.http_client.requests_args
        assert fulfillment_args["params"] is not None
        assert fulfillment_args["params"] == {
            "TransactionID": "C3F71F8D-1883-2B34-061F-96570678AEB0"
        }
        assert metadata_args["params"] is not None
        assert metadata_args["params"] == {"fndcontentid": "04960"}

    def test___validate_baker_taylor_kdrm_params(self) -> None:
        correct_modulus = (
            "vOa694N296HWud_b3CCIvIBOuGqwE_mAPcf2MXqGmC1VIolhDIkMsdX38bUBRT-Ui6Q_KacF4MPrD-"
            "vywhEmS1qVYDRV-Ee4wYENCFtgk92j9dPW9TIg1SC3g4oLw7dkvkIrWCzX_r42haAss5NnkKC8FJ7d"
            "14h-f3b-aNPjfowYupZtmmHdq9FEm-g6IjQBYBd569PeC_kXcGBreDspClUQZZMYGW3fYbvg-1vOG0"
            "Pyu0zExgJ9T6omF9gSbW0fojm2SFHhgOcyK271SwOiHUM2KTc7tZEndzoBiaR8t66N84Th7bZxYLWW"
            "hvGS3_YeTYo6OdQj_QBc_vFau5exVw"
        )

        correct_exponent = "ABCD"

        # Test that the validation method works correctly with valid parameters.
        BoundlessApi._validate_baker_taylor_kdrm_params(
            correct_modulus, correct_exponent
        )

        # Test error cases - modulus
        with pytest.raises(InvalidInputException) as excinfo:
            BoundlessApi._validate_baker_taylor_kdrm_params("", correct_exponent)
        assert "modulus" in str(excinfo.value.problem_detail)
        assert "String should have at least 342 characters" in str(
            excinfo.value.problem_detail
        )

        with pytest.raises(InvalidInputException) as excinfo:
            BoundlessApi._validate_baker_taylor_kdrm_params("a" * 343, correct_exponent)
        assert "modulus" in str(excinfo.value.problem_detail)
        assert "String should have at most 342 characters" in str(
            excinfo.value.problem_detail
        )

        with pytest.raises(InvalidInputException) as excinfo:
            BoundlessApi._validate_baker_taylor_kdrm_params("%" * 342, correct_exponent)
        assert "modulus" in str(excinfo.value.problem_detail)
        assert "String should be a url safe base64 encoded string" in str(
            excinfo.value.problem_detail
        )

        # Test error cases - exponent
        with pytest.raises(InvalidInputException) as excinfo:
            BoundlessApi._validate_baker_taylor_kdrm_params(correct_modulus, "ab*c")
        assert "exponent" in str(excinfo.value.problem_detail)
        assert "String should be a url safe base64 encoded string" in str(
            excinfo.value.problem_detail
        )

    def test_patron_activity(self, boundless: BoundlessFixture):
        """Test the method that locates all current activity
        for a patron.
        """
        data = boundless.files.sample_data("availability_with_loan_and_hold.xml")
        boundless.http_client.queue_response(200, content=data)
        patron = boundless.db.patron()
        patron.authorization_identifier = "a barcode"

        [hold, loan, reserved] = list(boundless.api.patron_activity(patron, "pin"))

        # We made a request that included the authorization identifier
        # of the patron in question.
        params = boundless.http_client.requests_args[1]["params"]
        assert params is not None
        assert params["patronId"] == patron.authorization_identifier

        # We got three results -- two holds and one loan.
        assert boundless.api.collection is not None
        assert isinstance(hold, HoldInfo)
        assert hold.collection_id == boundless.api.collection.id
        assert hold.identifier_type == Identifier.AXIS_360_ID
        assert hold.identifier == "0012533119"
        assert hold.hold_position == 1
        assert hold.end_date is None

        assert isinstance(loan, LoanInfo)
        assert loan.collection_id == boundless.api.collection.id
        assert loan.identifier_type == Identifier.AXIS_360_ID
        assert loan.identifier == "0015176429"
        assert loan.end_date == datetime_utc(2015, 8, 12, 17, 40, 27)
        assert loan.locked_to == FormatData(
            content_type=Representation.EPUB_MEDIA_TYPE,
            drm_scheme=DeliveryMechanism.ADOBE_DRM,
        )

        assert isinstance(reserved, HoldInfo)
        assert reserved.collection_id == boundless.api.collection.id
        assert reserved.identifier_type == Identifier.AXIS_360_ID
        assert reserved.identifier == "1111111111"
        assert reserved.end_date == datetime_utc(2015, 1, 1, 13, 11, 11)
        assert reserved.hold_position == 0

        # We are able to get activity information when the feed doesn't contain
        # fulfillment information.
        data = boundless.files.sample_data("availability_without_fulfillment.xml")
        boundless.http_client.queue_response(200, content=data)
        [loan] = list(boundless.api.patron_activity(patron, "pin"))
        assert loan.collection_id == boundless.api.collection.id
        assert loan.identifier_type == Identifier.AXIS_360_ID
        assert loan.identifier == "0015176429"
        assert loan.end_date == datetime_utc(2015, 8, 12, 17, 40, 27)

        # If the activity includes something with a Blio format, it is included in the results, just like AxisNow.
        data = boundless.files.sample_data("availability_with_axisnow_fulfillment.xml")
        boundless.http_client.queue_response(200, content=data)
        [loan] = list(boundless.api.patron_activity(patron, "pin"))
        assert isinstance(loan, LoanInfo)
        assert loan.collection_id == boundless.api.collection.id
        assert loan.identifier_type == Identifier.AXIS_360_ID
        assert loan.identifier == "0016820953"
        assert loan.end_date == datetime_utc(2020, 7, 15, 14, 34)
        assert loan.locked_to == FormatData(
            content_type=Representation.EPUB_MEDIA_TYPE,
            drm_scheme=DeliveryMechanism.BAKER_TAYLOR_KDRM_DRM,
        )

        # If the patron is not found, the parser will return an empty list, since
        # that patron can't have any loans or holds.
        data = boundless.files.sample_data("availability_patron_not_found.xml")
        boundless.http_client.queue_response(200, content=data)
        assert len(list(boundless.api.patron_activity(patron, "pin"))) == 0

    def test_update_licensepools_for_identifiers(self, boundless: BoundlessFixture):
        def _fetch_remote_availability(identifiers):
            for i, identifier in enumerate(identifiers):
                # The first identifer in the list is still
                # available.
                identifier_data = IdentifierData.from_identifier(identifier)
                bibliographic = BibliographicData(
                    data_source_name=DataSource.BOUNDLESS,
                    primary_identifier_data=identifier_data,
                )
                availability = CirculationData(
                    data_source_name=DataSource.BOUNDLESS,
                    primary_identifier_data=identifier_data,
                    licenses_owned=7,
                    licenses_available=6,
                )

                bibliographic.circulation = availability
                yield bibliographic, availability

                # The rest have been 'forgotten' by Boundless.
                break

        api = boundless.api
        api._fetch_remote_availability = MagicMock(
            side_effect=_fetch_remote_availability
        )
        mock_reap = create_autospec(api._reap)
        api._reap = mock_reap
        still_in_collection = boundless.db.identifier(
            identifier_type=Identifier.AXIS_360_ID
        )
        no_longer_in_collection = boundless.db.identifier(
            identifier_type=Identifier.AXIS_360_ID
        )
        api.update_licensepools_for_identifiers(
            [still_in_collection, no_longer_in_collection]
        )

        # The LicensePool for the first identifier was updated.
        [lp] = still_in_collection.licensed_through
        assert lp.licenses_owned == 7
        assert lp.licenses_available == 6

        # The second was reaped.
        mock_reap.assert_called_once_with(no_longer_in_collection)

    def test_fetch_remote_availability(self, boundless: BoundlessFixture):
        # Test the _fetch_remote_availability method, as
        # used by update_licensepools_for_identifiers.

        id1 = boundless.db.identifier(identifier_type=Identifier.AXIS_360_ID)
        id2 = boundless.db.identifier(identifier_type=Identifier.AXIS_360_ID)
        data = boundless.files.sample_data("availability_with_loans.xml")
        # Modify the sample data so that it appears to be talking
        # about one of the books we're going to request.
        data = data.replace(b"0012533119", id1.identifier.encode("utf8"))
        boundless.http_client.queue_response(200, content=data)
        results = [x for x in boundless.api._fetch_remote_availability([id1, id2])]

        # We asked for information on two identifiers.
        assert boundless.http_client.requests_args[1]["params"] == {
            "titleIds": f"{id1.identifier},{id2.identifier}"
        }

        # We got information on only one.
        [(metadata, circulation)] = results
        assert metadata.load_primary_identifier(boundless.db.session) == id1
        assert (
            metadata.title
            == "El caso de la gracia : Un periodista explora las evidencias de unas vidas transformadas"
        )
        assert circulation.licenses_owned == 2

    def test_reap(self, boundless: BoundlessFixture):
        # Test the _reap method, as used by
        # update_licensepools_for_identifiers.

        id1 = boundless.db.identifier(identifier_type=Identifier.AXIS_360_ID)
        assert [] == id1.licensed_through

        # If there is no LicensePool to reap, nothing happens.
        boundless.api._reap(id1)
        assert [] == id1.licensed_through

        # If there is a LicensePool but it has no owned licenses,
        # it's already been reaped, so nothing happens.
        (
            edition,
            pool,
        ) = boundless.db.edition(
            data_source_name=DataSource.BOUNDLESS,
            identifier_type=id1.type,
            identifier_id=id1.identifier,
            with_license_pool=True,
            collection=boundless.collection,
        )

        # This LicensePool has licenses, but it's not in a different
        # collection from the collection associated with this
        # BoundlessApi object, so it's not affected.
        collection2 = boundless.db.collection()
        (
            edition2,
            pool2,
        ) = boundless.db.edition(
            data_source_name=DataSource.BOUNDLESS,
            identifier_type=id1.type,
            identifier_id=id1.identifier,
            with_license_pool=True,
            collection=collection2,
        )

        pool.licenses_owned = 0
        pool2.licenses_owned = 10
        boundless.db.session.commit()
        updated = pool.last_checked
        updated2 = pool2.last_checked
        boundless.api._reap(id1)

        assert updated == pool.last_checked
        assert 0 == pool.licenses_owned
        assert updated2 == pool2.last_checked
        assert 10 == pool2.licenses_owned

        # If the LicensePool did have licenses, then reaping it
        # reflects the fact that the licenses are no longer owned.
        pool.licenses_owned = 10
        pool.licenses_available = 9
        pool.licenses_reserved = 8
        pool.patrons_in_hold_queue = 7
        boundless.api._reap(id1)
        assert 0 == pool.licenses_owned
        assert 0 == pool.licenses_available
        assert 0 == pool.licenses_reserved
        assert 0 == pool.patrons_in_hold_queue

    def test_update_book(self, boundless: BoundlessFixture):
        # Verify that the update_book method takes a BibliographicData object,
        # and creates appropriate data model objects.

        api = boundless.api
        e, e_new, lp, lp_new = api.update_book(
            boundless.BIBLIOGRAPHIC_DATA,
        )
        # A new LicensePool and Edition were created.
        assert True == lp_new
        assert True == e_new

        # The LicensePool reflects what it said in AVAILABILITY_DATA
        assert 9 == lp.licenses_owned

        # There's a Work created for the LicensePool, but presentation readiness
        # now depends on additional metadata to be populated later.
        assert False == lp.work.presentation_ready
        assert e == lp.work.presentation_edition

        # The Edition reflects what it said in BIBLIOGRAPHIC_DATA
        assert "Faith of My Fathers : A Family Memoir" == e.title

        # Now change a bit of the data and call the method again.
        new_circulation = CirculationData(
            data_source_name=DataSource.BOUNDLESS,
            primary_identifier_data=boundless.BIBLIOGRAPHIC_DATA.primary_identifier_data,
            licenses_owned=8,
            licenses_available=7,
        )

        # deepcopy would be preferable here, but I was running into low level errors.
        # A shallow copy should be sufficient here.
        bibliographic = copy.copy(boundless.BIBLIOGRAPHIC_DATA)
        bibliographic.circulation = new_circulation

        e2, e_new, lp2, lp_new = api.update_book(
            bibliographic=bibliographic,
        )

        # The same LicensePool and Edition are returned -- no new ones
        # are created.
        assert e2 == e
        assert False == e_new
        assert lp2 == lp
        assert False == lp_new

        # The LicensePool has been updated to reflect the new
        # CirculationData
        assert 8 == lp.licenses_owned
        assert 7 == lp.licenses_available

    def test_availability_by_title_ids(self, boundless: BoundlessFixture):
        ids = ["my_id"]
        with patch.object(boundless.api.api_requests, "availability") as availability:
            list(boundless.api.availability_by_title_ids(title_ids=ids))

        assert availability.call_args_list[0].kwargs["title_ids"] == ids

    def test__delivery_mechanism_to_internal_format(self) -> None:
        lpdm = LicensePoolDeliveryMechanism(delivery_mechanism=DeliveryMechanism())

        lpdm.delivery_mechanism.content_type = "unknown/content-type"
        lpdm.delivery_mechanism.drm_scheme = "unknown_drm_scheme"

        # If we are called with an unknown delivery mechanism, we raise an exception.
        with pytest.raises(
            DeliveryMechanismError,
            match=r"Could not map delivery mechanism unknown/content-type "
            r"\(unknown_drm_scheme\) to internal delivery mechanism!",
        ):
            BoundlessApi._delivery_mechanism_to_internal_format(lpdm)

        # Otherwise, the internal format is returned.
        lpdm.delivery_mechanism.content_type = Representation.EPUB_MEDIA_TYPE
        lpdm.delivery_mechanism.drm_scheme = DeliveryMechanism.ADOBE_DRM

        assert (
            BoundlessApi._delivery_mechanism_to_internal_format(lpdm)
            == BoundlessFormat.epub
        )

    def test_sort_delivery_mechanisms(
        self, db: DatabaseTransactionFixture, services_fixture: ServicesFixture
    ) -> None:
        def get_mechanisms(
            items: list[LicensePoolDeliveryMechanism],
        ) -> list[tuple[str | None, str | None]]:
            return [
                (dm.delivery_mechanism.content_type, dm.delivery_mechanism.drm_scheme)
                for dm in items
            ]

        edition = db.edition()
        pool = db.licensepool(edition)
        pool.set_delivery_mechanism(
            MediaTypes.EPUB_MEDIA_TYPE,
            DeliveryMechanism.BAKER_TAYLOR_KDRM_DRM,
            None,
        )
        annotator = CirculationManagerAnnotator(None)

        # Without the prioritize_boundless_drm setting, Adobe DRM is first.
        collection = db.collection(
            protocol=BoundlessApi,
            settings=db.boundless_settings(
                prioritize_boundless_drm=False,
            ),
        )
        pool.collection = collection
        assert get_mechanisms(annotator.visible_delivery_mechanisms(pool)) == [
            (MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
            (MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.BAKER_TAYLOR_KDRM_DRM),
        ]

        # With the prioritize_boundless_drm setting, Boundless DRM is first.
        collection = db.collection(
            protocol=BoundlessApi,
            settings=db.boundless_settings(
                prioritize_boundless_drm=True,
            ),
        )
        pool.collection = collection
        assert get_mechanisms(annotator.visible_delivery_mechanisms(pool)) == [
            (MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.BAKER_TAYLOR_KDRM_DRM),
            (MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
        ]

    def test_import_task(self) -> None:
        collection_id = MagicMock()
        force = MagicMock()
        with patch.object(boundless_tasks, "import_collection") as mock_import:
            result = BoundlessApi.import_task(collection_id, force)

        mock_import.s.assert_called_once_with(collection_id, import_all=force)
        assert result == mock_import.s.return_value
