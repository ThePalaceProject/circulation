from __future__ import annotations

import copy
import datetime
import json
from functools import partial
from unittest.mock import MagicMock, create_autospec, patch

import pytest
from freezegun import freeze_time

from palace.manager.api.axis.api import Axis360API
from palace.manager.api.axis.exception import Axis360ValidationError
from palace.manager.api.axis.fulfillment import (
    Axis360AcsFulfillment,
)
from palace.manager.api.circulation import DirectFulfillment, HoldInfo, LoanInfo
from palace.manager.api.circulation_exceptions import (
    CannotFulfill,
    FormatNotAvailable,
    InvalidInputException,
    NoActiveLoan,
    NotFoundOnRemote,
    RemoteInitiatedServerError,
)
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.format import FormatData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism
from palace.manager.sqlalchemy.model.resource import Representation
from palace.manager.util.datetime_helpers import datetime_utc, utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.library import LibraryFixture
from tests.manager.api.axis.conftest import Axis360Fixture


class TestAxis360API:
    def test__run_self_tests(
        self,
        db: DatabaseTransactionFixture,
        axis360: Axis360Fixture,
    ):
        # Verify that Axis360API._run_self_tests() calls the right
        # methods.
        api = axis360.api

        mock_refresh_bearer_token = create_autospec(
            api.api_requests.refresh_bearer_token,
            return_value=MagicMock(access_token="the new token"),
        )
        api.api_requests.refresh_bearer_token = mock_refresh_bearer_token
        mock_recent_activity = create_autospec(
            api.recent_activity, return_value=[(1, "a"), (2, "b"), (3, "c")]
        )
        api.recent_activity = mock_recent_activity
        mock_patron_activity = create_autospec(
            api.patron_activity, return_value=["loan", "hold"]
        )
        api.patron_activity = mock_patron_activity

        # Now let's make sure two Libraries have access to this
        # Collection -- one library with a default patron and one
        # without.
        no_default_patron = db.library()
        axis360.collection.associated_libraries.append(no_default_patron)

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
        mock_recent_activity.assert_called_once_with(
            time_run - datetime.timedelta(minutes=5)
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

    def test__run_self_tests_short_circuit(self, axis360: Axis360Fixture):
        # If we can't refresh the bearer token, the rest of the
        # self-tests aren't even run.

        api = axis360.api
        api.api_requests.refresh_bearer_token = MagicMock(
            side_effect=Exception("no way")
        )

        # Now that everything is set up, run the self-test. Only one
        # test will be run.
        [failure] = api._run_self_tests(axis360.db.session)
        assert failure.name == "Refreshing bearer token"
        assert failure.success is False
        assert failure.exception is not None
        assert failure.exception.args[0] == "no way"

    def test_create_identifier_strings(self, axis360: Axis360Fixture):
        identifier = axis360.db.identifier()
        values = Axis360API.create_identifier_strings(["foo", identifier])
        assert ["foo", identifier.identifier] == values

    def test_update_availability(self, axis360: Axis360Fixture):
        # Test the Axis 360 implementation of the update_availability method
        # defined by the CirculationAPI interface.

        # Create a LicensePool that needs updating.
        edition, pool = axis360.db.edition(
            identifier_type=Identifier.AXIS_360_ID,
            data_source_name=DataSource.AXIS_360,
            with_license_pool=True,
            collection=axis360.collection,
        )

        # We have never checked the circulation information for this
        # LicensePool. Put some random junk in the pool to verify
        # that it gets changed.
        pool.licenses_owned = 10
        pool.licenses_available = 5
        pool.patrons_in_hold_queue = 3
        assert None == pool.last_checked

        # Prepare availability information.
        data = axis360.files.sample_data("availability_with_loans.xml")

        # Modify the data so that it appears to be talking about the
        # book we just created.
        new_identifier = pool.identifier.identifier
        data = data.replace(b"0012533119", new_identifier.encode("utf8"))

        axis360.http_client.queue_response(200, content=data)

        axis360.api.update_availability(pool)

        # The availability information has been udpated, as has the
        # date the availability information was last checked.
        assert 2 == pool.licenses_owned
        assert 1 == pool.licenses_available
        assert 0 == pool.patrons_in_hold_queue
        assert pool.last_checked is not None

    def test_checkin_success(self, axis360: Axis360Fixture):
        # Verify that we can make a request to the EarlyCheckInTitle
        # endpoint and get a good response.
        edition, pool = axis360.db.edition(
            identifier_type=Identifier.AXIS_360_ID,
            data_source_name=DataSource.AXIS_360,
            with_license_pool=True,
        )
        data = axis360.files.sample_data("checkin_success.xml")
        axis360.http_client.queue_response(200, content=data)
        patron = axis360.db.patron()
        barcode = axis360.db.fresh_str()
        patron.authorization_identifier = barcode
        axis360.api.checkin(patron, "pin", pool)

        # Verify the format of the HTTP request that was made.
        assert axis360.http_client.requests_methods[1] == "GET"
        assert "/EarlyCheckInTitle/v3" in axis360.http_client.requests[1]
        assert axis360.http_client.requests_args[1]["params"] == {
            "itemID": pool.identifier.identifier,
            "patronID": barcode,
        }

    def test_checkin_failure(self, axis360: Axis360Fixture):
        # Verify that we correctly handle failure conditions sent from
        # the EarlyCheckInTitle endpoint.
        edition, pool = axis360.db.edition(
            identifier_type=Identifier.AXIS_360_ID,
            data_source_name=DataSource.AXIS_360,
            with_license_pool=True,
        )
        data = axis360.files.sample_data("checkin_failure.xml")
        axis360.http_client.queue_response(200, content=data)
        patron = axis360.db.patron()
        patron.authorization_identifier = axis360.db.fresh_str()
        with pytest.raises(NotFoundOnRemote):
            axis360.api.checkin(patron, "pin", pool)

    def test_place_hold(self, axis360: Axis360Fixture, library_fixture: LibraryFixture):
        edition, pool = axis360.db.edition(
            identifier_type=Identifier.AXIS_360_ID,
            data_source_name=DataSource.AXIS_360,
            with_license_pool=True,
        )
        data = axis360.files.sample_data("place_hold_success.xml")
        axis360.http_client.queue_response(200, content=data)
        library = library_fixture.library()
        library_settings = library_fixture.settings(library)
        patron = axis360.db.patron(library=library)
        library_settings.default_notification_email_address = (
            "notifications@example.com"
        )

        response = axis360.api.place_hold(patron, "pin", pool, None)
        assert response.hold_position == 1
        assert response.identifier_type == pool.identifier.type
        assert response.identifier == pool.identifier.identifier
        params = axis360.http_client.requests_args[1]["params"]
        assert params is not None
        assert params["email"] == "notifications@example.com"

    def test_release_hold(self, axis360: Axis360Fixture):
        edition, pool = axis360.db.edition(
            identifier_type=Identifier.AXIS_360_ID,
            data_source_name=DataSource.AXIS_360,
            with_license_pool=True,
        )
        data = axis360.files.sample_data("release_hold_success.xml")
        axis360.http_client.queue_response(200, content=data)
        patron = axis360.db.patron()
        barcode = axis360.db.fresh_str()
        patron.authorization_identifier = barcode

        axis360.api.release_hold(patron, "pin", pool)

        # Verify the format of the HTTP request that was made.
        assert axis360.http_client.requests_methods[1] == "GET"
        assert "/removeHold/v2" in axis360.http_client.requests[1]
        assert axis360.http_client.requests_args[1]["params"] == {
            "titleId": pool.identifier.identifier,
            "patronId": barcode,
        }

    def test_checkout(self, axis360: Axis360Fixture):
        # Verify that we can make a request to the CheckoutTitle
        # endpoint and get a good response.
        edition, pool = axis360.db.edition(
            identifier_type=Identifier.AXIS_360_ID,
            data_source_name=DataSource.AXIS_360,
            with_license_pool=True,
        )
        data = axis360.files.sample_data("checkout_success.xml")
        axis360.http_client.queue_response(200, content=data)
        patron = axis360.db.patron()
        barcode = axis360.db.fresh_str()
        patron.authorization_identifier = barcode
        delivery_mechanism = pool.delivery_mechanisms[0]

        axis360.api.checkout(patron, "pin", pool, delivery_mechanism)

        # Verify the format of the HTTP request that was made.
        assert axis360.http_client.requests_methods[1] == "POST"
        assert "/checkout/v2" in axis360.http_client.requests[1]
        assert axis360.http_client.requests_args[1]["params"] == {
            "titleId": pool.identifier.identifier,
            "patronId": barcode,
            "format": "ePub",
        }

    def test_fulfill_errors(self, axis360: Axis360Fixture):
        # Test our ability to fulfill an Axis 360 title.
        edition, pool = axis360.db.edition(
            identifier_type=Identifier.AXIS_360_ID,
            data_source_name=DataSource.AXIS_360,
            with_license_pool=True,
        )

        patron = axis360.db.patron()
        patron.authorization_identifier = "a barcode"
        delivery_mechanism = pool.delivery_mechanisms[0]

        fulfill = partial(
            axis360.api.fulfill,
            patron,
            "pin",
            licensepool=pool,
            delivery_mechanism=delivery_mechanism,
        )

        # If Axis 360 says a patron does not have a title checked out,
        # an attempt to fulfill that title will fail with NoActiveLoan.
        data = axis360.files.sample_data("availability_with_ebook_fulfillment.xml")
        axis360.http_client.queue_response(200, content=data)
        with pytest.raises(NoActiveLoan):
            fulfill()

        # If the title is checked out but Axis provides no download link,
        # the exception is CannotFulfill.
        pool.identifier.identifier = "0015176429"
        data = axis360.files.sample_data("availability_without_fulfillment.xml")
        axis360.http_client.queue_response(200, content=data)
        with pytest.raises(CannotFulfill):
            fulfill()

        # If axis shows the title as checked out, but in a format that we did
        # not request, we get a FormatNotAvailable exception.
        delivery_mechanism.delivery_mechanism.content_type = (
            Representation.EPUB_MEDIA_TYPE
        )
        delivery_mechanism.delivery_mechanism.drm_scheme = (
            DeliveryMechanism.BOUNDLESS_DRM
        )
        pool.identifier.identifier = "0016820953"
        data = axis360.files.sample_data("availability_with_ebook_fulfillment.xml")
        axis360.http_client.queue_response(200, content=data)
        with pytest.raises(FormatNotAvailable):
            fulfill()

        # Test errors with additional API calls made during fulfillment.
        pool.identifier.identifier = "0012244222"
        delivery_mechanism.delivery_mechanism.content_type = None
        delivery_mechanism.delivery_mechanism.drm_scheme = (
            DeliveryMechanism.FINDAWAY_DRM
        )
        data_dict = json.loads(
            axis360.files.sample_data("audiobook_fulfillment_info.json")
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
            axis360.http_client.queue_response(
                200,
                content=(
                    axis360.files.sample_data(
                        "availability_with_audiobook_fulfillment.xml"
                    )
                ),
            )
            axis360.http_client.queue_response(200, content=json.dumps(missing_field))
            with pytest.raises(Axis360ValidationError):
                fulfill()

        # Try with a bad expiration date.
        bad_date = data_dict.copy()
        bad_date["ExpirationDate"] = "not-a-date"
        axis360.http_client.queue_response(
            200,
            content=(
                axis360.files.sample_data("availability_with_audiobook_fulfillment.xml")
            ),
        )
        axis360.http_client.queue_response(200, content=json.dumps(bad_date))
        with pytest.raises(Axis360ValidationError):
            fulfill()

        # Try with an expired session key.
        expired_session_key = data_dict.copy()
        expired_session_key["FNDSessionKey"] = "Expired"
        axis360.http_client.queue_response(
            200,
            content=(
                axis360.files.sample_data("availability_with_audiobook_fulfillment.xml")
            ),
        )
        axis360.http_client.queue_response(200, content=json.dumps(expired_session_key))
        with pytest.raises(
            RemoteInitiatedServerError, match="Expired findaway session key"
        ):
            fulfill()

    def test_fulfill_acs(self, axis360: Axis360Fixture):
        # Test our ability to fulfill an Axis 360 title.
        edition, pool = axis360.db.edition(
            identifier_type=Identifier.AXIS_360_ID,
            identifier_id="0015176429",
            data_source_name=DataSource.AXIS_360,
            with_license_pool=True,
        )

        patron = axis360.db.patron()
        patron.authorization_identifier = "a barcode"
        delivery_mechanism = pool.delivery_mechanisms[0]

        fulfill = partial(
            axis360.api.fulfill,
            patron,
            "pin",
            licensepool=pool,
            delivery_mechanism=delivery_mechanism,
        )

        # If an ebook is checked out and we're asking for it to be
        # fulfilled through Adobe DRM, we get a Axis360AcsFulfillment
        # object with a content link.
        data = axis360.files.sample_data("availability_with_loan_and_hold.xml")
        axis360.http_client.queue_response(200, content=data)
        fulfillment = fulfill()
        assert isinstance(fulfillment, Axis360AcsFulfillment)
        assert fulfillment.content_type == DeliveryMechanism.ADOBE_DRM
        assert fulfillment.content_link == "http://fulfillment/"

        data = axis360.files.sample_data("availability_with_ebook_fulfillment.xml")
        delivery_mechanism.delivery_mechanism.content_type = (
            Representation.EPUB_MEDIA_TYPE
        )
        delivery_mechanism.delivery_mechanism.drm_scheme = DeliveryMechanism.ADOBE_DRM
        data = data.replace(b"0016820953", pool.identifier.identifier.encode("utf8"))
        axis360.http_client.queue_response(200, content=data)
        fulfillment = fulfill()
        assert isinstance(fulfillment, Axis360AcsFulfillment)
        assert (
            fulfillment.content_link
            == "http://adobe.acsm/?src=library&transactionId=2a34598b-12af-41e4-a926-af5e42da7fe5&isbn=9780763654573&format=F2"
        )

    def test_fulfill_boundless_drm(self, axis360: Axis360Fixture):
        # Test our ability to fulfill an Axis 360 title.
        edition, pool = axis360.db.edition(
            identifier_type=Identifier.AXIS_360_ID,
            identifier_id="0016820953",
            data_source_name=DataSource.AXIS_360,
            with_license_pool=True,
        )

        patron = axis360.db.patron()
        patron.authorization_identifier = "a barcode"
        delivery_mechanism = pool.delivery_mechanisms[0]
        delivery_mechanism.delivery_mechanism.content_type = (
            Representation.EPUB_MEDIA_TYPE
        )
        delivery_mechanism.delivery_mechanism.drm_scheme = (
            DeliveryMechanism.BOUNDLESS_DRM
        )

        fulfill = partial(
            axis360.api.fulfill,
            patron,
            "pin",
            licensepool=pool,
            delivery_mechanism=delivery_mechanism,
        )

        # If we ask for AxisNow format, we start the axisnow fulfillment workflow which
        # makes several api requests, and then returns a DirectFulfillment with the correct
        # content type and content link.

        # The Boundless DRM fulfillment requires additional parameters to be passed to the fulfill call. If they
        # are not provided we raise a InvalidInputException.
        axis360.http_client.queue_response(
            200,
            content=axis360.files.sample_data(
                "availability_with_axisnow_fulfillment.xml"
            ),
        )
        axis360.http_client.queue_response(
            200, content=axis360.files.sample_data("ebook_fulfillment_info.json")
        )
        with pytest.raises(
            InvalidInputException, match="Missing required URL parameters"
        ):
            fulfill()

        # Test a successful fulfillment with the required parameters.
        axis360.http_client.queue_response(
            200,
            content=axis360.files.sample_data(
                "availability_with_axisnow_fulfillment.xml"
            ),
        )
        axis360.http_client.queue_response(
            200, content=axis360.files.sample_data("ebook_fulfillment_info.json")
        )
        license_data = axis360.files.sample_data("license.json")
        axis360.http_client.queue_response(200, content=license_data)
        fulfillment = fulfill(
            client_ip="2.2.2.2",
            device_id="device-id",
            modulus="modulus",
            exponent="exponent",
        )

        assert isinstance(fulfillment, DirectFulfillment)
        assert fulfillment.content_type == DeliveryMechanism.BOUNDLESS_DRM
        assert fulfillment.content == license_data

    def test_fulfill_findaway(self, axis360: Axis360Fixture):
        # Test our ability to fulfill an Axis 360 title.
        edition, pool = axis360.db.edition(
            identifier_type=Identifier.AXIS_360_ID,
            identifier_id="0015176429",
            data_source_name=DataSource.AXIS_360,
            with_license_pool=True,
        )

        patron = axis360.db.patron()
        patron.authorization_identifier = "a barcode"
        delivery_mechanism = pool.delivery_mechanisms[0]

        fulfill = partial(
            axis360.api.fulfill,
            patron,
            "pin",
            licensepool=pool,
            delivery_mechanism=delivery_mechanism,
        )

        # If we ask for a findaway audiobook, we start the findaway fulfillment workflow which
        # makes several api requests, and then returns an DirectFulfillment with the correct
        # content type and content.
        data = axis360.files.sample_data("availability_with_audiobook_fulfillment.xml")
        data = data.replace(b"0012244222", pool.identifier.identifier.encode("utf8"))
        axis360.http_client.queue_response(200, content=data)
        axis360.http_client.queue_response(
            200, content=axis360.files.sample_data("audiobook_fulfillment_info.json")
        )
        axis360.http_client.queue_response(
            200, content=axis360.files.sample_data("audiobook_metadata.json")
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
        [_, _, fulfillment_args, metadata_args] = axis360.http_client.requests_args
        assert fulfillment_args["params"] is not None
        assert fulfillment_args["params"] == {
            "TransactionID": "C3F71F8D-1883-2B34-061F-96570678AEB0"
        }
        assert metadata_args["params"] is not None
        assert metadata_args["params"] == {"fndcontentid": "04960"}

    def test_patron_activity(self, axis360: Axis360Fixture):
        """Test the method that locates all current activity
        for a patron.
        """
        data = axis360.files.sample_data("availability_with_loan_and_hold.xml")
        axis360.http_client.queue_response(200, content=data)
        patron = axis360.db.patron()
        patron.authorization_identifier = "a barcode"

        [hold, loan, reserved] = list(axis360.api.patron_activity(patron, "pin"))

        # We made a request that included the authorization identifier
        # of the patron in question.
        params = axis360.http_client.requests_args[1]["params"]
        assert params is not None
        assert params["patronId"] == patron.authorization_identifier

        # We got three results -- two holds and one loan.
        assert axis360.api.collection is not None
        assert isinstance(hold, HoldInfo)
        assert hold.collection_id == axis360.api.collection.id
        assert hold.identifier_type == Identifier.AXIS_360_ID
        assert hold.identifier == "0012533119"
        assert hold.hold_position == 1
        assert hold.end_date is None

        assert isinstance(loan, LoanInfo)
        assert loan.collection_id == axis360.api.collection.id
        assert loan.identifier_type == Identifier.AXIS_360_ID
        assert loan.identifier == "0015176429"
        assert loan.end_date == datetime_utc(2015, 8, 12, 17, 40, 27)
        assert loan.locked_to == FormatData(
            content_type=Representation.EPUB_MEDIA_TYPE,
            drm_scheme=DeliveryMechanism.ADOBE_DRM,
        )

        assert isinstance(reserved, HoldInfo)
        assert reserved.collection_id == axis360.api.collection.id
        assert reserved.identifier_type == Identifier.AXIS_360_ID
        assert reserved.identifier == "1111111111"
        assert reserved.end_date == datetime_utc(2015, 1, 1, 13, 11, 11)
        assert reserved.hold_position == 0

        # We are able to get activity information when the feed doesn't contain
        # fulfillment information.
        data = axis360.files.sample_data("availability_without_fulfillment.xml")
        axis360.http_client.queue_response(200, content=data)
        [loan] = list(axis360.api.patron_activity(patron, "pin"))
        assert loan.collection_id == axis360.api.collection.id
        assert loan.identifier_type == Identifier.AXIS_360_ID
        assert loan.identifier == "0015176429"
        assert loan.end_date == datetime_utc(2015, 8, 12, 17, 40, 27)

        # If the activity includes something with a Blio format, it is not included in the results.
        data = axis360.files.sample_data("availability_with_axisnow_fulfillment.xml")
        axis360.http_client.queue_response(200, content=data)
        assert len(list(axis360.api.patron_activity(patron, "pin"))) == 0

        # If the patron is not found, the parser will return an empty list, since
        # that patron can't have any loans or holds.
        data = axis360.files.sample_data("availability_patron_not_found.xml")
        axis360.http_client.queue_response(200, content=data)
        assert len(list(axis360.api.patron_activity(patron, "pin"))) == 0

    def test_update_licensepools_for_identifiers(self, axis360: Axis360Fixture):
        def _fetch_remote_availability(identifiers):
            for i, identifier in enumerate(identifiers):
                # The first identifer in the list is still
                # available.
                identifier_data = IdentifierData.from_identifier(identifier)
                bibliographic = BibliographicData(
                    data_source_name=DataSource.AXIS_360,
                    primary_identifier_data=identifier_data,
                )
                availability = CirculationData(
                    data_source_name=DataSource.AXIS_360,
                    primary_identifier_data=identifier_data,
                    licenses_owned=7,
                    licenses_available=6,
                )

                bibliographic.circulation = availability
                yield bibliographic, availability

                # The rest have been 'forgotten' by Axis 360.
                break

        api = axis360.api
        api._fetch_remote_availability = MagicMock(
            side_effect=_fetch_remote_availability
        )
        mock_reap = create_autospec(api._reap)
        api._reap = mock_reap
        still_in_collection = axis360.db.identifier(
            identifier_type=Identifier.AXIS_360_ID
        )
        no_longer_in_collection = axis360.db.identifier(
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

    def test_fetch_remote_availability(self, axis360: Axis360Fixture):
        # Test the _fetch_remote_availability method, as
        # used by update_licensepools_for_identifiers.

        id1 = axis360.db.identifier(identifier_type=Identifier.AXIS_360_ID)
        id2 = axis360.db.identifier(identifier_type=Identifier.AXIS_360_ID)
        data = axis360.files.sample_data("availability_with_loans.xml")
        # Modify the sample data so that it appears to be talking
        # about one of the books we're going to request.
        data = data.replace(b"0012533119", id1.identifier.encode("utf8"))
        axis360.http_client.queue_response(200, content=data)
        results = [x for x in axis360.api._fetch_remote_availability([id1, id2])]

        # We asked for information on two identifiers.
        assert axis360.http_client.requests_args[1]["params"] == {
            "titleIds": f"{id1.identifier},{id2.identifier}"
        }

        # We got information on only one.
        [(metadata, circulation)] = results
        assert metadata.load_primary_identifier(axis360.db.session) == id1
        assert (
            metadata.title
            == "El caso de la gracia : Un periodista explora las evidencias de unas vidas transformadas"
        )
        assert circulation.licenses_owned == 2

    def test_reap(self, axis360: Axis360Fixture):
        # Test the _reap method, as used by
        # update_licensepools_for_identifiers.

        id1 = axis360.db.identifier(identifier_type=Identifier.AXIS_360_ID)
        assert [] == id1.licensed_through

        # If there is no LicensePool to reap, nothing happens.
        axis360.api._reap(id1)
        assert [] == id1.licensed_through

        # If there is a LicensePool but it has no owned licenses,
        # it's already been reaped, so nothing happens.
        (
            edition,
            pool,
        ) = axis360.db.edition(
            data_source_name=DataSource.AXIS_360,
            identifier_type=id1.type,
            identifier_id=id1.identifier,
            with_license_pool=True,
            collection=axis360.collection,
        )

        # This LicensePool has licenses, but it's not in a different
        # collection from the collection associated with this
        # Axis360API object, so it's not affected.
        collection2 = axis360.db.collection()
        (
            edition2,
            pool2,
        ) = axis360.db.edition(
            data_source_name=DataSource.AXIS_360,
            identifier_type=id1.type,
            identifier_id=id1.identifier,
            with_license_pool=True,
            collection=collection2,
        )

        pool.licenses_owned = 0
        pool2.licenses_owned = 10
        axis360.db.session.commit()
        updated = pool.last_checked
        updated2 = pool2.last_checked
        axis360.api._reap(id1)

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
        axis360.api._reap(id1)
        assert 0 == pool.licenses_owned
        assert 0 == pool.licenses_available
        assert 0 == pool.licenses_reserved
        assert 0 == pool.patrons_in_hold_queue

    def test_update_book(self, axis360: Axis360Fixture):
        # Verify that the update_book method takes a BibliographicData object,
        # and creates appropriate data model objects.

        api = axis360.api
        e, e_new, lp, lp_new = api.update_book(
            axis360.BIBLIOGRAPHIC_DATA,
        )
        # A new LicensePool and Edition were created.
        assert True == lp_new
        assert True == e_new

        # The LicensePool reflects what it said in AVAILABILITY_DATA
        assert 9 == lp.licenses_owned

        # There's a presentation-ready Work created for the
        # LicensePool.
        assert True == lp.work.presentation_ready
        assert e == lp.work.presentation_edition

        # The Edition reflects what it said in BIBLIOGRAPHIC_DATA
        assert "Faith of My Fathers : A Family Memoir" == e.title

        # Now change a bit of the data and call the method again.
        new_circulation = CirculationData(
            data_source_name=DataSource.AXIS_360,
            primary_identifier_data=axis360.BIBLIOGRAPHIC_DATA.primary_identifier_data,
            licenses_owned=8,
            licenses_available=7,
        )

        # deepcopy would be preferable here, but I was running into low level errors.
        # A shallow copy should be sufficient here.
        bibliographic = copy.copy(axis360.BIBLIOGRAPHIC_DATA)
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

    def test_availability_by_title_ids(self, axis360: Axis360Fixture):
        ids = ["my_id"]
        with patch.object(axis360.api.api_requests, "availability") as availability:
            list(axis360.api.availability_by_title_ids(title_ids=ids))

        assert availability.call_args_list[0].kwargs["title_ids"] == ids

    def test_recent_activity(self, axis360: Axis360Fixture):
        # Test the recent_activity method, which returns a list of
        # recent activity for the collection.
        api = axis360.api
        data = axis360.files.sample_data("tiny_collection.xml")
        axis360.http_client.queue_response(200, content=data)

        # Get the activity for the last 5 minutes.
        since = datetime_utc(2012, 10, 1, 15, 45, 25, 4456)
        activity = list(api.recent_activity(since))

        # We made a request to the correct URL.
        assert "/availability/v2" in axis360.http_client.requests[1]
        assert axis360.http_client.requests_args[1]["params"] == {
            "updatedDate": "10-01-2012 15:45:25",
        }

        assert len(activity) == 2
