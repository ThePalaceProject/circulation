from __future__ import annotations

import copy
import datetime
import json
from functools import partial
from unittest.mock import patch

import pytest

from palace.manager.api.axis.api import Axis360API
from palace.manager.api.axis.constants import Axis360APIConstants
from palace.manager.api.axis.fulfillment import (
    Axis360AcsFulfillment,
    Axis360Fulfillment,
)
from palace.manager.api.axis.settings import Axis360Settings
from palace.manager.api.circulation import HoldInfo, LoanInfo
from palace.manager.api.circulation_exceptions import (
    CannotFulfill,
    NoActiveLoan,
    NotFoundOnRemote,
)
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.integration.base import integration_settings_update
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.http import RemoteIntegrationException
from palace.manager.util.problem_detail import ProblemDetailException
from tests.fixtures.library import LibraryFixture
from tests.manager.api.axis.conftest import Axis360Fixture
from tests.mocks.axis import MockAxis360API


class TestAxis360API:
    def test__run_self_tests(
        self,
        axis360: Axis360Fixture,
    ):
        # Verify that Axis360API._run_self_tests() calls the right
        # methods.

        class Mock(MockAxis360API):
            "Mock every method used by Axis360API._run_self_tests."

            # First we will refresh the bearer token.
            def _refresh_bearer_token(self):
                return "the new token"

            # Then we will count the number of events in the past
            # give minutes.
            def recent_activity(self, since):
                self.recent_activity_called_with = since
                return [(1, "a"), (2, "b"), (3, "c")]

            # Then we will count the loans and holds for the default
            # patron.
            def patron_activity(self, patron, pin):
                self.patron_activity_called_with = (patron, pin)
                return ["loan", "hold"]

        # Now let's make sure two Libraries have access to this
        # Collection -- one library with a default patron and one
        # without.
        no_default_patron = axis360.db.library()
        axis360.collection.associated_libraries.append(no_default_patron)

        with_default_patron = axis360.db.default_library()
        axis360.db.simple_auth_integration(with_default_patron)

        # Now that everything is set up, run the self-test.
        api = Mock(axis360.db.session, axis360.collection)
        now = utc_now()
        [
            no_patron_credential,
            recent_circulation_events,
            patron_activity,
            pools_without_delivery,
            refresh_bearer_token,
        ] = sorted(api._run_self_tests(axis360.db.session), key=lambda x: str(x.name))
        assert "Refreshing bearer token" == refresh_bearer_token.name
        assert True == refresh_bearer_token.success
        assert "the new token" == refresh_bearer_token.result

        assert (
            "Acquiring test patron credentials for library %s" % no_default_patron.name
            == no_patron_credential.name
        )
        assert False == no_patron_credential.success
        assert "Library has no test patron configured." == str(
            no_patron_credential.exception
        )

        assert (
            "Asking for circulation events for the last five minutes"
            == recent_circulation_events.name
        )
        assert True == recent_circulation_events.success
        assert "Found 3 event(s)" == recent_circulation_events.result
        since = api.recent_activity_called_with
        five_minutes_ago = utc_now() - datetime.timedelta(minutes=5)
        assert (five_minutes_ago - since).total_seconds() < 5

        assert (
            "Checking activity for test patron for library %s"
            % with_default_patron.name
            == patron_activity.name
        )
        assert True == patron_activity.success
        assert "Found 2 loans/holds" == patron_activity.result
        patron, pin = api.patron_activity_called_with
        assert "username1" == patron.authorization_identifier
        assert "password1" == pin

        assert (
            "Checking for titles that have no delivery mechanisms."
            == pools_without_delivery.name
        )
        assert True == pools_without_delivery.success
        assert (
            "All titles in this collection have delivery mechanisms."
            == pools_without_delivery.result
        )

    def test__run_self_tests_short_circuit(self, axis360: Axis360Fixture):
        # If we can't refresh the bearer token, the rest of the
        # self-tests aren't even run.

        class Mock(MockAxis360API):
            def _refresh_bearer_token(self):
                raise Exception("no way")

        # Now that everything is set up, run the self-test. Only one
        # test will be run.
        api = Mock(axis360.db.session, axis360.collection)
        [failure] = api._run_self_tests(axis360.db.session)
        assert "Refreshing bearer token" == failure.name
        assert failure.success is False
        assert failure.exception is not None
        assert "no way" == failure.exception.args[0]

    def test_create_identifier_strings(self, axis360: Axis360Fixture):
        identifier = axis360.db.identifier()
        values = Axis360API.create_identifier_strings(["foo", identifier])
        assert ["foo", identifier.identifier] == values

    def test_availability_no_timeout(self, axis360: Axis360Fixture):
        # The availability API request has no timeout set, because it
        # may take time proportinate to the total size of the
        # collection.
        axis360.api.queue_response(200)
        axis360.api.availability()
        request = axis360.api.requests.pop()
        kwargs = request[-1]
        assert None == kwargs["timeout"]

    def test_availability_exception(self, axis360: Axis360Fixture):
        axis360.api.queue_response(500)

        with pytest.raises(RemoteIntegrationException) as excinfo:
            axis360.api.availability()
        assert (
            "Bad response from http://axis.test/availability/v2: Got status code 500 from external server, cannot continue."
            in str(excinfo.value)
        )

    def test_refresh_bearer_token_after_401(self, axis360: Axis360Fixture):
        # If we get a 401, we will fetch a new bearer token and try the
        # request again.

        axis360.api.queue_response(401)
        axis360.api.queue_response(200, content=json.dumps(dict(access_token="foo")))
        axis360.api.queue_response(200, content="The data")
        response = axis360.api.request("http://url/")
        assert b"The data" == response.content

    def test_refresh_bearer_token_error(self, axis360: Axis360Fixture):
        # Raise an exception if we don't get a 200 status code when
        # refreshing the bearer token.

        api = MockAxis360API(axis360.db.session, axis360.collection, with_token=False)
        api.queue_response(412)
        with pytest.raises(RemoteIntegrationException) as excinfo:
            api._refresh_bearer_token()
        assert (
            "Bad response from http://axis.test/accesstoken: Got status code 412 from external server, but can only continue on: 200."
            in str(excinfo.value)
        )

    def test_bearer_token_only_refreshed_once_after_401(self, axis360: Axis360Fixture):
        # If we get a 401 immediately after refreshing the token, we just
        # return the response instead of refreshing the token again.

        axis360.api.queue_response(401)
        axis360.api.queue_response(200, content=json.dumps(dict(access_token="foo")))
        axis360.api.queue_response(401)

        axis360.api.queue_response(301)

        response = axis360.api.request("http://url/")
        assert response.status_code == 401

        # The fourth request never got made.
        assert [301] == [x.status_code for x in axis360.api.responses]

    @pytest.mark.parametrize(
        "file, should_refresh",
        [
            pytest.param(None, True, id="no_message"),
            pytest.param("availability_invalid_token.xml", True, id="invalid_token"),
            pytest.param("availability_expired_token.xml", True, id="expired_token"),
            pytest.param(
                "availability_patron_not_found.xml", False, id="patron_not_found"
            ),
        ],
    )
    def test_refresh_bearer_token_based_on_token_status(
        self, axis360: Axis360Fixture, file: str | None, should_refresh: bool
    ):
        data = axis360.sample_data(file) if file else None

        axis360.api.queue_response(401, content=data)
        axis360.api.queue_response(200, content=json.dumps(dict(access_token="foo")))
        axis360.api.queue_response(200, content="The data")
        response = axis360.api.request("http://url/")

        if should_refresh:
            assert response.content == b"The data"
            assert response.status_code == 200
            assert len(axis360.api.requests) == 3
        else:
            assert response.content == data
            assert response.status_code == 401
            assert len(axis360.api.requests) == 1

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
        data = axis360.sample_data("availability_with_loans.xml")

        # Modify the data so that it appears to be talking about the
        # book we just created.
        new_identifier = pool.identifier.identifier
        data = data.replace(b"0012533119", new_identifier.encode("utf8"))

        axis360.api.queue_response(200, content=data)

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
        data = axis360.sample_data("checkin_success.xml")
        axis360.api.queue_response(200, content=data)
        patron = axis360.db.patron()
        barcode = axis360.db.fresh_str()
        patron.authorization_identifier = barcode
        axis360.api.checkin(patron, "pin", pool)

        # Verify the format of the HTTP request that was made.
        [request] = axis360.api.requests
        [url, args, kwargs] = request
        data = kwargs.pop("data")
        assert kwargs["method"] == "GET"
        expect = "/EarlyCheckInTitle/v3?itemID={}&patronID={}".format(
            pool.identifier.identifier,
            barcode,
        )
        assert expect in url

    def test_checkin_failure(self, axis360: Axis360Fixture):
        # Verify that we correctly handle failure conditions sent from
        # the EarlyCheckInTitle endpoint.
        edition, pool = axis360.db.edition(
            identifier_type=Identifier.AXIS_360_ID,
            data_source_name=DataSource.AXIS_360,
            with_license_pool=True,
        )
        data = axis360.sample_data("checkin_failure.xml")
        axis360.api.queue_response(200, content=data)
        patron = axis360.db.patron()
        patron.authorization_identifier = axis360.db.fresh_str()
        pytest.raises(NotFoundOnRemote, axis360.api.checkin, patron, "pin", pool)

    def test_place_hold(self, axis360: Axis360Fixture, library_fixture: LibraryFixture):
        edition, pool = axis360.db.edition(
            identifier_type=Identifier.AXIS_360_ID,
            data_source_name=DataSource.AXIS_360,
            with_license_pool=True,
        )
        data = axis360.sample_data("place_hold_success.xml")
        axis360.api.queue_response(200, content=data)
        library = library_fixture.library()
        library_settings = library_fixture.settings(library)
        patron = axis360.db.patron(library=library)
        library_settings.default_notification_email_address = (
            "notifications@example.com"
        )

        response = axis360.api.place_hold(patron, "pin", pool, None)
        assert 1 == response.hold_position
        assert response.identifier_type == pool.identifier.type
        assert response.identifier == pool.identifier.identifier
        [request] = axis360.api.requests
        params = request[-1]["params"]
        assert "notifications@example.com" == params["email"]

    def test_fulfill(self, axis360: Axis360Fixture):
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

        # If Axis 360 says a patron does not have a title checked out,
        # an attempt to fulfill that title will fail with NoActiveLoan.
        data = axis360.sample_data("availability_with_audiobook_fulfillment.xml")
        axis360.api.queue_response(200, content=data)
        pytest.raises(NoActiveLoan, fulfill)

        # If an ebook is checked out and we're not asking for it to be
        # fulfilled through Adobe DRM, we get a Axis360AcsFulfillment
        # object with a content link.
        data = axis360.sample_data("availability_with_loan_and_hold.xml")
        axis360.api.queue_response(200, content=data)
        fulfillment = fulfill()
        assert isinstance(fulfillment, Axis360AcsFulfillment)
        assert not isinstance(fulfillment, Axis360Fulfillment)
        assert DeliveryMechanism.ADOBE_DRM == fulfillment.content_type
        assert "http://fulfillment/" == fulfillment.content_link

        # If we ask for AxisNow format, we get an Axis360Fulfillment
        # containing an AxisNow manifest document.
        data = axis360.sample_data("availability_with_axisnow_fulfillment.xml")
        data = data.replace(b"0016820953", pool.identifier.identifier.encode("utf8"))
        axis360.api.queue_response(200, content=data)
        delivery_mechanism.drm_scheme = DeliveryMechanism.AXISNOW_DRM
        fulfillment = fulfill()
        assert isinstance(fulfillment, Axis360Fulfillment)

        # Looking up the details of the Axis360Fulfillment will
        # trigger another API request, so we won't do that; that's
        # tested in TestAxis360Fulfillment.

        # If the title is checked out but Axis provides no fulfillment
        # info, the exception is CannotFulfill.
        pool.identifier.identifier = "0015176429"
        data = axis360.sample_data("availability_without_fulfillment.xml")
        axis360.api.queue_response(200, content=data)
        pytest.raises(CannotFulfill, fulfill)

        # If we ask to fulfill an audiobook, we get an Axis360Fulfillment, since
        # it can handle both cases.
        #
        # Change our test LicensePool's identifier to match the data we're about
        # to load into the API.
        pool.identifier, ignore = Identifier.for_foreign_id(
            axis360.db.session, Identifier.AXIS_360_ID, "0012244222"
        )
        data = axis360.sample_data("availability_with_audiobook_fulfillment.xml")
        axis360.api.queue_response(200, content=data)
        delivery_mechanism.drm_scheme = DeliveryMechanism.FINDAWAY_DRM
        fulfillment = fulfill()
        assert isinstance(fulfillment, Axis360Fulfillment)

    def test_patron_activity(self, axis360: Axis360Fixture):
        """Test the method that locates all current activity
        for a patron.
        """
        data = axis360.sample_data("availability_with_loan_and_hold.xml")
        axis360.api.queue_response(200, content=data)
        patron = axis360.db.patron()
        patron.authorization_identifier = "a barcode"

        results = axis360.api.patron_activity(patron, "pin")

        # We made a request that included the authorization identifier
        # of the patron in question.
        [url, args, kwargs] = axis360.api.requests.pop()
        assert patron.authorization_identifier == kwargs["params"]["patronId"]

        # We got three results -- two holds and one loan.
        [hold1, loan, hold2] = sorted(results, key=lambda x: str(x.identifier))
        assert isinstance(hold1, HoldInfo)
        assert isinstance(hold2, HoldInfo)
        assert isinstance(loan, LoanInfo)

        # If the activity includes something with a Blio format, it is not included in the results.
        data = axis360.sample_data("availability_with_axisnow_fulfillment.xml")
        axis360.api.queue_response(200, content=data)
        results = axis360.api.patron_activity(patron, "pin")
        assert len(results) == 0

    def test_update_licensepools_for_identifiers(self, axis360: Axis360Fixture):
        class Mock(MockAxis360API):
            """Simulates an Axis 360 API that knows about some
            books but not others.
            """

            updated = []  # type: ignore
            reaped = []

            def _fetch_remote_availability(self, identifiers):
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

            def _reap(self, identifier):
                self.reaped.append(identifier)

        api = Mock(axis360.db.session, axis360.collection)
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
        assert 7 == lp.licenses_owned
        assert 6 == lp.licenses_available

        # The second was reaped.
        assert [no_longer_in_collection] == api.reaped

    def test_fetch_remote_availability(self, axis360: Axis360Fixture):
        # Test the _fetch_remote_availability method, as
        # used by update_licensepools_for_identifiers.

        id1 = axis360.db.identifier(identifier_type=Identifier.AXIS_360_ID)
        id2 = axis360.db.identifier(identifier_type=Identifier.AXIS_360_ID)
        data = axis360.sample_data("availability_with_loans.xml")
        # Modify the sample data so that it appears to be talking
        # about one of the books we're going to request.
        data = data.replace(b"0012533119", id1.identifier.encode("utf8"))
        axis360.api.queue_response(200, {}, data)
        results = [x for x in axis360.api._fetch_remote_availability([id1, id2])]

        # We asked for information on two identifiers.
        [request] = axis360.api.requests
        kwargs = request[-1]
        assert {"titleIds": "2001,2002"} == kwargs["params"]

        # We got information on only one.
        [(metadata, circulation)] = results
        assert id1 == metadata.load_primary_identifier(axis360.db.session)
        assert (
            "El caso de la gracia : Un periodista explora las evidencias de unas vidas transformadas"
            == metadata.title
        )
        assert 2 == circulation.licenses_owned

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

    def test_get_fulfillment_info(self, axis360: Axis360Fixture):
        # Test the get_fulfillment_info method, which makes an API request.

        api = MockAxis360API(axis360.db.session, axis360.collection)
        api.queue_response(200, {}, "the response")

        # Make a request and check the response.
        response = api.get_fulfillment_info("transaction ID")
        assert b"the response" == response.content

        # Verify that the 'HTTP request' was made to the right URL
        # with the right keyword arguments and the right HTTP method.
        url, args, kwargs = api.requests.pop()
        assert url.endswith(api.fulfillment_endpoint)
        assert "POST" == kwargs["method"]
        assert "transaction ID" == kwargs["params"]["TransactionID"]

    def test_get_audiobook_metadata(self, axis360: Axis360Fixture):
        # Test the get_audiobook_metadata method, which makes an API request.

        api = MockAxis360API(axis360.db.session, axis360.collection)
        api.queue_response(200, {}, "the response")

        # Make a request and check the response.
        response = api.get_audiobook_metadata("Findaway content ID")
        assert b"the response" == response.content

        # Verify that the 'HTTP request' was made to the right URL
        # with the right keyword arguments and the right HTTP method.
        url, args, kwargs = api.requests.pop()
        assert url.endswith(api.audiobook_metadata_endpoint)
        assert "POST" == kwargs["method"]
        assert "Findaway content ID" == kwargs["params"]["fndcontentid"]

    def test_update_book(self, axis360: Axis360Fixture):
        # Verify that the update_book method takes a BibliographicData object,
        # and creates appropriate data model objects.

        api = MockAxis360API(axis360.db.session, axis360.collection)
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

    @pytest.mark.parametrize(
        ("setting", "setting_value", "attribute", "attribute_value"),
        [
            (Axis360API.VERIFY_SSL, None, "verify_certificate", True),
            (Axis360API.VERIFY_SSL, True, "verify_certificate", True),
            (Axis360API.VERIFY_SSL, False, "verify_certificate", False),
        ],
    )
    def test_integration_settings(
        self,
        setting,
        setting_value,
        attribute,
        attribute_value,
        axis360: Axis360Fixture,
    ):
        config = axis360.collection.integration_configuration
        settings = config.settings_dict.copy()
        if setting_value is not None:
            settings[setting] = setting_value
            config.settings_dict = settings
        api = MockAxis360API(axis360.db.session, axis360.collection)
        assert getattr(api, attribute) == attribute_value

    @pytest.mark.parametrize(
        ("setting", "setting_value", "is_valid", "expected"),
        [
            (
                "url",
                "production",
                True,
                Axis360APIConstants.SERVER_NICKNAMES["production"],
            ),
            ("url", "qa", True, Axis360APIConstants.SERVER_NICKNAMES["qa"]),
            ("url", "not-production", False, None),
            ("url", "http://any.url.will.do", True, "http://any.url.will.do/"),
        ],
    )
    def test_integration_settings_url(
        self, setting, setting_value, is_valid, expected, axis360: Axis360Fixture
    ):
        config = axis360.collection.integration_configuration
        config.settings_dict[setting] = setting_value

        if is_valid:
            integration_settings_update(
                Axis360Settings, config, {setting: setting_value}, merge=True
            )
            api = MockAxis360API(axis360.db.session, axis360.collection)
            assert api.base_url == expected
        else:
            pytest.raises(
                ProblemDetailException,
                integration_settings_update,
                Axis360Settings,
                config,
                {setting: setting_value},
                merge=True,
            )

    def test_availablility_by_title_ids(self, axis360: Axis360Fixture):
        ids = ["my_id"]
        with patch.object(axis360.api, "availability") as availability:
            availability.content.return_value = """
            <?xml version="1.0" encoding="utf-8"?>
            """
            for metadata, circulation in axis360.api.availability_by_title_ids(
                title_ids=ids
            ):
                pass

            assert availability.call_args_list[0].kwargs["title_ids"] == ids
