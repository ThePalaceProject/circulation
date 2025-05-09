from __future__ import annotations

import copy
import datetime
import json
import socket
import ssl
import urllib
from contextlib import contextmanager
from functools import partial
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, Mock, PropertyMock, patch

import pytest

from palace.manager.api.axis import (
    AudiobookMetadataParser,
    AvailabilityResponseParser,
    Axis360AcsFulfillment,
    Axis360API,
    Axis360APIConstants,
    Axis360Fulfillment,
    Axis360FulfillmentInfoResponseParser,
    Axis360Settings,
    AxisNowManifest,
    BibliographicParser,
    CheckinResponseParser,
    CheckoutResponseParser,
    HoldReleaseResponseParser,
    HoldResponseParser,
    JSONResponseParser,
    StatusResponseParser,
)
from palace.manager.api.circulation import HoldInfo, LoanInfo, UrlFulfillment
from palace.manager.api.circulation_exceptions import (
    AlreadyCheckedOut,
    AlreadyOnHold,
    CannotFulfill,
    NoActiveLoan,
    NotFoundOnRemote,
    NotOnHold,
    PatronAuthorizationFailedException,
    RemoteInitiatedServerError,
)
from palace.manager.api.web_publication_manifest import FindawayManifest, SpineItem
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.contributor import ContributorData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.subject import SubjectData
from palace.manager.integration.base import integration_settings_update
from palace.manager.sqlalchemy.constants import LinkRelations, MediaTypes
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.util.datetime_helpers import datetime_utc, utc_now
from palace.manager.util.flask_util import Response
from palace.manager.util.http import RemoteIntegrationException
from palace.manager.util.problem_detail import (
    BaseProblemDetailException,
    ProblemDetailException,
)
from tests.fixtures.files import FilesFixture
from tests.fixtures.library import LibraryFixture
from tests.mocks.axis import MockAxis360API

if TYPE_CHECKING:
    from tests.fixtures.database import DatabaseTransactionFixture


class AxisFilesFixture(FilesFixture):
    """A fixture providing access to Axis files."""

    def __init__(self):
        super().__init__("axis")


@pytest.fixture()
def axis_files_fixture() -> AxisFilesFixture:
    """A fixture providing access to Axis files."""
    return AxisFilesFixture()


primary_identifier = IdentifierData(
    type=Identifier.AXIS_360_ID, identifier="0003642860"
)


class Axis360Fixture:
    # Sample bibliographic and availability data you can use in a test
    # without having to parse it from an XML file.

    BIBLIOGRAPHIC_DATA = BibliographicData(
        data_source_name=DataSource.AXIS_360,
        publisher="Random House Inc",
        language="eng",
        title="Faith of My Fathers : A Family Memoir",
        imprint="Random House Inc2",
        published=datetime_utc(2000, 3, 7, 0, 0),
        primary_identifier_data=primary_identifier,
        identifiers=[IdentifierData(type=Identifier.ISBN, identifier="9780375504587")],
        contributors=[
            ContributorData(
                sort_name="McCain, John", roles=[Contributor.Role.PRIMARY_AUTHOR]
            ),
            ContributorData(sort_name="Salter, Mark", roles=[Contributor.Role.AUTHOR]),
        ],
        subjects=[
            SubjectData(
                type=Subject.BISAC, identifier="BIOGRAPHY & AUTOBIOGRAPHY / Political"
            ),
            SubjectData(type=Subject.FREEFORM_AUDIENCE, identifier="Adult"),
        ],
        circulation=CirculationData(
            data_source_name=DataSource.AXIS_360,
            primary_identifier_data=primary_identifier,
            licenses_owned=9,
            licenses_available=8,
            licenses_reserved=0,
            patrons_in_hold_queue=0,
            last_checked=datetime_utc(2015, 5, 20, 2, 9, 8),
        ),
    )

    def __init__(self, db: DatabaseTransactionFixture, files: AxisFilesFixture):
        self.db = db
        self.files = files
        self.collection = MockAxis360API.mock_collection(
            db.session, db.default_library()
        )
        self.api = MockAxis360API(db.session, self.collection)

    def sample_data(self, filename):
        return self.files.sample_data(filename)

    def sample_text(self, filename):
        return self.files.sample_text(filename)


@pytest.fixture(scope="function")
def axis360(
    db: DatabaseTransactionFixture, axis_files_fixture: AxisFilesFixture
) -> Axis360Fixture:
    return Axis360Fixture(db, axis_files_fixture)


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


class TestParsers:
    def test_status_parser(self, axis360: Axis360Fixture):
        data = axis360.sample_data("availability_patron_not_found.xml")
        parser = StatusResponseParser()
        parsed = parser.process_first(data)
        assert parsed is not None
        status, message = parsed
        assert status == 3122
        assert message == "Patron information is not found."

        data = axis360.sample_data("availability_with_loans.xml")
        parsed = parser.process_first(data)
        assert parsed is not None
        status, message = parsed
        assert status == 0
        assert message == "Availability Data is Successfully retrieved."

        data = axis360.sample_data("availability_with_ebook_fulfillment.xml")
        parsed = parser.process_first(data)
        assert parsed is not None
        status, message = parsed
        assert status == 0
        assert message == "Availability Data is Successfully retrieved."

        data = axis360.sample_data("checkin_failure.xml")
        parsed = parser.process_first(data)
        assert parsed is not None
        status, message = parsed
        assert status == 3103
        assert message == "Invalid Title Id"

        data = axis360.sample_data("invalid_error_code.xml")
        assert parser.process_first(data) is None

        data = axis360.sample_data("missing_error_code.xml")
        assert parser.process_first(data) is None
        assert parser.process_first(None) is None
        assert parser.process_first(b"") is None
        assert parser.process_first(b"not xml") is None

    def test_bibliographic_parser(self, axis360: Axis360Fixture):
        # Make sure the bibliographic information gets properly
        # collated in preparation for creating Edition objects.

        data = axis360.sample_data("tiny_collection.xml")

        [bib1, av1], [bib2, av2] = BibliographicParser().process_all(data)

        # We test for availability information in a separate test.
        # Here we just make sure it is present.
        assert av1 is not None
        assert av2 is not None

        # But we did get bibliographic information.
        assert bib1 is not None
        assert bib2 is not None

        assert "Faith of My Fathers : A Family Memoir" == bib1.title
        assert "eng" == bib1.language
        assert datetime.date(2000, 3, 7) == bib1.published

        assert "Simon & Schuster" == bib2.publisher
        assert "Pocket Books" == bib2.imprint

        assert Edition.BOOK_MEDIUM == bib1.medium

        # TODO: Would be nicer if we could test getting a real value
        # for this.
        assert None == bib2.series

        # Book #1 has two links -- a description and a cover image.
        [description, cover] = bib1.links
        assert Hyperlink.DESCRIPTION == description.rel
        assert Representation.TEXT_PLAIN == description.media_type
        assert isinstance(description.content, str)
        assert description.content.startswith("John McCain's deeply moving memoir")

        # The cover image simulates the current state of the B&T cover
        # service, where we get a thumbnail-sized image URL in the
        # Axis 360 API response and we can hack the URL to get the
        # full-sized image URL.
        assert LinkRelations.IMAGE == cover.rel
        assert (
            "http://contentcafecloud.baker-taylor.com/Jacket.svc/D65D0665-050A-487B-9908-16E6D8FF5C3E/9780375504587/Large/Empty"
            == cover.href
        )
        assert MediaTypes.JPEG_MEDIA_TYPE == cover.media_type

        assert LinkRelations.THUMBNAIL_IMAGE == cover.thumbnail.rel
        assert (
            "http://contentcafecloud.baker-taylor.com/Jacket.svc/D65D0665-050A-487B-9908-16E6D8FF5C3E/9780375504587/Medium/Empty"
            == cover.thumbnail.href
        )
        assert MediaTypes.JPEG_MEDIA_TYPE == cover.thumbnail.media_type

        # Book #1 has a primary author, another author and a narrator.
        #
        # TODO: The narrator data is simulated. we haven't actually
        # verified that Axis 360 sends narrator information in the
        # same format as author information.
        [cont1, cont2, narrator] = bib1.contributors
        assert "McCain, John" == cont1.sort_name
        assert (Contributor.Role.PRIMARY_AUTHOR,) == cont1.roles

        assert "Salter, Mark" == cont2.sort_name
        assert (Contributor.Role.AUTHOR,) == cont2.roles

        assert "McCain, John S. III" == narrator.sort_name
        assert (Contributor.Role.NARRATOR,) == narrator.roles

        # Book #2 only has a primary author.
        [cont] = bib2.contributors
        assert "Pollero, Rhonda" == cont.sort_name
        assert (Contributor.Role.PRIMARY_AUTHOR,) == cont.roles

        axis_id, isbn = sorted(bib1.identifiers, key=lambda x: x.identifier)
        assert "0003642860" == axis_id.identifier
        assert "9780375504587" == isbn.identifier

        # Check the subjects for #2 because it includes an audience,
        # unlike #1.
        subjects = sorted(bib2.subjects, key=lambda x: x.identifier or "")
        assert [
            Subject.BISAC,
            Subject.BISAC,
            Subject.BISAC,
            Subject.AXIS_360_AUDIENCE,
        ] == [x.type for x in subjects]
        general_fiction, women_sleuths, romantic_suspense = sorted(
            x.name for x in subjects if x.type == Subject.BISAC and x.name is not None
        )
        assert "FICTION / General" == general_fiction
        assert "FICTION / Mystery & Detective / Women Sleuths" == women_sleuths
        assert "FICTION / Romance / Suspense" == romantic_suspense

        [adult] = [
            x.identifier for x in subjects if x.type == Subject.AXIS_360_AUDIENCE
        ]
        assert "General Adult" == adult

        # The second book has a cover image simulating some possible
        # future case, where B&T change their cover service so that
        # the size URL hack no longer works. In this case, we treat
        # the image URL as both the full-sized image and the
        # thumbnail.
        [cover] = bib2.links
        assert LinkRelations.IMAGE == cover.rel
        assert "http://some-other-server/image.jpg" == cover.href
        assert MediaTypes.JPEG_MEDIA_TYPE == cover.media_type

        assert LinkRelations.THUMBNAIL_IMAGE == cover.thumbnail.rel
        assert "http://some-other-server/image.jpg" == cover.thumbnail.href
        assert MediaTypes.JPEG_MEDIA_TYPE == cover.thumbnail.media_type

        # The first book is available in two formats -- "ePub" and "AxisNow"
        [adobe, axisnow] = bib1.circulation.formats
        assert Representation.EPUB_MEDIA_TYPE == adobe.content_type
        assert DeliveryMechanism.ADOBE_DRM == adobe.drm_scheme

        assert None == axisnow.content_type
        assert DeliveryMechanism.AXISNOW_DRM == axisnow.drm_scheme

        # The second book is available in 'Blio' format, which
        # is treated as an alternate name for 'AxisNow'
        [axisnow] = bib2.circulation.formats
        assert None == axisnow.content_type
        assert DeliveryMechanism.AXISNOW_DRM == axisnow.drm_scheme

    def test_bibliographic_parser_audiobook(self, axis360: Axis360Fixture):
        # TODO - we need a real example to test from. The example we were
        # given is a hacked-up ebook. Ideally we would be able to check
        # narrator information here.
        data = axis360.sample_data("availability_with_audiobook_fulfillment.xml")

        [[bib, av]] = BibliographicParser().process_all(data)
        assert av is not None
        assert bib is not None

        assert "Back Spin" == bib.title
        assert Edition.AUDIO_MEDIUM == bib.medium

        # The audiobook has one DeliveryMechanism, in which the Findaway licensing document
        # acts as both the content type and the DRM scheme.
        [findaway] = bib.circulation.formats
        assert None == findaway.content_type
        assert DeliveryMechanism.FINDAWAY_DRM == findaway.drm_scheme

        # Although the audiobook is also available in the "AxisNow"
        # format, no second delivery mechanism was created for it, the
        # way it would have been for an ebook.
        assert b"<formatName>AxisNow</formatName>" in data

    def test_bibliographic_parser_blio_format(self, axis360: Axis360Fixture):
        # This book is available as 'Blio' but not 'AxisNow'.
        data = axis360.sample_data("availability_with_audiobook_fulfillment.xml")
        data = data.replace(b"Acoustik", b"Blio")
        data = data.replace(b"AxisNow", b"No Such Format")

        [[bib, av]] = BibliographicParser().process_all(data)
        assert av is not None
        assert bib is not None

        # A book in Blio format is treated as an AxisNow ebook.
        assert Edition.BOOK_MEDIUM == bib.medium
        [axisnow] = bib.circulation.formats
        assert None == axisnow.content_type
        assert DeliveryMechanism.AXISNOW_DRM == axisnow.drm_scheme

    def test_bibliographic_parser_blio_and_axisnow_format(
        self, axis360: Axis360Fixture
    ):
        # This book is available as both 'Blio' and 'AxisNow'.
        data = axis360.sample_data("availability_with_audiobook_fulfillment.xml")
        data = data.replace(b"Acoustik", b"Blio")

        [[bib, av]] = BibliographicParser().process_all(data)
        assert av is not None
        assert bib is not None

        # There is only one FormatData -- 'Blio' and 'AxisNow' mean the same thing.
        assert Edition.BOOK_MEDIUM == bib.medium
        [axisnow] = bib.circulation.formats
        assert None == axisnow.content_type
        assert DeliveryMechanism.AXISNOW_DRM == axisnow.drm_scheme

    def test_bibliographic_parser_unsupported_format(self, axis360: Axis360Fixture):
        data = axis360.sample_data("availability_with_audiobook_fulfillment.xml")
        data = data.replace(b"Acoustik", b"No Such Format 1")
        data = data.replace(b"AxisNow", b"No Such Format 2")

        [[bib, av]] = BibliographicParser().process_all(data)
        assert av is not None
        assert bib is not None

        # We don't support any of the formats, so no FormatData objects were created.
        assert [] == bib.circulation.formats

    def test_parse_author_role(self, axis360: Axis360Fixture):
        """Suffixes on author names are turned into roles."""
        author = "Dyssegaard, Elisabeth Kallick (TRN)"
        parse = BibliographicParser.parse_contributor
        c = parse(author)
        assert "Dyssegaard, Elisabeth Kallick" == c.sort_name
        assert (Contributor.Role.TRANSLATOR,) == c.roles

        # A corporate author is given a normal author role.
        author = "Bob, Inc. (COR)"
        c = parse(author, primary_author_found=False)
        assert "Bob, Inc." == c.sort_name
        assert (Contributor.Role.PRIMARY_AUTHOR,) == c.roles

        c = parse(author, primary_author_found=True)
        assert "Bob, Inc." == c.sort_name
        assert (Contributor.Role.AUTHOR,) == c.roles

        # An unknown author type is given an unknown role
        author = "Eve, Mallory (ZZZ)"
        c = parse(author, primary_author_found=False)
        assert "Eve, Mallory" == c.sort_name
        assert (Contributor.Role.UNKNOWN,) == c.roles

        # force_role overwrites whatever other role might be
        # assigned.
        author = "Bob, Inc. (COR)"
        c = parse(
            author, primary_author_found=False, force_role=Contributor.Role.NARRATOR
        )
        assert (Contributor.Role.NARRATOR,) == c.roles

    def test_availability_parser(self, axis360: Axis360Fixture):
        """Make sure the availability information gets properly
        collated in preparation for updating a LicensePool.
        """

        data = axis360.sample_data("tiny_collection.xml")

        [bib1, av1], [bib2, av2] = BibliographicParser().process_all(data)

        # We already tested the bibliographic information, so we just make sure
        # it is present.
        assert bib1 is not None
        assert bib2 is not None

        # But we did get availability information.
        assert av1 is not None
        assert av2 is not None

        assert (
            "0003642860" == av1.load_primary_identifier(axis360.db.session).identifier
        )
        assert 9 == av1.licenses_owned
        assert 9 == av1.licenses_available
        assert 0 == av1.patrons_in_hold_queue


class Axis360FixturePlusParsers(Axis360Fixture):
    def __init__(self, db: DatabaseTransactionFixture, files: AxisFilesFixture):
        super().__init__(db, files)

        # We don't need an actual Collection object to test most of
        # these classes, but we do need to test that whatever object
        # we _claim_ is a Collection will have its id put into the
        # right spot of HoldInfo and LoanInfo objects.

        self.default_collection = MagicMock(spec=Collection)
        type(self.default_collection).id = PropertyMock(return_value=1337)


@pytest.fixture(scope="function")
def axis360parsers(
    db: DatabaseTransactionFixture, axis_files_fixture: AxisFilesFixture
) -> Axis360FixturePlusParsers:
    return Axis360FixturePlusParsers(db, axis_files_fixture)


class TestRaiseExceptionOnError:
    def test_internal_server_error(self, axis360parsers: Axis360FixturePlusParsers):
        data = axis360parsers.sample_data("internal_server_error.xml")
        parser = HoldReleaseResponseParser()
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            parser.process_first(data)
        assert "Internal Server Error" in str(excinfo.value)

    def test_ignore_error_codes(self, axis360parsers: Axis360FixturePlusParsers):
        # A parser subclass can decide not to raise exceptions
        # when encountering specific error codes.
        data = axis360parsers.sample_data("internal_server_error.xml")
        retval = object()

        class IgnoreISE(HoldReleaseResponseParser):
            def process_one(self, e, namespaces):
                self.raise_exception_on_error(e, namespaces, ignore_error_codes=[5000])
                return retval

        # Unlike in test_internal_server_error, no exception is
        # raised, because we told the parser to ignore this particular
        # error code.
        parser = IgnoreISE()
        assert retval == parser.process_first(data)

    def test_internal_server_error2(self, axis360parsers: Axis360FixturePlusParsers):
        data = axis360parsers.sample_data("invalid_error_code.xml")
        parser = HoldReleaseResponseParser()
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            parser.process_first(data)
        assert "Invalid response code from Axis 360: abcd" in str(excinfo.value)

    def test_missing_error_code(self, axis360parsers: Axis360FixturePlusParsers):
        data = axis360parsers.sample_data("missing_error_code.xml")
        parser = HoldReleaseResponseParser()
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            parser.process_first(data)
        assert "No status code!" in str(excinfo.value)


class TestCheckinResponseParser:
    def test_parse_checkin_success(self, axis360parsers: Axis360FixturePlusParsers):
        # The response parser raises an exception if there's a problem,
        # and returne True otherwise.
        #
        # "Book is not on loan" is not treated as a problem.
        for filename in ("checkin_success.xml", "checkin_not_checked_out.xml"):
            data = axis360parsers.sample_data(filename)
            parser = CheckinResponseParser()
            parsed = parser.process_first(data)
            assert parsed is True

    def test_parse_checkin_failure(self, axis360parsers: Axis360FixturePlusParsers):
        data = axis360parsers.sample_data("checkin_failure.xml")
        parser = CheckinResponseParser()
        pytest.raises(NotFoundOnRemote, parser.process_first, data)


class TestCheckoutResponseParser:
    def test_parse_checkout_success(self, axis360parsers: Axis360FixturePlusParsers):
        data = axis360parsers.sample_data("checkout_success.xml")
        parser = CheckoutResponseParser()
        parsed = parser.process_first(data)
        assert datetime_utc(2015, 8, 11, 18, 57, 42) == parsed

    def test_parse_already_checked_out(self, axis360parsers: Axis360FixturePlusParsers):
        data = axis360parsers.sample_data("already_checked_out.xml")
        parser = CheckoutResponseParser()
        pytest.raises(AlreadyCheckedOut, parser.process_first, data)

    def test_parse_not_found_on_remote(self, axis360parsers: Axis360FixturePlusParsers):
        data = axis360parsers.sample_data("not_found_on_remote.xml")
        parser = CheckoutResponseParser()
        pytest.raises(NotFoundOnRemote, parser.process_first, data)


class TestHoldResponseParser:
    def test_parse_hold_success(self, axis360parsers: Axis360FixturePlusParsers):
        data = axis360parsers.sample_data("place_hold_success.xml")
        parser = HoldResponseParser()
        parsed = parser.process_first(data)
        assert 1 == parsed

    def test_parse_already_on_hold(self, axis360parsers: Axis360FixturePlusParsers):
        data = axis360parsers.sample_data("already_on_hold.xml")
        parser = HoldResponseParser()
        pytest.raises(AlreadyOnHold, parser.process_first, data)


class TestHoldReleaseResponseParser:
    def test_success(self, axis360parsers: Axis360FixturePlusParsers):
        data = axis360parsers.sample_data("release_hold_success.xml")
        parser = HoldReleaseResponseParser()
        assert True == parser.process_first(data)

    def test_failure(self, axis360parsers: Axis360FixturePlusParsers):
        data = axis360parsers.sample_data("release_hold_failure.xml")
        parser = HoldReleaseResponseParser()
        pytest.raises(NotOnHold, parser.process_first, data)


class TestAvailabilityResponseParser:
    """Unlike other response parser tests, this one needs
    access to a real database session, because it needs a real Collection
    to put into its MockAxis360API.
    """

    def test_parse_loan_and_hold(self, axis360parsers: Axis360FixturePlusParsers):
        data = axis360parsers.sample_text("availability_with_loan_and_hold.xml")
        parser = AvailabilityResponseParser(axis360parsers.api)
        activity = list(parser.process_all(data))
        hold, loan, reserved = sorted(
            activity, key=lambda x: "" if x is None else str(x.identifier)
        )
        assert isinstance(hold, HoldInfo)
        assert isinstance(loan, LoanInfo)
        assert isinstance(reserved, HoldInfo)
        assert axis360parsers.api.collection is not None
        assert axis360parsers.api.collection.id == hold.collection_id
        assert Identifier.AXIS_360_ID == hold.identifier_type
        assert "0012533119" == hold.identifier
        assert 1 == hold.hold_position
        assert hold.end_date is None

        assert axis360parsers.api.collection.id == loan.collection_id
        assert "0015176429" == loan.identifier
        assert isinstance(loan.fulfillment, UrlFulfillment)
        assert "http://fulfillment/" == loan.fulfillment.content_link
        assert datetime_utc(2015, 8, 12, 17, 40, 27) == loan.end_date

        assert axis360parsers.api.collection.id == reserved.collection_id
        assert "1111111111" == reserved.identifier
        assert datetime_utc(2015, 1, 1, 13, 11, 11) == reserved.end_date
        assert 0 == reserved.hold_position

    def test_parse_loan_no_availability(
        self, axis360parsers: Axis360FixturePlusParsers
    ):
        data = axis360parsers.sample_text("availability_without_fulfillment.xml")
        parser = AvailabilityResponseParser(axis360parsers.api)
        [loan] = list(parser.process_all(data))
        assert isinstance(loan, LoanInfo)

        assert axis360parsers.api.collection is not None
        assert axis360parsers.api.collection.id == loan.collection_id
        assert "0015176429" == loan.identifier
        assert None == loan.fulfillment
        assert datetime_utc(2015, 8, 12, 17, 40, 27) == loan.end_date

    def test_parse_audiobook_availability(
        self, axis360parsers: Axis360FixturePlusParsers
    ):
        data = axis360parsers.sample_text("availability_with_audiobook_fulfillment.xml")
        parser = AvailabilityResponseParser(axis360parsers.api)
        [loan] = list(parser.process_all(data))
        assert isinstance(loan, LoanInfo)
        fulfillment = loan.fulfillment
        assert isinstance(fulfillment, Axis360Fulfillment)

        # The transaction ID is stored as the .key. If we actually
        # need to make a manifest for this book, the key will be used
        # in two more API requests. (See TestAxis360Fulfillment
        # for that.)
        assert "C3F71F8D-1883-2B34-061F-96570678AEB0" == fulfillment.key

        # The API object is present in the Fulfillment and ready to go.
        assert axis360parsers.api == fulfillment.api

    def test_parse_ebook_availability(self, axis360parsers: Axis360FixturePlusParsers):
        # AvailabilityResponseParser will behave differently depending on whether
        # we ask for the book as an ePub or through AxisNow.
        data = axis360parsers.sample_text("availability_with_ebook_fulfillment.xml")

        # First, ask for an ePub.
        epub_parser = AvailabilityResponseParser(axis360parsers.api, "ePub")
        [availability] = list(epub_parser.process_all(data))
        assert isinstance(availability, LoanInfo)
        fulfillment = availability.fulfillment

        # This particular file has a downloadUrl ready to go, so we
        # get a standard Axis360AcsFulfillment object with that downloadUrl
        # as its content_link.
        assert isinstance(fulfillment, Axis360AcsFulfillment)
        assert not isinstance(fulfillment, Axis360Fulfillment)
        assert (
            "http://adobe.acsm/?src=library&transactionId=2a34598b-12af-41e4-a926-af5e42da7fe5&isbn=9780763654573&format=F2"
            == fulfillment.content_link
        )

        # Next ask for AxisNow -- this will be more like
        # test_parse_audiobook_availability, since it requires an
        # additional API request.

        axisnow_parser = AvailabilityResponseParser(
            axis360parsers.api, axis360parsers.api.AXISNOW
        )
        [availability] = list(axisnow_parser.process_all(data))
        assert isinstance(availability, LoanInfo)
        fulfillment = availability.fulfillment
        assert isinstance(fulfillment, Axis360Fulfillment)
        assert "6670197A-D264-447A-86C7-E4CB829C0236" == fulfillment.key

        # The API object is present in the Fulfillment and ready to go
        # make that extra request.
        assert axis360parsers.api == fulfillment.api

    def test_patron_not_found(self, axis360parsers: Axis360FixturePlusParsers):
        # If the patron is not found, the parser will return an empty list, since
        # that patron can't have any loans or holds.
        data = axis360parsers.sample_text("availability_patron_not_found.xml")
        parser = AvailabilityResponseParser(axis360parsers.api)
        assert list(parser.process_all(data)) == []


class TestJSONResponseParser:
    def test__required_key(self):
        m = JSONResponseParser._required_key
        parsed = dict(key="value")

        # If the value is present, _required_key acts just like get().
        assert "value" == m("key", parsed)

        # If not, it raises a RemoteInitiatedServerError.
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            m("absent", parsed)
        assert (
            "Required key absent not present in Axis 360 fulfillment document: {'key': 'value'}"
            in str(excinfo.value)
        )

    def test_verify_status_code(self):
        success = dict(Status=dict(Code=0000))
        failure = dict(Status=dict(Code=1000, Message="A message"))
        missing = dict()

        m = JSONResponseParser.verify_status_code

        # If the document's Status object indicates success, nothing
        # happens.
        m(success)

        # If it indicates failure, an appropriate exception is raised.
        with pytest.raises(PatronAuthorizationFailedException) as excinfo:
            m(failure)
        assert "A message" in str(excinfo.value)

        # If the Status object is missing, a more generic exception is
        # raised.
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            m(missing)
        assert (
            "Required key Status not present in Axis 360 fulfillment document"
            in str(excinfo.value)
        )

    def test_parse(self):
        class Mock(JSONResponseParser):
            def _parse(self, parsed, **kwargs):
                self.called_with = parsed, kwargs
                return "success"

        parser = Mock()

        # Test success.
        doc = dict(Status=dict(Code=0000))

        # The JSON will be parsed and passed in to _parse(); all other
        # keyword arguments to parse() will be passed through to _parse().
        result = parser.parse(json.dumps(doc), arg2="value2")
        assert "success" == result
        assert (doc, dict(arg2="value2")) == parser.called_with

        # It also works if the JSON was already parsed.
        result = parser.parse(doc, foo="bar")
        assert (doc, {"foo": "bar"}) == parser.called_with

        # Non-JSON input causes an error.
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            parser.parse("I'm not JSON")
        assert (
            'Invalid response from Axis 360 (was expecting JSON): "I\'m not JSON"'
            in str(excinfo.value)
        )


class TestAxis360FulfillmentInfoResponseParser:
    def test__parse_findaway(self, axis360parsers: Axis360FixturePlusParsers) -> None:
        # _parse will create a valid FindawayManifest given a
        # complete document.

        parser = Axis360FulfillmentInfoResponseParser(api=axis360parsers.api)
        m = parser._parse

        edition, pool = axis360parsers.db.edition(with_license_pool=True)

        def get_data():
            # We'll be modifying this document to simulate failures,
            # so make it easy to load a fresh copy.
            return json.loads(
                axis360parsers.sample_data("audiobook_fulfillment_info.json")
            )

        # This is the data we just got from a call to Axis 360's
        # getfulfillmentInfo endpoint.
        data = get_data()

        # When we call _parse, the API is going to fire off an
        # additional request to the getaudiobookmetadata endpoint, so
        # it can create a complete FindawayManifest. Queue up the
        # response to that request.
        audiobook_metadata = axis360parsers.sample_data("audiobook_metadata.json")
        axis360parsers.api.queue_response(200, {}, audiobook_metadata)

        manifest, expires = m(data, license_pool=pool)

        assert isinstance(manifest, FindawayManifest)
        metadata = manifest.metadata

        # The manifest contains information from the LicensePool's presentation
        # edition
        assert edition.title == metadata["title"]

        # It contains DRM licensing information from Findaway via the
        # Axis 360 API.
        encrypted = metadata["encrypted"]
        assert (
            "0f547af1-38c1-4b1c-8a1a-169d353065d0" == encrypted["findaway:sessionKey"]
        )
        assert "5babb89b16a4ed7d8238f498" == encrypted["findaway:checkoutId"]
        assert "04960" == encrypted["findaway:fulfillmentId"]
        assert "58ee81c6d3d8eb3b05597cdc" == encrypted["findaway:licenseId"]

        # The spine items and duration have been filled in by the call to
        # the getaudiobookmetadata endpoint.
        assert 8150.87 == metadata["duration"]
        assert 5 == len(manifest.readingOrder)

        # We also know when the licensing document expires.
        assert datetime_utc(2018, 9, 29, 18, 34) == expires

        # Now strategically remove required information from the
        # document and verify that extraction fails.
        #
        for field in (
            "FNDContentID",
            "FNDLicenseID",
            "FNDSessionKey",
            "ExpirationDate",
        ):
            missing_field = get_data()
            del missing_field[field]
            with pytest.raises(RemoteInitiatedServerError) as excinfo:
                m(missing_field, license_pool=pool)
            assert "Required key %s not present" % field in str(excinfo.value)

        # Try with a bad expiration date.
        bad_date = get_data()
        bad_date["ExpirationDate"] = "not-a-date"
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            m(bad_date, license_pool=pool)
        assert "Could not parse expiration date: not-a-date" in str(excinfo.value)

        # Try with an expired session key.
        expired_session_key = get_data()
        expired_session_key["FNDSessionKey"] = "Expired"
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            m(expired_session_key, license_pool=pool)
        assert "Expired findaway session key" in str(excinfo.value)

    def test__parse_axisnow(self, axis360parsers: Axis360FixturePlusParsers) -> None:
        # _parse will create a valid AxisNowManifest given a
        # complete document.

        parser = Axis360FulfillmentInfoResponseParser(api=axis360parsers.api)
        m = parser._parse

        edition, pool = axis360parsers.db.edition(with_license_pool=True)

        def get_data():
            # We'll be modifying this document to simulate failures,
            # so make it easy to load a fresh copy.
            return json.loads(axis360parsers.sample_data("ebook_fulfillment_info.json"))

        # This is the data we just got from a call to Axis 360's
        # getfulfillmentInfo endpoint.
        data = get_data()

        # Since this is an ebook, not an audiobook, there will be no
        # second request to the API, the way there is in the audiobook
        # test.
        manifest, expires = m(data, license_pool=pool)

        assert isinstance(manifest, AxisNowManifest)
        assert {
            "book_vault_uuid": "1c11c31f-81c2-41bb-9179-491114c3f121",
            "isbn": "9780547351551",
        } == json.loads(str(manifest))

        # Try with a bad expiration date.
        bad_date = get_data()
        bad_date["ExpirationDate"] = "not-a-date"
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            m(bad_date, license_pool=pool)
        assert "Could not parse expiration date: not-a-date" in str(excinfo.value)


class TestAudiobookMetadataParser:
    def test__parse(self, axis360: Axis360Fixture):
        # _parse will find the Findaway account ID and
        # the spine items.
        class Mock(AudiobookMetadataParser):
            @classmethod
            def _extract_spine_item(cls, part):
                return part + " (extracted)"

        metadata = dict(
            fndaccountid="An account ID", readingOrder=["Spine item 1", "Spine item 2"]
        )
        account_id, spine_items = Mock()._parse(metadata)

        assert "An account ID" == account_id
        assert ["Spine item 1 (extracted)", "Spine item 2 (extracted)"] == spine_items

        # No data? Nothing will be parsed.
        account_id, spine_items = Mock()._parse({})
        assert None == account_id
        assert [] == spine_items

    def test__extract_spine_item(self, axis360: Axis360Fixture):
        # _extract_spine_item will turn data from Findaway into
        # a SpineItem object.
        m = AudiobookMetadataParser._extract_spine_item
        item = m(
            dict(duration=100.4, fndpart=2, fndsequence=3, title="The Gathering Storm")
        )
        assert isinstance(item, SpineItem)
        assert "The Gathering Storm" == item.title
        assert 2 == item.part
        assert 3 == item.sequence
        assert 100.4 == item.duration
        assert Representation.MP3_MEDIA_TYPE == item.media_type

        # We get a SpineItem even if all the data about the spine item
        # is missing -- these are the default values.
        item = m({})
        assert None == item.title
        assert 0 == item.part
        assert 0 == item.sequence
        assert 0 == item.duration
        assert Representation.MP3_MEDIA_TYPE == item.media_type


class TestAxis360Fulfillment:
    """An Axis360Fulfillment can fulfill a title whether it's an ebook
    (fulfilled through AxisNow) or an audiobook (fulfilled through
    Findaway).
    """

    def test_fetch_audiobook(self, axis360: Axis360Fixture):
        # When Findaway information is present in the response from
        # the fulfillment API, a second request is made to get
        # spine-item metadata. Information from both requests is
        # combined into a Findaway fulfillment document.
        fulfillment_info = axis360.sample_data("audiobook_fulfillment_info.json")
        axis360.api.queue_response(200, {}, fulfillment_info)

        metadata = axis360.sample_data("audiobook_metadata.json")
        axis360.api.queue_response(200, {}, metadata)

        # Setup.
        edition, pool = axis360.db.edition(with_license_pool=True)
        identifier = pool.identifier
        fulfillment = Axis360Fulfillment(
            axis360.api,
            pool.data_source.name,
            identifier.type,
            identifier.identifier,
            "transaction_id",
        )
        assert fulfillment.content_type is None
        assert fulfillment.content is None

        # Turn the crank.
        fulfillment.response()

        # The Axis360Fulfillment now contains a Findaway manifest
        # document.
        assert fulfillment.content_type == DeliveryMechanism.FINDAWAY_DRM
        assert fulfillment.content is not None
        assert isinstance(fulfillment.content, str)

        # The manifest document combines information from the
        # fulfillment document and the metadata document.
        for required in (
            '"findaway:sessionKey": "0f547af1-38c1-4b1c-8a1a-169d353065d0"',
            '"duration": 8150.87',
        ):
            assert required in fulfillment.content

    def test_fetch_ebook(self, axis360: Axis360Fixture):
        # When no Findaway information is present in the response from
        # the fulfillment API, information from the request is
        # used to make an AxisNow fulfillment document.

        fulfillment_info = axis360.sample_data("ebook_fulfillment_info.json")
        axis360.api.queue_response(200, {}, fulfillment_info)

        # Setup.
        edition, pool = axis360.db.edition(with_license_pool=True)
        identifier = pool.identifier
        fulfillment = Axis360Fulfillment(
            axis360.api,
            pool.data_source.name,
            identifier.type,
            identifier.identifier,
            "transaction_id",
        )
        assert fulfillment.content_type is None
        assert fulfillment.content is None

        # Turn the crank.
        fulfillment.response()

        # The Axis360Fulfillment now contains an AxisNow manifest
        # document derived from the fulfillment document.
        assert fulfillment.content_type == DeliveryMechanism.AXISNOW_DRM
        assert (
            fulfillment.content
            == '{"book_vault_uuid": "1c11c31f-81c2-41bb-9179-491114c3f121", "isbn": "9780547351551"}'
        )


class TestAxisNowManifest:
    """Test the simple data format used to communicate an entry point into
    AxisNow."""

    def test_unicode(self):
        manifest = AxisNowManifest("A UUID", "An ISBN")
        assert '{"book_vault_uuid": "A UUID", "isbn": "An ISBN"}' == str(manifest)
        assert DeliveryMechanism.AXISNOW_DRM == manifest.MEDIA_TYPE


class Axis360ProviderFixture(Axis360Fixture):
    def __init__(self, db: DatabaseTransactionFixture, files: AxisFilesFixture):
        super().__init__(db, files)
        mock_api = MockAxis360API(db.session, self.collection)
        self.api = mock_api


@pytest.fixture(scope="function")
def axis360provider(
    db: DatabaseTransactionFixture, axis_files_fixture: AxisFilesFixture
) -> Axis360ProviderFixture:
    return Axis360ProviderFixture(db, axis_files_fixture)


class Axis360AcsFulfillmentFixture:
    def __init__(self, mock_urlopen: MagicMock):
        self.fulfillment_info = partial(
            Axis360AcsFulfillment,
            content_link="https://fake.url",
            verify=False,
        )
        self.mock_request = self.create_mock_request()
        self.mock_urlopen = mock_urlopen
        self.mock_urlopen.return_value = self.mock_request

    @staticmethod
    def create_mock_request() -> MagicMock:
        # Create a mock request object that we can use in the tests
        response = MagicMock(return_value="")
        type(response).headers = PropertyMock(return_value=[])
        type(response).status = PropertyMock(return_value=200)
        mock_request = MagicMock()
        mock_request.__enter__.return_value = response
        mock_request.__exit__.return_value = None
        return mock_request

    @classmethod
    @contextmanager
    def fixture(self):
        with patch("urllib.request.urlopen") as mock_urlopen:
            yield Axis360AcsFulfillmentFixture(mock_urlopen)


@pytest.fixture
def axis360_acs_fulfillment_fixture():
    with Axis360AcsFulfillmentFixture.fixture() as fixture:
        yield fixture


class TestAxis360AcsFulfillment:
    def test_url_encoding_not_capitalized(
        self, axis360_acs_fulfillment_fixture: Axis360AcsFulfillmentFixture
    ):
        # Mock the urllopen function to make sure that the URL is not actually requested
        # then make sure that when the request is built the %3a character encoded in the
        # string is not uppercased to be %3A.

        fulfillment = axis360_acs_fulfillment_fixture.fulfillment_info(
            content_link="https://test.com/?param=%3atest123"
        )
        response = fulfillment.response()
        axis360_acs_fulfillment_fixture.mock_urlopen.assert_called()
        called_url = axis360_acs_fulfillment_fixture.mock_urlopen.call_args[0][0]
        assert called_url is not None
        assert called_url.selector == "/?param=%3atest123"
        assert called_url.host == "test.com"
        assert type(response) == Response
        mock_request = axis360_acs_fulfillment_fixture.mock_request
        mock_request.__enter__.assert_called()
        mock_request.__enter__.return_value.read.assert_called()
        assert "status" in dir(mock_request.__enter__.return_value)
        assert "headers" in dir(mock_request.__enter__.return_value)
        mock_request.__exit__.assert_called()

    @pytest.mark.parametrize(
        "exception",
        [
            urllib.error.HTTPError(url="", code=301, msg="", hdrs={}, fp=Mock()),  # type: ignore
            socket.timeout(),
            urllib.error.URLError(reason=""),
            ssl.SSLError(),
        ],
        ids=lambda val: val.__class__.__name__,
    )
    def test_exception_raises_problem_detail_exception(
        self,
        axis360_acs_fulfillment_fixture: Axis360AcsFulfillmentFixture,
        exception: Exception,
    ):
        # Check that when the urlopen function throws an exception, we catch the exception and
        # we turn it into a problem detail to be returned to the client. This mimics the behavior
        # of the http utils function that we are bypassing with this fulfillment method.
        axis360_acs_fulfillment_fixture.mock_urlopen.side_effect = exception
        fulfillment = axis360_acs_fulfillment_fixture.fulfillment_info()
        with pytest.raises(BaseProblemDetailException):
            fulfillment.response()

    @pytest.mark.parametrize(
        ("verify", "verify_mode", "check_hostname"),
        [(True, ssl.CERT_REQUIRED, True), (False, ssl.CERT_NONE, False)],
    )
    def test_verify_ssl(
        self,
        axis360_acs_fulfillment_fixture: Axis360AcsFulfillmentFixture,
        verify: bool,
        verify_mode: ssl.VerifyMode,
        check_hostname: bool,
    ):
        # Make sure that when the verify parameter of the fulfillment method is set we use the
        # correct SSL context to either verify or not verify the ssl certificate for the
        # URL we are fetching.
        fulfillment = axis360_acs_fulfillment_fixture.fulfillment_info(verify=verify)
        fulfillment.response()
        axis360_acs_fulfillment_fixture.mock_urlopen.assert_called()
        assert "context" in axis360_acs_fulfillment_fixture.mock_urlopen.call_args[1]
        context = axis360_acs_fulfillment_fixture.mock_urlopen.call_args[1]["context"]
        assert context.verify_mode == verify_mode
        assert context.check_hostname == check_hostname
