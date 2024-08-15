from __future__ import annotations

import datetime
import json
from typing import cast
from unittest.mock import MagicMock

import pytest

from palace.manager.api.circulation import FulfillmentInfo, LoanInfo
from palace.manager.api.circulation_exceptions import (
    NoAvailableCopies,
    PatronAuthorizationFailedException,
    RemoteInitiatedServerError,
)
from palace.manager.api.enki import (
    BibliographicParser,
    EnkiAPI,
    EnkiCollectionReaper,
    EnkiImport,
)
from palace.manager.core.metadata_layer import CirculationData, Metadata, TimestampData
from palace.manager.service.analytics.analytics import Analytics
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism, LicensePool
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.datetime_helpers import datetime_utc, utc_now
from palace.manager.util.http import RemoteIntegrationException, RequestTimedOut
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import FilesFixture
from tests.mocks.enki import MockEnkiAPI
from tests.mocks.mock import MockRequestsResponse


class EnkiFilesFixture(FilesFixture):
    """A fixture providing access to Enki files."""

    def __init__(self):
        super().__init__("enki")


@pytest.fixture()
def enki_files_fixture() -> EnkiFilesFixture:
    """A fixture providing access to Enki files."""
    return EnkiFilesFixture()


class EnkiTestFixure:
    def __init__(self, db: DatabaseTransactionFixture, files: EnkiFilesFixture):
        self.db = db
        self.files = files
        self.api = MockEnkiAPI(db.session, db.default_library())
        collection = self.api.collection
        assert collection is not None
        self.collection = collection


@pytest.fixture(scope="function")
def enki_test_fixture(
    db: DatabaseTransactionFixture, enki_files_fixture: EnkiFilesFixture
) -> EnkiTestFixure:
    return EnkiTestFixure(db, enki_files_fixture)


class TestEnkiAPI:
    def test_constructor(self, enki_test_fixture: EnkiTestFixure):
        db = enki_test_fixture.db
        # The constructor must be given an Enki collection.
        collection = db.collection(protocol="Overdrive", url="http://test.enki.url")
        with pytest.raises(ValueError) as excinfo:
            EnkiAPI(db.session, collection)
        assert "Collection protocol is Overdrive, but passed into EnkiAPI!" in str(
            excinfo.value
        )

        collection.protocol = EnkiAPI.label()
        EnkiAPI(db.session, collection)

    def test_enki_library_id(self, enki_test_fixture: EnkiTestFixure):
        db = enki_test_fixture.db
        # The default library has already had this value set on its
        # association with the mock Enki collection.
        m = enki_test_fixture.api.enki_library_id
        assert "c" == m(db.default_library())

        # Associate another library with the mock Enki collection
        # and set its Enki library ID.
        other_library = db.library()
        assert other_library.id is not None
        config = enki_test_fixture.api.integration_configuration()
        assert config is not None

        config.libraries.append(other_library)
        lib_config = config.for_library(other_library)
        assert lib_config is not None
        DatabaseTransactionFixture.set_settings(
            lib_config,
            **{enki_test_fixture.api.ENKI_LIBRARY_ID_KEY: "other library id"},
        )
        db.session.commit()
        assert "other library id" == m(other_library)

    def test_collection(self, enki_test_fixture: EnkiTestFixure):
        assert enki_test_fixture.collection == enki_test_fixture.api.collection

    def test__run_self_tests(
        self,
        enki_test_fixture: EnkiTestFixure,
    ):
        db = enki_test_fixture.db

        # Mock every method that will be called by the self-test.
        class Mock(MockEnkiAPI):
            def recent_activity(self, start, end):
                self.recent_activity_called_with = (start, end)
                yield 1
                yield 2

            def updated_titles(self, since):
                self.updated_titles_called_with = since
                yield 1
                yield 2
                yield 3

            patron_activity_called_with = []

            def patron_activity(self, patron, pin):
                self.patron_activity_called_with.append((patron, pin))
                yield 1

        api = Mock(db.session, db.default_library())

        # Now let's make sure two Libraries have access to the
        # Collection used in the API -- one library with a default
        # patron and one without.
        no_default_patron = db.library()
        assert api.collection is not None
        api.collection.libraries.append(no_default_patron)

        with_default_patron = db.default_library()
        db.simple_auth_integration(with_default_patron)

        # Now that everything is set up, run the self-test.
        (
            no_patron_activity,
            default_patron_activity,
            circulation_changes,
            collection_changes,
        ) = sorted(api._run_self_tests(db.session), key=lambda x: str(x.name))

        # Verify that each test method was called and returned the
        # expected SelfTestResult object.
        now = utc_now()
        one_hour_ago = now - datetime.timedelta(hours=1)
        one_day_ago = now - datetime.timedelta(hours=24)
        start, end = api.recent_activity_called_with
        assert (start - one_hour_ago).total_seconds() < 2
        assert (end - now).total_seconds() < 2
        assert True == circulation_changes.success
        assert "2 circulation events in the last hour" == circulation_changes.result

        assert (api.updated_titles_called_with - one_day_ago).total_seconds() < 2
        assert True == collection_changes.success
        assert "3 titles added/updated in the last day" == collection_changes.result

        assert (
            "Acquiring test patron credentials for library %s" % no_default_patron.name
            == no_patron_activity.name
        )
        assert False == no_patron_activity.success
        assert "Library has no test patron configured." == str(
            no_patron_activity.exception
        )

        assert (
            "Checking patron activity, using test patron for library %s"
            % with_default_patron.name
            == default_patron_activity.name
        )
        assert True == default_patron_activity.success
        assert "Total loans and holds: 1" == default_patron_activity.result

    def test_request_retried_once(self, enki_test_fixture: EnkiTestFixure):
        """A request that times out is retried."""

        db = enki_test_fixture.db

        class TimesOutOnce(EnkiAPI):
            timeout = True
            called_with = []

            def _request(self, *args, **kwargs):
                self.called_with.append((args, kwargs))
                if self.timeout:
                    self.timeout = False
                    raise RequestTimedOut("url", "timeout")
                else:
                    return MockRequestsResponse(200, content="content")

        api = TimesOutOnce(db.session, enki_test_fixture.collection)
        response = api.request("url")

        # Two identical requests were made.
        r1, r2 = api.called_with
        assert r1 == r2

        # In the end, we got our content.
        assert 200 == response.status_code
        assert b"content" == response.content

    def test_request_retried_only_once(self, enki_test_fixture: EnkiTestFixure):
        """A request that times out twice is not retried."""

        db = enki_test_fixture.db

        class TimesOut(EnkiAPI):
            calls = 0

            def _request(self, *args, **kwargs):
                self.calls += 1
                raise RequestTimedOut("url", "timeout")

        api = TimesOut(db.session, enki_test_fixture.collection)
        pytest.raises(RequestTimedOut, api.request, "url")

        # Only two requests were made.
        assert 2 == api.calls

    def test_request_error_indicator(self, enki_test_fixture: EnkiTestFixure):
        db = enki_test_fixture.db

        # A response that looks like Enki's HTML error message is
        # turned into a RemoteIntegrationException.
        class Oops(EnkiAPI):
            timeout = True
            called_with = []

            def _request(self, *args, **kwargs):
                self.called_with.append((args, kwargs))
                return MockRequestsResponse(
                    200,
                    content="<html><title>oh no</title><body>%s</body>"
                    % (EnkiAPI.ERROR_INDICATOR),
                )

        api = Oops(db.session, enki_test_fixture.collection)
        with pytest.raises(RemoteIntegrationException) as excinfo:
            api.request("url")
        assert "An unknown error occured" in str(excinfo.value)

    def test__minutes_since(self, enki_test_fixture: EnkiTestFixure):
        """Test the _minutes_since helper method."""
        an_hour_ago = utc_now() - datetime.timedelta(minutes=60)
        assert 60 == EnkiAPI._minutes_since(an_hour_ago)

    def test_recent_activity(self, enki_test_fixture: EnkiTestFixure):
        db = enki_test_fixture.db
        now = utc_now()
        epoch = datetime_utc(1970, 1, 1)
        epoch_plus_one_hour = epoch + datetime.timedelta(hours=1)
        data = enki_test_fixture.files.sample_data("get_recent_activity.json")
        enki_test_fixture.api.queue_response(200, content=data)
        activity = list(
            enki_test_fixture.api.recent_activity(epoch, epoch_plus_one_hour)
        )
        assert 43 == len(activity)
        for i in activity:
            assert isinstance(i, CirculationData)
        [
            method,
            url,
            headers,
            data,
            params,
            kwargs,
        ] = enki_test_fixture.api.requests.pop()
        assert "get" == method
        assert "https://enkilibrary.org/API/ItemAPI" == url
        assert "getRecentActivityTime" == params["method"]
        assert "0" == params["stime"]
        assert "3600" == params["etime"]

        # Unlike some API calls, it's not necessary to pass 'lib' in here.
        assert "lib" not in params

    def test_updated_titles(self, enki_test_fixture: EnkiTestFixure):
        db = enki_test_fixture.db
        one_minute_ago = utc_now() - datetime.timedelta(minutes=1)
        data = enki_test_fixture.files.sample_data("get_update_titles.json")
        enki_test_fixture.api.queue_response(200, content=data)
        activity = list(enki_test_fixture.api.updated_titles(since=one_minute_ago))

        assert 6 == len(activity)
        for i in activity:
            assert isinstance(i, Metadata)

        assert "Nervous System" == activity[0].title
        assert 1 == activity[0].circulation.licenses_owned

        [
            method,
            url,
            headers,
            data,
            params,
            kwargs,
        ] = enki_test_fixture.api.requests.pop()
        assert "get" == method
        assert "https://enkilibrary.org/API/ListAPI" == url
        assert "getUpdateTitles" == params["method"]
        assert 1 == params["minutes"]

        # The Enki library ID is a known 'safe' value since we're not acting
        # in the context of any particular library here.
        assert "0" == params["lib"]

    def test_get_item(self, enki_test_fixture: EnkiTestFixure):
        db = enki_test_fixture.db
        data = enki_test_fixture.files.sample_data("get_item_french_title.json")
        enki_test_fixture.api.queue_response(200, content=data)
        metadata = enki_test_fixture.api.get_item("an id")

        # We get a Metadata with associated CirculationData.
        assert isinstance(metadata, Metadata)
        assert "Le But est le Seul Choix" == metadata.title
        assert 1 == metadata.circulation.licenses_owned

        [
            method,
            url,
            headers,
            data,
            params,
            kwargs,
        ] = enki_test_fixture.api.requests.pop()
        assert "get" == method
        assert "https://enkilibrary.org/API/ItemAPI" == url
        assert "an id" == params["recordid"]
        assert "getItem" == params["method"]

        # The Enki library ID is a known 'safe' value since we're not acting
        # in the context of any particular library here.
        assert "0" == params["lib"]

        # We asked for a large cover image in case this Metadata is
        # to be used to form a local picture of the book's metadata.
        assert "large" == params["size"]

    def test_get_item_not_found(self, enki_test_fixture: EnkiTestFixure):
        enki_test_fixture.api.queue_response(200, content="<html>No such book</html>")
        metadata = enki_test_fixture.api.get_item("an id")
        assert None == metadata

    def test_get_all_titles(self, enki_test_fixture: EnkiTestFixure):
        db = enki_test_fixture.db
        # get_all_titles and get_update_titles return data in the same
        # format, so we can use one to mock the other.
        data = enki_test_fixture.files.sample_data("get_update_titles.json")
        enki_test_fixture.api.queue_response(200, content=data)

        results = list(enki_test_fixture.api.get_all_titles())
        assert 6 == len(results)
        metadata = results[0]
        assert isinstance(metadata, Metadata)
        assert "Nervous System" == metadata.title
        assert 1 == metadata.circulation.licenses_owned

        [
            method,
            url,
            headers,
            data,
            params,
            kwargs,
        ] = enki_test_fixture.api.requests.pop()
        assert "get" == method
        assert "https://enkilibrary.org/API/ListAPI" == url
        assert "getAllTitles" == params["method"]
        assert "secontent" == params["id"]

        # Unlike some API calls, it's not necessary to pass 'lib' in here.
        assert "lib" not in params

    def test__epoch_to_struct(self, enki_test_fixture: EnkiTestFixure):
        """Test the _epoch_to_struct helper method."""
        assert datetime_utc(1970, 1, 1) == EnkiAPI._epoch_to_struct("0")

    def test_checkout_open_access_parser(self, enki_test_fixture: EnkiTestFixure):
        """Test that checkout info for non-ACS Enki books is parsed correctly."""
        data = enki_test_fixture.files.sample_data("checked_out_direct.json")
        result = json.loads(data)
        loan = enki_test_fixture.api.parse_patron_loans(
            result["result"]["checkedOutItems"][0]
        )
        assert loan.data_source_name == DataSource.ENKI
        assert loan.identifier_type == Identifier.ENKI_ID
        assert loan.identifier == "2"
        assert loan.start_date == datetime_utc(2017, 8, 23, 19, 31, 58, 0)
        assert loan.end_date == datetime_utc(2017, 9, 13, 19, 31, 58, 0)

    def test_checkout_acs_parser(self, enki_test_fixture: EnkiTestFixure):
        """Test that checkout info for ACS Enki books is parsed correctly."""
        data = enki_test_fixture.files.sample_data("checked_out_acs.json")
        result = json.loads(data)
        loan = enki_test_fixture.api.parse_patron_loans(
            result["result"]["checkedOutItems"][0]
        )
        assert loan.data_source_name == DataSource.ENKI
        assert loan.identifier_type == Identifier.ENKI_ID
        assert loan.identifier == "3334"
        assert loan.start_date == datetime_utc(2017, 8, 23, 19, 42, 35, 0)
        assert loan.end_date == datetime_utc(2017, 9, 13, 19, 42, 35, 0)

    def test_checkout_success(self, enki_test_fixture: EnkiTestFixure):
        db = enki_test_fixture.db
        # Test the checkout() method.
        patron = db.patron()
        patron.authorization_identifier = "123"
        pool = db.licensepool(None)

        data = enki_test_fixture.files.sample_data("checked_out_acs.json")
        enki_test_fixture.api.queue_response(200, content=data)
        loan = enki_test_fixture.api.checkout(patron, "pin", pool, MagicMock())

        # An appropriate request to the "getSELink" endpoint was made.,
        [
            method,
            url,
            headers,
            data,
            params,
            kwargs,
        ] = enki_test_fixture.api.requests.pop()
        assert "get" == method
        assert enki_test_fixture.api.base_url + "UserAPI" == url
        assert "getSELink" == params["method"]
        assert "123" == params["username"]
        assert "pin" == params["password"]

        # In particular, the Enki library ID associated with the
        # patron's library was used as the 'lib' parameter.
        assert "c" == params["lib"]

        # A LoanInfo for the loan was returned.
        assert isinstance(loan, LoanInfo)
        assert loan.identifier == pool.identifier.identifier
        assert loan.collection_id == pool.collection.id
        assert loan.start_date == None
        assert loan.end_date == datetime_utc(2017, 9, 13, 19, 42, 35, 0)

    def test_checkout_bad_authorization(self, enki_test_fixture: EnkiTestFixure):
        """Test that the correct exception is thrown upon an unsuccessful login."""
        db = enki_test_fixture.db
        with pytest.raises(PatronAuthorizationFailedException):
            data = enki_test_fixture.files.sample_data("login_unsuccessful.json")
            enki_test_fixture.api.queue_response(200, content=data)
            result = json.loads(data)

            edition, pool = db.edition(
                identifier_type=Identifier.ENKI_ID,
                data_source_name=DataSource.ENKI,
                with_license_pool=True,
            )
            pool.identifier.identifier = "notanid"

            patron = db.patron(external_identifier="notabarcode")

            loan = enki_test_fixture.api.checkout(patron, "notapin", pool, MagicMock())

    def test_checkout_not_available(self, enki_test_fixture: EnkiTestFixure):
        """Test that the correct exception is thrown upon an unsuccessful login."""
        db = enki_test_fixture.db
        with pytest.raises(NoAvailableCopies):
            data = enki_test_fixture.files.sample_data("no_copies.json")
            enki_test_fixture.api.queue_response(200, content=data)
            result = json.loads(data)

            edition, pool = db.edition(
                identifier_type=Identifier.ENKI_ID,
                data_source_name=DataSource.ENKI,
                with_license_pool=True,
            )
            pool.identifier.identifier = "econtentRecord1"
            patron = db.patron(external_identifier="12345678901234")

            loan = enki_test_fixture.api.checkout(patron, "1234", pool, MagicMock())

    def test_fulfillment_open_access_parser(self, enki_test_fixture: EnkiTestFixure):
        """Test that fulfillment info for non-ACS Enki books is parsed correctly."""
        data = enki_test_fixture.files.sample_data("checked_out_direct.json")
        result = json.loads(data)
        fulfill_data = enki_test_fixture.api.parse_fulfill_result(result["result"])
        assert (
            fulfill_data[0]
            == """http://cccl.enkilibrary.org/API/UserAPI?method=downloadEContentFile&username=21901000008080&password=deng&lib=1&recordId=2"""
        )
        assert fulfill_data[1] == "epub"

    def test_fulfillment_acs_parser(self, enki_test_fixture: EnkiTestFixure):
        """Test that fulfillment info for ACS Enki books is parsed correctly."""
        data = enki_test_fixture.files.sample_data("checked_out_acs.json")
        result = json.loads(data)
        fulfill_data = enki_test_fixture.api.parse_fulfill_result(result["result"])
        assert (
            fulfill_data[0]
            == """http://afs.enkilibrary.org/fulfillment/URLLink.acsm?action=enterloan&ordersource=Califa&orderid=ACS4-9243146841581187248119581&resid=urn%3Auuid%3Ad5f54da9-8177-43de-a53d-ef521bc113b4&gbauthdate=Wed%2C+23+Aug+2017+19%3A42%3A35+%2B0000&dateval=1503517355&rights=%24lat%231505331755%24&gblver=4&auth=8604f0fc3f014365ea8d3c4198c721ed7ed2c16d"""
        )
        assert fulfill_data[1] == "epub"

    def test_fulfill_success(self, enki_test_fixture: EnkiTestFixure):
        db = enki_test_fixture.db
        # Test the fulfill() method.
        patron = db.patron()
        patron.authorization_identifier = "123"
        pool = db.licensepool(None)

        data = enki_test_fixture.files.sample_data("checked_out_acs.json")
        enki_test_fixture.api.queue_response(200, content=data)
        fulfillment = enki_test_fixture.api.fulfill(patron, "pin", pool, MagicMock())

        # An appropriate request to the "getSELink" endpoint was made.,
        [
            method,
            url,
            headers,
            data,
            params,
            kwargs,
        ] = enki_test_fixture.api.requests.pop()
        assert "get" == method
        assert enki_test_fixture.api.base_url + "UserAPI" == url
        assert "getSELink" == params["method"]
        assert "123" == params["username"]
        assert "pin" == params["password"]

        # In particular, the Enki library ID associated with the
        # patron's library was used as the 'lib' parameter.
        assert "c" == params["lib"]

        # A FulfillmentInfo for the loan was returned.
        assert isinstance(fulfillment, FulfillmentInfo)
        assert fulfillment.identifier == pool.identifier.identifier
        assert fulfillment.collection_id == pool.collection.id
        assert DeliveryMechanism.ADOBE_DRM == fulfillment.content_type
        assert fulfillment.content_link is not None
        assert fulfillment.content_link.startswith(
            "http://afs.enkilibrary.org/fulfillment/URLLink.acsm"
        )
        assert fulfillment.content_expires == datetime_utc(2017, 9, 13, 19, 42, 35, 0)

    def test_patron_activity(self, enki_test_fixture: EnkiTestFixure):
        db = enki_test_fixture.db
        data = enki_test_fixture.files.sample_data("patron_response.json")
        enki_test_fixture.api.queue_response(200, content=data)
        patron = db.patron()
        patron.authorization_identifier = "123"
        [loan] = enki_test_fixture.api.patron_activity(patron, "pin")

        # An appropriate Enki API call was issued.
        [
            method,
            url,
            headers,
            data,
            params,
            kwargs,
        ] = enki_test_fixture.api.requests.pop()
        assert "get" == method
        assert enki_test_fixture.api.base_url + "UserAPI" == url
        assert "getSEPatronData" == params["method"]
        assert "123" == params["username"]
        assert "pin" == params["password"]

        # In particular, the Enki library ID associated with the
        # patron's library was used as the 'lib' parameter.
        assert "c" == params["lib"]

        # The result is a single LoanInfo.
        assert isinstance(loan, LoanInfo)
        assert Identifier.ENKI_ID == loan.identifier_type
        assert DataSource.ENKI == loan.data_source_name
        assert "231" == loan.identifier
        assert enki_test_fixture.collection == loan.collection(db.session)
        assert datetime_utc(2017, 8, 15, 14, 56, 51) == loan.start_date
        assert datetime_utc(2017, 9, 5, 14, 56, 51) == loan.end_date

    def test_patron_activity_failure(self, enki_test_fixture: EnkiTestFixure):
        db = enki_test_fixture.db
        patron = db.patron()
        enki_test_fixture.api.queue_response(404, content="No such patron")
        collect = lambda: list(enki_test_fixture.api.patron_activity(patron, "pin"))
        pytest.raises(PatronAuthorizationFailedException, collect)

        msg = dict(result=dict(message="Login unsuccessful."))
        enki_test_fixture.api.queue_response(200, content=json.dumps(msg))
        pytest.raises(PatronAuthorizationFailedException, collect)

        msg = dict(result=dict(message="Some other error."))
        enki_test_fixture.api.queue_response(200, content=json.dumps(msg))
        pytest.raises(RemoteInitiatedServerError, collect)


class TestBibliographicParser:
    def test_process_all(self, enki_test_fixture: EnkiTestFixure):
        class Mock(BibliographicParser):
            inputs = []

            def extract_bibliographic(self, element):
                self.inputs.append(element)

        parser = Mock()

        def consume(*args):
            """Consume a generator's output."""
            list(parser.process_all(*args))

        # First try various inputs that run successfully but don't
        # extract any data.
        consume("{}")
        assert [] == parser.inputs

        consume(dict(result=dict()))
        assert [] == parser.inputs

        consume(dict(result=dict(titles=[])))
        assert [] == parser.inputs

        # Now try a list of books that is split up and each book
        # processed separately.
        data = enki_test_fixture.files.sample_data("get_update_titles.json")
        consume(data)
        assert 6 == len(parser.inputs)

    def test_extract_bibliographic(self, enki_test_fixture: EnkiTestFixure):
        """Test the ability to turn an individual book data blob
        into a Metadata.
        """
        data = json.loads(
            enki_test_fixture.files.sample_data("get_item_french_title.json")
        )
        parser = BibliographicParser()
        m = parser.extract_bibliographic(data["result"])
        assert isinstance(m, Metadata)

        assert "Le But est le Seul Choix" == m.title
        assert "fre" == m.language
        assert "Law of Time Press" == m.publisher

        # Two identifiers, Enki and ISBN, with Enki being primary.
        enki, isbn = sorted(m.identifiers, key=lambda x: x.type)
        assert Identifier.ENKI_ID == enki.type
        assert "21135" == enki.identifier
        assert enki == m.primary_identifier

        assert Identifier.ISBN == isbn.type
        assert "9780988432727" == isbn.identifier

        # One contributor
        [contributor] = m.contributors
        assert "Hoffmeister, David" == contributor.sort_name
        assert [Contributor.Role.AUTHOR] == contributor.roles

        # Two links -- full-sized image and description.
        image, description = sorted(m.links, key=lambda x: x.rel)
        assert Hyperlink.IMAGE == image.rel
        assert (
            "https://enkilibrary.org/bookcover.php?id=21135&isbn=9780988432727&category=EMedia&size=large"
            == image.href
        )

        assert Hyperlink.DESCRIPTION == description.rel
        assert "text/html" == description.media_type
        assert description.content.startswith("David Hoffmeister r&eacute;")

        # The full-sized image has a thumbnail.
        assert Hyperlink.THUMBNAIL_IMAGE == image.thumbnail.rel
        assert "http://thumbnail/" == image.thumbnail.href

        # Four subjects.
        subjects = sorted(m.subjects, key=lambda x: x.identifier)

        # All subjects are classified as tags, rather than BISAC, due
        # to inconsistencies in the data presentation.
        for i in subjects:
            assert i.type == Subject.TAG
            assert None == i.name
        assert [
            "BODY MIND SPIRIT Spirituality General",
            "BODY, MIND & SPIRIT / Spirituality / General.",
            "Spirituality",
            "Spirituality.",
        ] == [x.identifier for x in subjects]

        # We also have information about the current availability.
        circulation = m.circulation
        assert isinstance(circulation, CirculationData)
        assert 1 == circulation.licenses_owned
        assert 1 == circulation.licenses_available
        assert 0 == circulation.licenses_reserved
        assert 0 == circulation.patrons_in_hold_queue

        # The book is available as an ACS-encrypted EPUB.
        [format] = circulation.formats
        assert Representation.EPUB_MEDIA_TYPE == format.content_type
        assert DeliveryMechanism.ADOBE_DRM == format.drm_scheme

    def test_extract_bibliographic_pdf(self, enki_test_fixture: EnkiTestFixure):
        """Test the ability to distingush between PDF and EPUB results"""
        data = json.loads(
            enki_test_fixture.files.sample_data("pdf_document_entry.json")
        )
        parser = BibliographicParser()
        m = parser.extract_bibliographic(data["result"])
        assert isinstance(m, Metadata)

        # The book is available as a non-ACS PDF.
        circulation = m.circulation
        assert isinstance(circulation, CirculationData)
        [format] = circulation.formats
        assert Representation.PDF_MEDIA_TYPE == format.content_type
        assert DeliveryMechanism.NO_DRM == format.drm_scheme


class TestEnkiImport:
    def test_import_instantiation(self, enki_test_fixture: EnkiTestFixure):
        """Test that EnkiImport can be instantiated"""
        db = enki_test_fixture.db
        importer = EnkiImport(
            db.session, enki_test_fixture.collection, api_class=enki_test_fixture.api
        )
        assert enki_test_fixture.api == importer.api
        assert enki_test_fixture.collection == importer.collection

    def test_run_once(self, enki_test_fixture: EnkiTestFixure):
        db = enki_test_fixture.db
        dummy_value = object()

        class Mock(EnkiImport):
            incremental_import_called_with = dummy_value

            def full_import(self):
                self.full_import_called = True
                return 10

            def incremental_import(self, since):
                self.incremental_import_called_with = since
                return 4, 7

        importer = Mock(
            db.session, enki_test_fixture.collection, api_class=enki_test_fixture.api
        )

        # If the incoming TimestampData makes it look like the process
        # has never successfully completed, full_import() is called.
        progress = TimestampData(start=None)
        importer.run_once(progress)
        assert True == importer.full_import_called
        assert (
            "New or modified titles: 10. Titles with circulation changes: 0."
            == progress.achievements
        )

        # It doesn't call incremental_import().
        assert dummy_value == importer.incremental_import_called_with

        # If run_once() is called with a TimestampData that indicates
        # an earlier successful run, a time five minutes before the
        # previous completion time is passed into incremental_import()
        importer.full_import_called = False

        a_while_ago = datetime_utc(2011, 1, 1)
        even_earlier = a_while_ago - datetime.timedelta(days=100)
        timestamp = TimestampData(start=even_earlier, finish=a_while_ago)
        new_timestamp = importer.run_once(timestamp)

        passed_in = importer.incremental_import_called_with
        expect = a_while_ago - importer.OVERLAP
        assert abs((passed_in - expect).total_seconds()) < 2  # type: ignore[operator]

        # full_import was not called.
        assert False == importer.full_import_called

        # The proposed new TimestampData covers the entire timespan
        # from the 'expect' period to now.
        assert expect == new_timestamp.start
        now = utc_now()
        assert (now - new_timestamp.finish).total_seconds() < 2
        assert (
            "New or modified titles: 4. Titles with circulation changes: 7."
            == new_timestamp.achievements
        )

    def test_full_import(self, enki_test_fixture: EnkiTestFixure):
        """full_import calls get_all_titles over and over again until
        it returns nothing, and processes every book it receives.
        """
        db = enki_test_fixture.db

        class MockAPI:
            def __init__(self, pages):
                """Act like an Enki API with predefined pages of results."""
                self.pages = pages
                self.get_all_titles_called_with = []

            def get_all_titles(self, strt, qty):
                self.get_all_titles_called_with.append((strt, qty))
                if self.pages:
                    return self.pages.pop(0)
                return []

        class Mock(EnkiImport):
            processed = []

            def process_book(self, data):
                self.processed.append(data)

        # Simulate an Enki site with two pages of results.
        pages = [[1, 2], [3]]
        api = MockAPI(pages)

        # Do the 'import'.
        importer = Mock(
            db.session, enki_test_fixture.collection, api_class=cast(EnkiAPI, api)
        )
        importer.full_import()

        # get_all_titles was called three times, once for the first two
        # pages and a third time to verify that there are no more results.
        assert [(0, 10), (10, 10), (20, 10)] == api.get_all_titles_called_with

        # Every item on every 'page' of results was processed.
        assert [1, 2, 3] == importer.processed

    def test_incremental_import(self, enki_test_fixture: EnkiTestFixure):
        """incremental_import calls process_book() on the output of
        EnkiAPI.updated_titles(), and then calls update_circulation().
        """
        db = enki_test_fixture.db

        class MockAPI:
            def updated_titles(self, since):
                self.updated_titles_called_with = since
                yield 1
                yield 2

        class Mock(EnkiImport):
            processed = []

            def process_book(self, data):
                self.processed.append(data)

            def update_circulation(self, since):
                self.update_circulation_called_with = since

        api = MockAPI()
        importer = Mock(
            db.session, enki_test_fixture.collection, api_class=cast(EnkiAPI, api)
        )
        since = MagicMock()
        importer.incremental_import(since)

        # The 'since' value was passed into both methods.
        assert since == api.updated_titles_called_with
        assert since == importer.update_circulation_called_with

        # The two items yielded by updated_titles() were run
        # through process_book().
        assert [1, 2] == importer.processed

    def test_update_circulation(self, enki_test_fixture: EnkiTestFixure):
        db = enki_test_fixture.db

        # update_circulation() makes two-hour slices out of time
        # between the previous run and now, and passes each slice into
        # _update_circulation, keeping track of the total number of
        # circulation events encountered.
        class Mock(EnkiImport):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self._update_circulation_called_with = []
                self.sizes = [1, 2]

            def _update_circulation(self, start, end):
                # Pretend that one circulation event was discovered
                # during the given time span.
                self._update_circulation_called_with.append((start, end))
                return self.sizes.pop()

        # Call update_circulation() on a time three hours in the
        # past. It will return a count of 3 -- the sum of the return
        # values from our mocked _update_circulation().
        now = utc_now()
        one_hour_ago = now - datetime.timedelta(hours=1)
        three_hours_ago = now - datetime.timedelta(hours=3)
        mock_api = MockEnkiAPI(
            db.session,
            enki_test_fixture.db.default_library(),
            enki_test_fixture.collection,
        )
        monitor = Mock(db.session, enki_test_fixture.collection, api_class=mock_api)
        assert 3 == monitor.update_circulation(three_hours_ago)

        # slice_timespan() sliced up the timeline into two-hour
        # chunks. It yielded up two chunks: "three hours ago" to "one
        # hour ago" and "one hour ago" to "now".
        #
        # _update_circulation() was called on each chunk, and in each
        # case the return value was an item popped from monitor.sizes.
        chunk1, chunk2 = monitor._update_circulation_called_with
        assert (three_hours_ago, one_hour_ago) == chunk1
        assert one_hour_ago == chunk2[0]
        assert (chunk2[1] - now).total_seconds() < 2

        # our mocked 'sizes' list is now empty.
        assert [] == monitor.sizes

    def test__update_circulation(self, enki_test_fixture: EnkiTestFixure):
        db = enki_test_fixture.db
        # Here's information about a book we didn't know about before.
        circ_data = {
            "result": {
                "records": 1,
                "recentactivity": [
                    {
                        "historyid": "3738",
                        "id": "34278",
                        "recordId": "econtentRecord34278",
                        "time": "2018-06-26 10:08:23",
                        "action": "Checked Out",
                        "isbn": "9781618856050",
                        "availability": {
                            "accessType": "acs",
                            "totalCopies": "1",
                            "availableCopies": 0,
                            "onHold": 0,
                        },
                    }
                ],
            }
        }

        # Because the book is unknown, update_circulation will do a follow-up
        # call to api.get_item to get bibliographic information.
        bib_data = {
            "result": {
                "id": "34278",
                "recordId": "econtentRecord34278",
                "isbn": "9781618856050",
                "title": "A book",
                "availability": {
                    "accessType": "acs",
                    "totalCopies": "1",
                    "availableCopies": 0,
                    "onHold": 0,
                },
            }
        }

        api = MockEnkiAPI(db.session, db.default_library())
        api.queue_response(200, content=json.dumps(circ_data))
        api.queue_response(200, content=json.dumps(bib_data))

        from tests.mocks.analytics_provider import MockAnalyticsProvider

        analytics = MockAnalyticsProvider()
        monitor = EnkiImport(
            db.session,
            enki_test_fixture.collection,
            api_class=api,
            analytics=cast(Analytics, analytics),
        )
        end = utc_now()

        # Ask for circulation events from one hour in 1970.
        start = datetime_utc(1970, 1, 1, 0, 0, 0)
        end = datetime_utc(1970, 1, 1, 1, 0, 0)
        monitor._update_circulation(start, end)

        # Two requests were made -- one to getRecentActivityTime
        # and one to getItem.
        [method, url, headers, data, params, kwargs] = api.requests.pop(0)
        assert "get" == method
        assert "https://enkilibrary.org/API/ItemAPI" == url
        assert "getRecentActivityTime" == params["method"]

        # The parameters passed to getRecentActivityTime show the
        # start and end points of the request as seconds since the
        # epoch.
        assert "0" == params["stime"]
        assert "3600" == params["etime"]

        [method, url, headers, data, params, kwargs] = api.requests.pop(0)
        assert "get" == method
        assert "https://enkilibrary.org/API/ItemAPI" == url
        assert "getItem" == params["method"]
        assert "34278" == params["recordid"]

        # We ended up with one Work, one LicensePool, and one Edition.
        work = db.session.query(Work).one()
        licensepool = db.session.query(LicensePool).one()
        edition = db.session.query(Edition).one()
        assert [licensepool] == work.license_pools
        assert edition == licensepool.presentation_edition

        identifier = licensepool.identifier
        assert Identifier.ENKI_ID == identifier.type
        assert "34278" == identifier.identifier

        # The LicensePool and Edition take their data from the mock API
        # requests.
        assert "A book" == work.title
        assert 1 == licensepool.licenses_owned
        assert 0 == licensepool.licenses_available

        # An analytics event was sent out for the newly discovered book.
        # No more DISTRIBUTOR events
        assert 0 == analytics.count

        # Now let's see what update_circulation does when the work
        # already exists.
        circ_data["result"]["recentactivity"][0]["availability"]["totalCopies"] = 10  # type: ignore
        api.queue_response(200, content=json.dumps(circ_data))
        # We're not queuing up more bib data, but that's no problem --
        # EnkiImport won't ask for it.

        # Pump the monitor again.
        monitor._update_circulation(start, end)

        # We made a single request, to getRecentActivityTime.
        [method, url, headers, data, params, kwargs] = api.requests.pop(0)
        assert "getRecentActivityTime" == params["method"]

        # The LicensePool was updated, but no new objects were created.
        assert 10 == licensepool.licenses_owned
        for c in (LicensePool, Edition, Work):
            assert 1 == db.session.query(c).count()

    def test_process_book(self):
        """This functionality is tested as part of
        test_update_circulation.
        """


class TestEnkiCollectionReaper:
    def test_book_that_doesnt_need_reaping_is_left_alone(
        self, enki_test_fixture: EnkiTestFixure
    ):
        db = enki_test_fixture.db
        # We're happy with this book.
        edition, pool = db.edition(
            identifier_type=Identifier.ENKI_ID,
            identifier_id="21135",
            data_source_name=DataSource.ENKI,
            with_license_pool=True,
            collection=enki_test_fixture.collection,
        )
        pool.licenses_owned = 10
        pool.licenses_available = 9
        pool.patrons_in_hold_queue = 5

        # Enki still considers this book to be in the library's
        # collection.
        data = enki_test_fixture.files.sample_data("get_item_french_title.json")
        enki_test_fixture.api.queue_response(200, content=data)

        reaper = EnkiCollectionReaper(
            db.session, enki_test_fixture.collection, api_class=enki_test_fixture.api
        )

        # Run the identifier through the reaper.
        reaper.process_item(pool.identifier)

        # The book was left alone.
        assert 10 == pool.licenses_owned
        assert 9 == pool.licenses_available
        assert 5 == pool.patrons_in_hold_queue

    def test_reaped_book_has_zero_licenses(self, enki_test_fixture: EnkiTestFixure):
        db = enki_test_fixture.db
        # Create a LicensePool that needs updating.
        edition, pool = db.edition(
            identifier_type=Identifier.ENKI_ID,
            data_source_name=DataSource.ENKI,
            with_license_pool=True,
            collection=enki_test_fixture.collection,
        )

        # This is a specific record ID that should never exist
        nonexistent_id = "econtentRecord0"

        # We have never checked the circulation information for this
        # LicensePool. Put some random junk in the pool to verify
        # that it gets changed.
        pool.licenses_owned = 10
        pool.licenses_available = 5
        pool.patrons_in_hold_queue = 3
        pool.identifier.identifier = nonexistent_id
        assert None == pool.last_checked

        # Enki will claim it doesn't know about this book
        # by sending an HTML error instead of JSON data.
        data = "<html></html>"
        enki_test_fixture.api.queue_response(200, content=data)

        reaper = EnkiCollectionReaper(
            db.session, enki_test_fixture.collection, api_class=enki_test_fixture.api
        )

        # Run the identifier through the reaper.
        reaper.process_item(pool.identifier)

        # The item's circulation information has been zeroed out.
        assert 0 == pool.licenses_owned
        assert 0 == pool.licenses_available
        assert 0 == pool.patrons_in_hold_queue
