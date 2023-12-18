import datetime
import urllib.parse
from decimal import Decimal
from unittest.mock import MagicMock, patch

import feedparser
import pytest
from flask import Response as FlaskResponse
from flask import url_for
from werkzeug import Response as wkResponse

from api.axis import Axis360API, Axis360FulfillmentInfo
from api.circulation import (
    BaseCirculationAPI,
    CirculationAPI,
    FulfillmentInfo,
    HoldInfo,
    LoanInfo,
)
from api.circulation_exceptions import (
    AlreadyOnHold,
    NoAvailableCopies,
    NoLicenses,
    NotFoundOnRemote,
    PatronHoldLimitReached,
)
from api.problem_details import (
    BAD_DELIVERY_MECHANISM,
    CANNOT_RELEASE_HOLD,
    HOLD_LIMIT_REACHED,
    NO_ACTIVE_LOAN,
    NOT_FOUND_ON_REMOTE,
    OUTSTANDING_FINES,
)
from core.model import (
    Collection,
    DataSource,
    DeliveryMechanism,
    ExternalIntegration,
    Hold,
    Identifier,
    LicensePool,
    LicensePoolDeliveryMechanism,
    Loan,
    MediaTypes,
    Representation,
    RightsStatus,
    Work,
    get_one,
    get_one_or_create,
)
from core.problem_details import INTEGRATION_ERROR, INVALID_INPUT
from core.util.datetime_helpers import datetime_utc, utc_now
from core.util.flask_util import Response
from core.util.http import RemoteIntegrationException
from core.util.opds_writer import OPDSFeed
from core.util.problem_detail import ProblemDetail
from tests.core.mock import DummyHTTPClient
from tests.fixtures.api_controller import CirculationControllerFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.library import LibraryFixture


class LoanFixture(CirculationControllerFixture):
    identifier: Identifier
    data_source: DataSource
    mech2: LicensePoolDeliveryMechanism
    mech1: LicensePoolDeliveryMechanism
    pool: LicensePool

    def __init__(self, db: DatabaseTransactionFixture):
        super().__init__(db)
        self.pool = self.english_1.license_pools[0]
        [self.mech1] = self.pool.delivery_mechanisms
        self.mech2 = self.pool.set_delivery_mechanism(
            Representation.PDF_MEDIA_TYPE,
            DeliveryMechanism.NO_DRM,
            RightsStatus.CC_BY,
            None,
        )
        self.edition = self.pool.presentation_edition
        self.data_source = self.edition.data_source
        self.identifier = self.edition.primary_identifier


@pytest.fixture(scope="function")
def loan_fixture(db: DatabaseTransactionFixture):
    return LoanFixture(db)


class TestLoanController:
    def test_can_fulfill_without_loan(self, loan_fixture: LoanFixture):
        """Test the circumstances under which a title can be fulfilled
        in the absence of an active loan for that title.
        """
        m = loan_fixture.manager.loans.can_fulfill_without_loan

        # If the library has a way of authenticating patrons (as the
        # default library does), then fulfilling a title always
        # requires an active loan.
        patron = object()
        pool = object()
        lpdm = object()
        assert False == m(loan_fixture.db.default_library(), patron, pool, lpdm)

        # If the library does not authenticate patrons, then this
        # _may_ be possible, but
        # CirculationAPI.can_fulfill_without_loan also has to say it's
        # okay.
        class MockLibraryAuthenticator:
            identifies_individuals = False

        short_name = loan_fixture.db.default_library().short_name
        assert short_name is not None
        loan_fixture.manager.auth.library_authenticators[
            short_name
        ] = MockLibraryAuthenticator()

        def mock_can_fulfill_without_loan(patron, pool, lpdm):
            self.called_with = (patron, pool, lpdm)
            return True

        with loan_fixture.request_context_with_library("/"):
            loan_fixture.manager.loans.circulation.can_fulfill_without_loan = (
                mock_can_fulfill_without_loan
            )
            assert True == m(loan_fixture.db.default_library(), patron, pool, lpdm)
            assert (patron, pool, lpdm) == self.called_with

    def test_patron_circulation_retrieval(self, loan_fixture: LoanFixture):
        """The controller can get loans and holds for a patron, even if
        there are multiple licensepools on the Work.
        """
        # Give the Work a second LicensePool.
        edition, other_pool = loan_fixture.db.edition(
            with_open_access_download=True,
            with_license_pool=True,
            data_source_name=DataSource.BIBLIOTHECA,
            collection=loan_fixture.pool.collection,
        )
        other_pool.identifier = loan_fixture.identifier
        other_pool.work = loan_fixture.pool.work

        pools = loan_fixture.manager.loans.load_licensepools(
            loan_fixture.library,
            loan_fixture.identifier.type,
            loan_fixture.identifier.identifier,
        )

        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            loan_fixture.manager.loans.authenticated_patron_from_request()

            # Without a loan or a hold, nothing is returned.
            # No loans.
            result = loan_fixture.manager.loans.get_patron_loan(
                loan_fixture.default_patron, pools
            )
            assert (None, None) == result

            # No holds.
            result = loan_fixture.manager.loans.get_patron_hold(
                loan_fixture.default_patron, pools
            )
            assert (None, None) == result

            # When there's a loan, we retrieve it.
            loan, newly_created = loan_fixture.pool.loan_to(loan_fixture.default_patron)
            result = loan_fixture.manager.loans.get_patron_loan(
                loan_fixture.default_patron, pools
            )
            assert (loan, loan_fixture.pool) == result

            # When there's a hold, we retrieve it.
            hold, newly_created = other_pool.on_hold_to(loan_fixture.default_patron)
            result = loan_fixture.manager.loans.get_patron_hold(
                loan_fixture.default_patron, pools
            )
            assert (hold, other_pool) == result

    def test_borrow_success(self, loan_fixture: LoanFixture):
        # Create a loanable LicensePool.
        work = loan_fixture.db.work(
            with_license_pool=True, with_open_access_download=False
        )
        pool = work.license_pools[0]
        loan_fixture.manager.d_circulation.queue_checkout(
            pool,
            LoanInfo(
                pool.collection,
                pool.data_source.name,
                pool.identifier.type,
                pool.identifier.identifier,
                utc_now(),
                utc_now() + datetime.timedelta(seconds=3600),
            ),
        )
        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            loan_fixture.manager.loans.authenticated_patron_from_request()
            response = loan_fixture.manager.loans.borrow(
                loan_fixture.identifier.type, loan_fixture.identifier.identifier
            )

            # A loan has been created for this license pool.
            loan = get_one(
                loan_fixture.db.session, Loan, license_pool=loan_fixture.pool
            )
            assert loan is not None
            # The loan has yet to be fulfilled.
            assert None == loan.fulfillment

            # We've been given an OPDS feed with one entry, which tells us how
            # to fulfill the license.
            assert 201 == response.status_code
            feed = feedparser.parse(response.get_data())
            [entry] = feed["entries"]
            fulfillment_links = [
                x["href"]
                for x in entry["links"]
                if x["rel"] == OPDSFeed.ACQUISITION_REL
            ]

            assert loan_fixture.mech1.resource is not None

            # Make sure the two delivery mechanisms are incompatible.
            loan_fixture.mech1.delivery_mechanism.drm_scheme = "DRM Scheme 1"
            loan_fixture.mech2.delivery_mechanism.drm_scheme = "DRM Scheme 2"
            fulfillable_mechanism = loan_fixture.mech1
            loan_fixture.db.session.commit()

            expects = [
                url_for(
                    "fulfill",
                    license_pool_id=loan_fixture.pool.id,
                    mechanism_id=mech.delivery_mechanism.id,
                    library_short_name=loan_fixture.library.short_name,
                    _external=True,
                )
                for mech in [loan_fixture.mech1, loan_fixture.mech2]
            ]
            assert set(expects) == set(fulfillment_links)

            # Make sure the first delivery mechanism has the data necessary
            # to carry out an open source fulfillment.
            assert loan_fixture.mech1.resource is not None
            assert loan_fixture.mech1.resource.representation is not None
            assert loan_fixture.mech1.resource.representation.url is not None

            # Now let's try to fulfill the loan using the first delivery mechanism.
            fulfillment = FulfillmentInfo(
                loan_fixture.pool.collection,
                loan_fixture.pool.data_source,
                loan_fixture.pool.identifier.type,
                loan_fixture.pool.identifier.identifier,
                content_link=fulfillable_mechanism.resource.representation.public_url,
                content_type=fulfillable_mechanism.resource.representation.media_type,
                content=None,
                content_expires=None,
            )
            loan_fixture.manager.d_circulation.queue_fulfill(
                loan_fixture.pool, fulfillment
            )

            assert isinstance(loan_fixture.pool.id, int)
            response = loan_fixture.manager.loans.fulfill(
                loan_fixture.pool.id,
                fulfillable_mechanism.delivery_mechanism.id,
            )
            if isinstance(response, ProblemDetail):
                j, status, headers = response.response
                raise Exception(repr(j))
            assert 302 == response.status_code
            assert (
                fulfillable_mechanism.resource.representation.public_url
                == response.headers.get("Location")
            )

            # The mechanism we used has been registered with the loan.
            assert fulfillable_mechanism == loan.fulfillment

            # Set the pool to be non-open-access, so we have to make an
            # external request to obtain the book.
            loan_fixture.pool.open_access = False

            http = DummyHTTPClient()

            fulfillment = FulfillmentInfo(
                loan_fixture.pool.collection,
                loan_fixture.pool.data_source,
                loan_fixture.pool.identifier.type,
                loan_fixture.pool.identifier.identifier,
                content_link=fulfillable_mechanism.resource.url,
                content_type=fulfillable_mechanism.resource.representation.media_type,
                content=None,
                content_expires=None,
            )

            # Now that we've set a mechanism, we can fulfill the loan
            # again without specifying a mechanism.
            loan_fixture.manager.d_circulation.queue_fulfill(
                loan_fixture.pool, fulfillment
            )
            http.queue_response(200, content="I am an ACSM file")

            response = loan_fixture.manager.loans.fulfill(
                loan_fixture.pool.id, do_get=http.do_get
            )
            assert 200 == response.status_code
            assert "I am an ACSM file" == response.get_data(as_text=True)
            assert http.requests == [fulfillable_mechanism.resource.url]

            # But we can't use some other mechanism -- we're stuck with
            # the first one we chose.
            response = loan_fixture.manager.loans.fulfill(
                loan_fixture.pool.id, loan_fixture.mech2.delivery_mechanism.id
            )

            assert 409 == response.status_code
            assert (
                "You already fulfilled this loan as application/epub+zip (DRM Scheme 1), you can't also do it as application/pdf (DRM Scheme 2)"
                in response.detail
            )

            # If the remote server fails, we get a problem detail.
            def doomed_get(url, headers, **kwargs):
                raise RemoteIntegrationException("fulfill service", "Error!")

            loan_fixture.manager.d_circulation.queue_fulfill(
                loan_fixture.pool, fulfillment
            )

            response = loan_fixture.manager.loans.fulfill(
                loan_fixture.pool.id, do_get=doomed_get
            )
            assert isinstance(response, ProblemDetail)
            assert 502 == response.status_code

    def test_borrow_and_fulfill_with_streaming_delivery_mechanism(
        self, loan_fixture: LoanFixture
    ):
        # Create a pool with a streaming delivery mechanism
        work = loan_fixture.db.work(
            with_license_pool=True, with_open_access_download=False
        )
        edition = work.presentation_edition
        pool = work.license_pools[0]
        pool.open_access = False
        streaming_mech = pool.set_delivery_mechanism(
            DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
            DeliveryMechanism.OVERDRIVE_DRM,
            RightsStatus.IN_COPYRIGHT,
            None,
        )
        identifier = edition.primary_identifier

        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            loan_fixture.manager.loans.authenticated_patron_from_request()
            loan_fixture.manager.d_circulation.queue_checkout(
                pool,
                LoanInfo(
                    pool.collection,
                    pool.data_source.name,
                    pool.identifier.type,
                    pool.identifier.identifier,
                    utc_now(),
                    utc_now() + datetime.timedelta(seconds=3600),
                ),
            )
            response = loan_fixture.manager.loans.borrow(
                identifier.type, identifier.identifier
            )

            # A loan has been created for this license pool.
            loan = get_one(loan_fixture.db.session, Loan, license_pool=pool)
            assert loan is not None
            # The loan has yet to be fulfilled.
            assert None == loan.fulfillment

            # We've been given an OPDS feed with two delivery mechanisms, which tell us how
            # to fulfill the license.
            assert 201 == response.status_code
            feed = feedparser.parse(response.get_data())
            [entry] = feed["entries"]
            fulfillment_links = [
                x["href"]
                for x in entry["links"]
                if x["rel"] == OPDSFeed.ACQUISITION_REL
            ]
            [mech1, mech2] = sorted(
                pool.delivery_mechanisms,
                key=lambda x: x.delivery_mechanism.is_streaming,
            )

            streaming_mechanism = mech2

            expects = [
                url_for(
                    "fulfill",
                    license_pool_id=pool.id,
                    mechanism_id=mech.delivery_mechanism.id,
                    library_short_name=loan_fixture.library.short_name,
                    _external=True,
                )
                for mech in [mech1, mech2]
            ]
            assert set(expects) == set(fulfillment_links)

            # Now let's try to fulfill the loan using the streaming mechanism.
            loan_fixture.manager.d_circulation.queue_fulfill(
                pool,
                FulfillmentInfo(
                    pool.collection,
                    pool.data_source.name,
                    pool.identifier.type,
                    pool.identifier.identifier,
                    "http://streaming-content-link",
                    Representation.TEXT_HTML_MEDIA_TYPE
                    + DeliveryMechanism.STREAMING_PROFILE,
                    None,
                    None,
                ),
            )
            response = loan_fixture.manager.loans.fulfill(
                pool.id, streaming_mechanism.delivery_mechanism.id
            )

            # We get an OPDS entry.
            assert 200 == response.status_code
            opds_entries = feedparser.parse(response.response[0])["entries"]
            assert 1 == len(opds_entries)
            links = opds_entries[0]["links"]

            # The entry includes one fulfill link.
            fulfill_links = [
                link
                for link in links
                if link["rel"] == "http://opds-spec.org/acquisition"
            ]
            assert 1 == len(fulfill_links)

            assert (
                Representation.TEXT_HTML_MEDIA_TYPE
                + DeliveryMechanism.STREAMING_PROFILE
                == fulfill_links[0]["type"]
            )
            assert "http://streaming-content-link" == fulfill_links[0]["href"]

            # The mechanism has not been set, since fulfilling a streaming
            # mechanism does not lock in the format.
            assert None == loan.fulfillment

            # We can still use the other mechanism too.
            http = DummyHTTPClient()
            http.queue_response(200, content="I am an ACSM file")

            loan_fixture.manager.d_circulation.queue_fulfill(
                pool,
                FulfillmentInfo(
                    pool.collection,
                    pool.data_source.name,
                    pool.identifier.type,
                    pool.identifier.identifier,
                    "http://other-content-link",
                    Representation.TEXT_HTML_MEDIA_TYPE,
                    None,
                    None,
                ),
            )
            response = loan_fixture.manager.loans.fulfill(
                pool.id, mech1.delivery_mechanism.id, do_get=http.do_get
            )
            assert 200 == response.status_code

            # Now the fulfillment has been set to the other mechanism.
            assert mech1 == loan.fulfillment

            # But we can still fulfill the streaming mechanism again.
            loan_fixture.manager.d_circulation.queue_fulfill(
                pool,
                FulfillmentInfo(
                    pool.collection,
                    pool.data_source.name,
                    pool.identifier.type,
                    pool.identifier.identifier,
                    "http://streaming-content-link",
                    Representation.TEXT_HTML_MEDIA_TYPE
                    + DeliveryMechanism.STREAMING_PROFILE,
                    None,
                    None,
                ),
            )

            response = loan_fixture.manager.loans.fulfill(
                pool.id, streaming_mechanism.delivery_mechanism.id
            )
            assert 200 == response.status_code
            opds_entries = feedparser.parse(response.response[0])["entries"]
            assert 1 == len(opds_entries)
            links = opds_entries[0]["links"]

            fulfill_links = [
                link
                for link in links
                if link["rel"] == "http://opds-spec.org/acquisition"
            ]
            assert 1 == len(fulfill_links)

            assert (
                Representation.TEXT_HTML_MEDIA_TYPE
                + DeliveryMechanism.STREAMING_PROFILE
                == fulfill_links[0]["type"]
            )
            assert "http://streaming-content-link" == fulfill_links[0]["href"]

    def test_borrow_nonexistent_delivery_mechanism(self, loan_fixture: LoanFixture):
        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            loan_fixture.manager.loans.authenticated_patron_from_request()
            response = loan_fixture.manager.loans.borrow(
                loan_fixture.identifier.type, loan_fixture.identifier.identifier, -100
            )
            assert BAD_DELIVERY_MECHANISM == response

    def test_borrow_creates_hold_when_no_available_copies(
        self, loan_fixture: LoanFixture
    ):
        threem_edition, pool = loan_fixture.db.edition(
            with_open_access_download=False,
            data_source_name=DataSource.THREEM,
            identifier_type=Identifier.THREEM_ID,
            with_license_pool=True,
        )
        threem_book = loan_fixture.db.work(
            presentation_edition=threem_edition,
        )
        pool.licenses_available = 0
        pool.open_access = False

        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            loan_fixture.manager.loans.authenticated_patron_from_request()
            loan_fixture.manager.d_circulation.queue_checkout(pool, NoAvailableCopies())
            loan_fixture.manager.d_circulation.queue_hold(
                pool,
                HoldInfo(
                    pool.collection,
                    pool.data_source.name,
                    pool.identifier.type,
                    pool.identifier.identifier,
                    utc_now(),
                    utc_now() + datetime.timedelta(seconds=3600),
                    1,
                ),
            )
            response = loan_fixture.manager.loans.borrow(
                pool.identifier.type, pool.identifier.identifier
            )
            assert 201 == response.status_code

            # A hold has been created for this license pool.
            hold = get_one(loan_fixture.db.session, Hold, license_pool=pool)
            assert hold != None

    def test_borrow_nolicenses(self, loan_fixture: LoanFixture):
        edition, pool = loan_fixture.db.edition(
            with_open_access_download=False,
            data_source_name=DataSource.GUTENBERG,
            identifier_type=Identifier.GUTENBERG_ID,
            with_license_pool=True,
        )

        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            loan_fixture.manager.loans.authenticated_patron_from_request()
            loan_fixture.manager.d_circulation.queue_checkout(pool, NoLicenses())

            response = loan_fixture.manager.loans.borrow(
                pool.identifier.type, pool.identifier.identifier
            )
            assert 404 == response.status_code
            assert NOT_FOUND_ON_REMOTE == response

    def test_borrow_creates_local_hold_if_remote_hold_exists(
        self, loan_fixture: LoanFixture
    ):
        """We try to check out a book, but turns out we already have it
        on hold.
        """
        threem_edition, pool = loan_fixture.db.edition(
            with_open_access_download=False,
            data_source_name=DataSource.THREEM,
            identifier_type=Identifier.THREEM_ID,
            with_license_pool=True,
        )
        threem_book = loan_fixture.db.work(
            presentation_edition=threem_edition,
        )
        pool.licenses_available = 0
        pool.open_access = False

        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            loan_fixture.manager.loans.authenticated_patron_from_request()
            loan_fixture.manager.d_circulation.queue_checkout(pool, AlreadyOnHold())
            loan_fixture.manager.d_circulation.queue_hold(
                pool,
                HoldInfo(
                    pool.collection,
                    pool.data_source.name,
                    pool.identifier.type,
                    pool.identifier.identifier,
                    utc_now(),
                    utc_now() + datetime.timedelta(seconds=3600),
                    1,
                ),
            )
            response = loan_fixture.manager.loans.borrow(
                pool.identifier.type, pool.identifier.identifier
            )
            assert 201 == response.status_code

            # A hold has been created for this license pool.
            hold = get_one(loan_fixture.db.session, Hold, license_pool=pool)
            assert hold != None

    def test_borrow_fails_when_work_not_present_on_remote(
        self, loan_fixture: LoanFixture
    ):
        threem_edition, pool = loan_fixture.db.edition(
            with_open_access_download=False,
            data_source_name=DataSource.THREEM,
            identifier_type=Identifier.THREEM_ID,
            with_license_pool=True,
        )
        threem_book = loan_fixture.db.work(
            presentation_edition=threem_edition,
        )
        pool.licenses_available = 1
        pool.open_access = False

        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            loan_fixture.manager.loans.authenticated_patron_from_request()
            loan_fixture.manager.d_circulation.queue_checkout(pool, NotFoundOnRemote())
            response = loan_fixture.manager.loans.borrow(
                pool.identifier.type, pool.identifier.identifier
            )
            assert 404 == response.status_code
            assert (
                "http://librarysimplified.org/terms/problem/not-found-on-remote"
                == response.uri
            )

    def test_borrow_succeeds_when_work_already_checked_out(
        self, loan_fixture: LoanFixture
    ):
        # An attempt to borrow a book that's already on loan is
        # treated as success without even going to the remote API.
        loan, _ignore = get_one_or_create(
            loan_fixture.db.session,
            Loan,
            license_pool=loan_fixture.pool,
            patron=loan_fixture.default_patron,
        )

        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            loan_fixture.manager.loans.authenticated_patron_from_request()

            # Set it up that going to the remote API would raise an
            # exception, to prove we're not going to do that.
            circulation = loan_fixture.manager.d_circulation
            circulation.queue_checkout(loan.license_pool, NotFoundOnRemote())

            mock_remote = circulation.api_for_license_pool(loan.license_pool)
            assert 1 == len(mock_remote.responses["checkout"])
            response = loan_fixture.manager.loans.borrow(
                loan_fixture.identifier.type, loan_fixture.identifier.identifier
            )

            # No checkout request was actually made to the remote.
            assert 1 == len(mock_remote.responses["checkout"])

            # We got an OPDS entry that includes at least one
            # fulfillment link, which is what we expect when we ask
            # about an active loan.
            assert 200 == response.status_code
            [entry] = feedparser.parse(response.response[0])["entries"]
            assert any(
                [
                    x
                    for x in entry["links"]
                    if x["rel"] == "http://opds-spec.org/acquisition"
                ]
            )

    def test_fulfill(self, loan_fixture: LoanFixture):
        # Verify that arguments to the fulfill() method are propagated
        # correctly to the CirculationAPI.

        controller = loan_fixture.manager.loans
        mock = MagicMock(spec=CirculationAPI)
        controller.manager.circulation_apis[loan_fixture.db.default_library().id] = mock

        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            authenticated = controller.authenticated_patron_from_request()
            loan, ignore = loan_fixture.pool.loan_to(authenticated)

            # Try to fulfill the loan.
            assert isinstance(loan_fixture.pool.id, int)
            controller.fulfill(
                loan_fixture.pool.id, loan_fixture.mech2.delivery_mechanism.id
            )

            # Verify that the right arguments were passed into
            # CirculationAPI.
            mock.fulfill.assert_called_once_with(
                authenticated,
                loan_fixture.valid_credentials["password"],
                loan_fixture.pool,
                loan_fixture.mech2,
            )

    @pytest.mark.parametrize(
        "as_response_value",
        [
            Response(status=200, response="Here's your response"),
            Response(status=401, response="Error"),
            Response(status=500, response="Fault"),
        ],
    )
    def test_fulfill_returns_fulfillment_info_implementing_as_response(
        self, as_response_value, loan_fixture: LoanFixture
    ):
        # If CirculationAPI.fulfill returns a FulfillmentInfo that
        # defines as_response, the result of as_response is returned
        # directly and the normal process of converting a FulfillmentInfo
        # to a Flask response is skipped.
        class MockFulfillmentInfo(FulfillmentInfo):
            @property
            def as_response(self):
                return as_response_value

        class MockCirculationAPI:
            def fulfill(self, *args, **kwargs):
                return MockFulfillmentInfo(
                    loan_fixture.db.default_collection(),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )

        controller = loan_fixture.manager.loans
        mock = MockCirculationAPI()
        controller.manager.circulation_apis[loan_fixture.db.default_library().id] = mock

        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            authenticated = controller.authenticated_patron_from_request()
            loan, ignore = loan_fixture.pool.loan_to(authenticated)

            # Fulfill the loan.
            assert isinstance(loan_fixture.pool.id, int)
            result = controller.fulfill(
                loan_fixture.pool.id, loan_fixture.mech2.delivery_mechanism.id
            )

            # The result of MockFulfillmentInfo.as_response was
            # returned directly.
            assert as_response_value == result

    def test_fulfill_without_active_loan(self, loan_fixture: LoanFixture):
        controller = loan_fixture.manager.loans

        # Most of the time, it is not possible to fulfill a title if the
        # patron has no active loan for the title. This might be
        # because the patron never checked out the book...
        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            controller.authenticated_patron_from_request()
            assert isinstance(loan_fixture.pool.id, int)
            response = controller.fulfill(
                loan_fixture.pool.id, loan_fixture.mech2.delivery_mechanism.id
            )
            assert isinstance(response, ProblemDetail)
            assert NO_ACTIVE_LOAN.uri == response.uri

        # ...or it might be because there is no authenticated patron.
        with loan_fixture.request_context_with_library("/"):
            response = controller.fulfill(
                loan_fixture.pool.id, loan_fixture.mech2.delivery_mechanism.id
            )
            assert isinstance(response, FlaskResponse)
            assert 401 == response.status_code

        # ...or it might be because of an error communicating
        # with the authentication provider.
        old_authenticated_patron = controller.authenticated_patron_from_request

        def mock_authenticated_patron():
            return INTEGRATION_ERROR

        controller.authenticated_patron_from_request = mock_authenticated_patron
        with loan_fixture.request_context_with_library("/"):
            problem = controller.fulfill(
                loan_fixture.pool.id, loan_fixture.mech2.delivery_mechanism.id
            )
            assert INTEGRATION_ERROR == problem
        controller.authenticated_patron_from_request = old_authenticated_patron

        # However, if can_fulfill_without_loan returns True, then
        # fulfill() will be called. If fulfill() returns a
        # FulfillmentInfo, then the title is fulfilled, with no loan
        # having been created.
        #
        # To that end, we'll mock can_fulfill_without_loan and fulfill.
        def mock_can_fulfill_without_loan(*args, **kwargs):
            return True

        def mock_fulfill(*args, **kwargs):
            return FulfillmentInfo(
                loan_fixture.collection,
                loan_fixture.pool.data_source.name,
                loan_fixture.pool.identifier.type,
                loan_fixture.pool.identifier.identifier,
                None,
                "text/html",
                "here's your book",
                utc_now(),
            )

        # Now we're able to fulfill the book even without
        # authenticating a patron.
        with loan_fixture.request_context_with_library("/"):
            controller.can_fulfill_without_loan = mock_can_fulfill_without_loan
            controller.circulation.fulfill = mock_fulfill
            response = controller.fulfill(
                loan_fixture.pool.id, loan_fixture.mech2.delivery_mechanism.id
            )

            assert isinstance(response, wkResponse)
            assert "here's your book" == response.get_data(as_text=True)
            assert [] == loan_fixture.db.session.query(Loan).all()

    def test_fulfill_without_single_item_feed(self, loan_fixture: LoanFixture):
        """A streaming fulfillment fails due to the feed method failing"""
        controller = loan_fixture.manager.loans
        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            circulation = controller.circulation
            authenticated = controller.authenticated_patron_from_request()
            loan_fixture.pool.loan_to(authenticated)
            with patch(
                "api.controller.opds_feed.OPDSAcquisitionFeed.single_entry_loans_feed"
            ) as feed, patch.object(circulation, "fulfill") as fulfill:
                # Complex setup
                # The fulfillmentInfo should not be have response type
                fulfill.return_value.as_response = None
                # The single_item_feed must return this error
                feed.return_value = NOT_FOUND_ON_REMOTE
                # The content type needs to be streaming
                loan_fixture.mech1.delivery_mechanism.content_type = (
                    DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE
                )

                assert isinstance(loan_fixture.pool.id, int)
                response = controller.fulfill(
                    loan_fixture.pool.id, loan_fixture.mech1.delivery_mechanism.id
                )
                assert response == NOT_FOUND_ON_REMOTE

    def test_no_drm_fulfill(self, loan_fixture: LoanFixture):
        """In case a work does not have DRM for it's fulfillment.
        We must simply redirect the client to the non-DRM'd location
        instead doing a proxy download"""
        # setup the patron, work and loan
        patron = loan_fixture.db.patron()
        work: Work = loan_fixture.db.work(
            with_license_pool=True, data_source_name=DataSource.OVERDRIVE
        )

        pool_opt: LicensePool | None = work.active_license_pool()
        assert pool_opt is not None
        pool: LicensePool = pool_opt
        pool.loan_to(patron)
        controller = loan_fixture.manager.loans

        # This work has a no-DRM fulfillment criteria
        lpdm = pool.set_delivery_mechanism(
            MediaTypes.EPUB_MEDIA_TYPE,
            DeliveryMechanism.NO_DRM,
            RightsStatus.IN_COPYRIGHT,
        )
        lpdm.delivery_mechanism.default_client_can_fulfill = True

        # Mock out the flow
        api = MagicMock(spec=BaseCirculationAPI)
        api.fulfill.return_value = FulfillmentInfo(
            loan_fixture.db.default_collection(),
            DataSource.OVERDRIVE,
            "overdrive",
            pool.identifier.identifier,
            "https://example.org/redirect_to_epub",
            MediaTypes.EPUB_MEDIA_TYPE,
            "",
            None,
            content_link_redirect=True,
        )
        controller.can_fulfill_without_loan = MagicMock(return_value=False)
        controller.authenticated_patron_from_request = MagicMock(return_value=patron)

        with loan_fixture.request_context_with_library(
            "/",
            library=loan_fixture.db.default_library(),
            headers=dict(Authorization=loan_fixture.valid_auth),
        ):
            loan_fixture.manager.circulation_apis[
                loan_fixture.db.default_library().id
            ] = CirculationAPI(
                loan_fixture.db.session, loan_fixture.db.default_library()
            )
            controller.circulation.api_for_collection[
                loan_fixture.db.default_collection().id
            ] = api
            assert isinstance(pool.id, int)
            response = controller.fulfill(pool.id, lpdm.delivery_mechanism.id)

        assert isinstance(response, wkResponse)
        assert response.status_code == 302
        assert response.location == "https://example.org/redirect_to_epub"

        # Axis360 variant
        api = MagicMock(spec=Axis360API)
        api.collection = loan_fixture.db.default_collection()
        api._db = loan_fixture.db.session
        axis360_ff = Axis360FulfillmentInfo(
            api, DataSource.AXIS_360, "Axis 360 ID", "xxxxxx", "xxxxxx"
        )
        api.get_fulfillment_info.return_value = MagicMock(
            content={
                "ExpirationDate": "2020-01-01 00:00:00",
                "Status": dict(Code=1, Message="Worked."),
                "ISBN": "ISBN ID",
                "BookVaultUUID": "Vault ID",
            }
        )
        api.fulfill.return_value = axis360_ff
        assert isinstance(pool.id, int)
        with loan_fixture.request_context_with_library(
            "/",
            library=loan_fixture.db.default_library(),
            headers=dict(Authorization=loan_fixture.valid_auth),
        ):
            controller.circulation.api_for_collection[
                loan_fixture.db.default_collection().id
            ] = api
            response = controller.fulfill(pool.id, lpdm.delivery_mechanism.id)

        assert isinstance(response, wkResponse)
        assert response.status_code == 200
        assert response.json == {"book_vault_uuid": "Vault ID", "isbn": "ISBN ID"}

    def test_revoke_loan(self, loan_fixture: LoanFixture):
        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            patron = loan_fixture.manager.loans.authenticated_patron_from_request()
            loan, newly_created = loan_fixture.pool.loan_to(patron)

            loan_fixture.manager.d_circulation.queue_checkin(loan_fixture.pool, True)

            response = loan_fixture.manager.loans.revoke(loan_fixture.pool.id)

            assert 200 == response.status_code

    def test_revoke_hold(self, loan_fixture: LoanFixture):
        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            patron = loan_fixture.manager.loans.authenticated_patron_from_request()
            hold, newly_created = loan_fixture.pool.on_hold_to(patron, position=0)

            loan_fixture.manager.d_circulation.queue_release_hold(
                loan_fixture.pool, True
            )

            response = loan_fixture.manager.loans.revoke(loan_fixture.pool.id)

            assert 200 == response.status_code

    def test_revoke_hold_nonexistent_licensepool(self, loan_fixture: LoanFixture):
        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            patron = loan_fixture.manager.loans.authenticated_patron_from_request()
            response = loan_fixture.manager.loans.revoke(-10)
            assert isinstance(response, ProblemDetail)
            assert INVALID_INPUT.uri == response.uri

    def test_hold_fails_when_patron_is_at_hold_limit(self, loan_fixture: LoanFixture):
        edition, pool = loan_fixture.db.edition(with_license_pool=True)
        pool.open_access = False
        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            patron = loan_fixture.manager.loans.authenticated_patron_from_request()
            loan_fixture.manager.d_circulation.queue_checkout(pool, NoAvailableCopies())
            loan_fixture.manager.d_circulation.queue_hold(
                pool, PatronHoldLimitReached()
            )
            response = loan_fixture.manager.loans.borrow(
                pool.identifier.type, pool.identifier.identifier
            )
            assert isinstance(response, ProblemDetail)
            assert HOLD_LIMIT_REACHED.uri == response.uri

    def test_borrow_fails_with_outstanding_fines(
        self, loan_fixture: LoanFixture, library_fixture: LibraryFixture
    ):
        threem_edition, pool = loan_fixture.db.edition(
            with_open_access_download=False,
            data_source_name=DataSource.THREEM,
            identifier_type=Identifier.THREEM_ID,
            with_license_pool=True,
        )
        threem_book = loan_fixture.db.work(
            presentation_edition=threem_edition,
        )
        pool.open_access = False

        library = loan_fixture.db.default_library()
        settings = library_fixture.settings(library)

        settings.max_outstanding_fines = 0.50
        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            # The patron's credentials are valid, but they have a lot
            # of fines.
            patron = loan_fixture.manager.loans.authenticated_patron_from_request()
            patron.fines = Decimal("12345678.90")
            response = loan_fixture.manager.loans.borrow(
                pool.identifier.type, pool.identifier.identifier
            )

            assert 403 == response.status_code
            assert OUTSTANDING_FINES.uri == response.uri
            assert "$12345678.90 outstanding" in response.detail

        # Reduce the patron's fines, and there's no problem.
        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            patron = loan_fixture.manager.loans.authenticated_patron_from_request()
            patron.fines = Decimal("0.49")
            loan_fixture.manager.d_circulation.queue_checkout(
                pool,
                LoanInfo(
                    pool.collection,
                    pool.data_source.name,
                    pool.identifier.type,
                    pool.identifier.identifier,
                    utc_now(),
                    utc_now() + datetime.timedelta(seconds=3600),
                ),
            )
            response = loan_fixture.manager.loans.borrow(
                pool.identifier.type, pool.identifier.identifier
            )

            assert 201 == response.status_code

    def test_3m_cant_revoke_hold_if_reserved(self, loan_fixture: LoanFixture):
        threem_edition, pool = loan_fixture.db.edition(
            with_open_access_download=False,
            data_source_name=DataSource.THREEM,
            identifier_type=Identifier.THREEM_ID,
            with_license_pool=True,
        )
        threem_book = loan_fixture.db.work(
            presentation_edition=threem_edition,
        )
        pool.open_access = False

        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            patron = loan_fixture.manager.loans.authenticated_patron_from_request()
            hold, newly_created = pool.on_hold_to(patron, position=0)
            response = loan_fixture.manager.loans.revoke(pool.id)
            assert 400 == response.status_code
            assert CANNOT_RELEASE_HOLD.uri == response.uri
            assert (
                "Cannot release a hold once it enters reserved state."
                == response.detail
            )

    def test_active_loans(self, loan_fixture: LoanFixture):
        # First, verify that this controller supports conditional HTTP
        # GET by calling handle_conditional_request and propagating
        # any Response it returns.
        response_304 = Response(status=304)

        def handle_conditional_request(last_modified=None):
            return response_304

        original_handle_conditional_request = (
            loan_fixture.controller.handle_conditional_request
        )
        loan_fixture.manager.loans.handle_conditional_request = (
            handle_conditional_request
        )

        # Before making any requests, set the patron's last_loan_activity_sync
        # to a known value.
        patron = None
        with loan_fixture.request_context_with_library("/"):
            patron = loan_fixture.controller.authenticated_patron(
                loan_fixture.valid_credentials
            )
        now = utc_now()
        patron.last_loan_activity_sync = now

        # Make a request -- it doesn't have If-Modified-Since, but our
        # mocked handle_conditional_request will treat it as a
        # successful conditional request.
        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            patron = loan_fixture.manager.loans.authenticated_patron_from_request()
            response = loan_fixture.manager.loans.sync()
            assert response is response_304

        # Since the conditional request succeeded, we did not call out
        # to the vendor APIs, and patron.last_loan_activity_sync was
        # not updated.
        assert now == patron.last_loan_activity_sync

        # Leaving patron.last_loan_activity_sync alone will stop the
        # circulation manager from calling out to the external APIs,
        # since it was set to a recent time. We test this explicitly
        # later, but for now, clear it out.
        patron.last_loan_activity_sync = None

        # Un-mock handle_conditional_request. It will be called over
        # the course of this test, but it will not notice any more
        # conditional requests -- the detailed behavior of
        # handle_conditional_request is tested elsewhere.
        loan_fixture.manager.loans.handle_conditional_request = (
            original_handle_conditional_request
        )

        # If the request is not conditional, an OPDS feed is returned.
        # This feed is empty because the patron has no loans.
        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            patron = loan_fixture.manager.loans.authenticated_patron_from_request()
            response = loan_fixture.manager.loans.sync()
            assert not "<entry>" in response.get_data(as_text=True)
            assert response.headers["Cache-Control"].startswith("private,")

            # patron.last_loan_activity_sync was set to the moment the
            # LoanController started calling out to the remote APIs.
            new_sync_time = patron.last_loan_activity_sync
            assert new_sync_time > now

        # Set up a bunch of loans on the remote APIs.
        overdrive_edition, overdrive_pool = loan_fixture.db.edition(
            with_open_access_download=False,
            data_source_name=DataSource.OVERDRIVE,
            identifier_type=Identifier.OVERDRIVE_ID,
            with_license_pool=True,
        )
        overdrive_book = loan_fixture.db.work(
            presentation_edition=overdrive_edition,
        )
        overdrive_pool.open_access = False

        bibliotheca_edition, bibliotheca_pool = loan_fixture.db.edition(
            with_open_access_download=False,
            data_source_name=DataSource.BIBLIOTHECA,
            identifier_type=Identifier.BIBLIOTHECA_ID,
            with_license_pool=True,
        )
        bibliotheca_book = loan_fixture.db.work(
            presentation_edition=bibliotheca_edition,
        )
        bibliotheca_pool.licenses_available = 0
        bibliotheca_pool.open_access = False

        loan_fixture.manager.d_circulation.add_remote_loan(
            overdrive_pool.collection,
            overdrive_pool.data_source,
            overdrive_pool.identifier.type,
            overdrive_pool.identifier.identifier,
            utc_now(),
            utc_now() + datetime.timedelta(seconds=3600),
        )
        loan_fixture.manager.d_circulation.add_remote_hold(
            bibliotheca_pool.collection,
            bibliotheca_pool.data_source,
            bibliotheca_pool.identifier.type,
            bibliotheca_pool.identifier.identifier,
            utc_now(),
            utc_now() + datetime.timedelta(seconds=3600),
            0,
        )

        # Making a new request so soon after the last one means the
        # circulation manager won't actually call out to the vendor
        # APIs. The resulting feed won't reflect what we know to be
        # the reality.
        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            patron = loan_fixture.manager.loans.authenticated_patron_from_request()
            response = loan_fixture.manager.loans.sync()
            assert "<entry>" not in response.get_data(as_text=True)

        # patron.last_loan_activity_sync was not changed as the result
        # of this request, since we didn't go to the vendor APIs.
        assert patron.last_loan_activity_sync == new_sync_time

        # Change it now, to a timestamp far in the past.
        long_ago = datetime_utc(2000, 1, 1)
        patron.last_loan_activity_sync = long_ago

        # This ensures that when we request the loans feed again, the
        # LoanController actually goes out to the vendor APIs for new
        # information.
        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            patron = loan_fixture.manager.loans.authenticated_patron_from_request()
            response = loan_fixture.manager.loans.sync()

            # This time, the feed contains entries.
            feed = feedparser.parse(response.data)
            entries = feed["entries"]

            overdrive_entry = [
                entry for entry in entries if entry["title"] == overdrive_book.title
            ][0]
            bibliotheca_entry = [
                entry for entry in entries if entry["title"] == bibliotheca_book.title
            ][0]

            assert overdrive_entry["opds_availability"]["status"] == "available"
            assert bibliotheca_entry["opds_availability"]["status"] == "ready"

            overdrive_links = overdrive_entry["links"]
            fulfill_link = [
                x
                for x in overdrive_links
                if x["rel"] == "http://opds-spec.org/acquisition"
            ][0]["href"]
            revoke_link = [
                x for x in overdrive_links if x["rel"] == OPDSFeed.REVOKE_LOAN_REL
            ][0]["href"]
            bibliotheca_links = bibliotheca_entry["links"]
            borrow_link = [
                x
                for x in bibliotheca_links
                if x["rel"] == "http://opds-spec.org/acquisition/borrow"
            ][0]["href"]
            bibliotheca_revoke_links = [
                x for x in bibliotheca_links if x["rel"] == OPDSFeed.REVOKE_LOAN_REL
            ]

            assert urllib.parse.quote("%s/fulfill" % overdrive_pool.id) in fulfill_link
            assert urllib.parse.quote("%s/revoke" % overdrive_pool.id) in revoke_link
            assert (
                urllib.parse.quote(
                    "%s/%s/borrow"
                    % (
                        bibliotheca_pool.identifier.type,
                        bibliotheca_pool.identifier.identifier,
                    )
                )
                in borrow_link
            )
            assert 0 == len(bibliotheca_revoke_links)

            # Since we went out the the vendor APIs,
            # patron.last_loan_activity_sync was updated.
            assert patron.last_loan_activity_sync > new_sync_time

    @pytest.mark.parametrize(
        "target_loan_duration, "
        "db_loan_duration, "
        "opds_response_loan_duration, "
        "collection_protocol, "
        "collection_data_source_name, "
        "collection_default_loan_period",
        [
            [
                # Loan without duration, collection without configured loan period
                None,
                None,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD,
                ExternalIntegration.OPDS_IMPORT,
                None,
                None,
            ],  # DB and OPDS response loan duration mismatch
            [
                # Loan duration < CM STANDARD_DEFAULT_LOAN_PERIOD, collection without configured loan period
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 1,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 1,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 1,
                ExternalIntegration.OPDS_IMPORT,
                None,
                None,
            ],
            [
                # Loan duration > CM STANDARD_DEFAULT_LOAN_PERIOD, collection without configured loan period
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 1,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 1,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 1,
                ExternalIntegration.OPDS_IMPORT,
                None,
                None,
            ],
            [
                # Loan without duration, collection loan period < CM STANDARD_DEFAULT_LOAN_PERIOD
                None,
                None,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 2,
                ExternalIntegration.BIBLIOTHECA,
                DataSource.BIBLIOTHECA,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 2,
            ],  # DB and OPDS response loan duration mismatch
            [
                # Loan duration < collection loan period < CM STANDARD_DEFAULT_LOAN_PERIOD
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 3,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 3,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 3,
                ExternalIntegration.BIBLIOTHECA,
                DataSource.BIBLIOTHECA,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 2,
            ],
            [
                # Collection loan period < loan duration < CM STANDARD_DEFAULT_LOAN_PERIOD
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 1,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 1,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 1,
                ExternalIntegration.BIBLIOTHECA,
                DataSource.BIBLIOTHECA,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 2,
            ],
            [
                # Collection loan period < CM STANDARD_DEFAULT_LOAN_PERIOD < loan duration
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 1,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 1,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 1,
                ExternalIntegration.BIBLIOTHECA,
                DataSource.BIBLIOTHECA,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 2,
            ],
            [
                # Loan without duration, CM STANDARD_DEFAULT_LOAN_PERIOD < collection loan period
                None,
                None,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 2,
                ExternalIntegration.BIBLIOTHECA,
                DataSource.BIBLIOTHECA,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 2,
            ],  # DB and OPDS response loan duration mismatch
            [
                # Loan duration < CM STANDARD_DEFAULT_LOAN_PERIOD < collection loan period
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 1,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 1,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 1,
                ExternalIntegration.BIBLIOTHECA,
                DataSource.BIBLIOTHECA,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 2,
            ],
            [
                # CM STANDARD_DEFAULT_LOAN_PERIOD < loan duration < collection loan period
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 1,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 1,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 1,
                ExternalIntegration.BIBLIOTHECA,
                DataSource.BIBLIOTHECA,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 2,
            ],
            [
                # CM STANDARD_DEFAULT_LOAN_PERIOD < collection loan period < loan duration
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 3,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 3,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 3,
                ExternalIntegration.BIBLIOTHECA,
                DataSource.BIBLIOTHECA,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 2,
            ],
        ],
    )
    def test_loan_duration_settings_impact_on_loans_and_borrow_response(
        self,
        loan_fixture: LoanFixture,
        target_loan_duration: int,
        db_loan_duration: int,
        opds_response_loan_duration: int,
        collection_protocol: str,
        collection_data_source_name: str,
        collection_default_loan_period: int,
    ):
        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            loan_fixture.manager.loans.authenticated_patron_from_request()

            loan_start = utc_now()

            loan_end = None
            if target_loan_duration:
                loan_end = loan_start + datetime.timedelta(days=target_loan_duration)

            collection = loan_fixture.db.collection(
                protocol=collection_protocol,
                data_source_name=collection_data_source_name,
            )

            collection.libraries.append(loan_fixture.db.default_library())
            if collection_default_loan_period:
                lib_config = collection.integration_configuration.for_library(
                    loan_fixture.db.default_library()
                )
                assert lib_config is not None
                DatabaseTransactionFixture.set_settings(
                    lib_config,
                    collection.loan_period_key(),
                    collection_default_loan_period,
                )

            def create_work_and_return_license_pool_and_loan_info(**kwargs):
                loan_start = kwargs.pop("loan_start", utc_now())
                loan_end = kwargs.pop("loan_end", None)

                work = loan_fixture.db.work(
                    with_license_pool=True, with_open_access_download=False, **kwargs
                )
                license_pool = work.license_pools[0]

                loan_info = LoanInfo(
                    license_pool.collection,
                    license_pool.data_source.name,
                    license_pool.identifier.type,
                    license_pool.identifier.identifier,
                    loan_start,
                    loan_end,
                )

                return license_pool, loan_info

            license_pool, loan_info = create_work_and_return_license_pool_and_loan_info(
                loan_start=loan_start,
                loan_end=loan_end,
                data_source_name=collection_data_source_name,
                collection=collection,
            )

            loan_fixture.manager.d_circulation.queue_checkout(license_pool, loan_info)

            response = loan_fixture.manager.loans.borrow(
                license_pool.identifier.type, license_pool.identifier.identifier
            )

            loan = get_one(loan_fixture.db.session, Loan, license_pool=license_pool)
            assert loan is not None

            def parse_loan_until_field_from_opds_response(opds_response):
                feed = feedparser.parse(opds_response.data)
                [entry] = feed.get("entries")
                availability = entry.get("opds_availability")
                until = availability.get("until")

                return until

            loan_response_until = parse_loan_until_field_from_opds_response(response)

            expected_db_loan_end = None
            if db_loan_duration:
                expected_db_loan_end = loan_start + datetime.timedelta(
                    days=db_loan_duration
                )

            expected_opds_response_loan_end = None
            if opds_response_loan_duration:
                expected_opds_response_loan_end = loan_start + datetime.timedelta(
                    days=opds_response_loan_duration
                )

            def format_datetime(none_or_datetime):
                if none_or_datetime is None:
                    return None

                if isinstance(none_or_datetime, str):
                    return none_or_datetime

                return datetime.datetime.strftime(
                    none_or_datetime, "%Y-%m-%dT%H:%M:%S+00:00"
                )

            assert format_datetime(loan.end) == format_datetime(expected_db_loan_end)
            assert format_datetime(loan_response_until) == format_datetime(
                expected_opds_response_loan_end
            )
