import datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, create_autospec, patch
from urllib.parse import quote

import feedparser
import pytest
from flask import Response as FlaskResponse, url_for
from werkzeug import Response as wkResponse

from palace.manager.api.circulation.base import BaseCirculationAPI
from palace.manager.api.circulation.data import HoldInfo, LoanInfo
from palace.manager.api.circulation.dispatcher import CirculationApiDispatcher
from palace.manager.api.circulation.exceptions import (
    AlreadyOnHold,
    CannotReleaseHold,
    CannotReturn,
    HoldsNotPermitted,
    NoAvailableCopies,
    NoLicenses,
    NotFoundOnRemote,
    PatronHoldLimitReached,
)
from palace.manager.api.circulation.fulfillment import (
    DirectFulfillment,
    FetchFulfillment,
    Fulfillment,
    RedirectFulfillment,
)
from palace.manager.api.problem_details import (
    BAD_DELIVERY_MECHANISM,
    CANNOT_RELEASE_HOLD,
    COULD_NOT_MIRROR_TO_REMOTE,
    HOLD_LIMIT_REACHED,
    HOLDS_NOT_PERMITTED,
    NO_ACTIVE_LOAN,
    NO_LICENSES,
    NOT_FOUND_ON_REMOTE,
    OUTSTANDING_FINES,
)
from palace.manager.core.problem_details import INTEGRATION_ERROR, INVALID_INPUT
from palace.manager.feed.serializer.opds2 import OPDS2Serializer
from palace.manager.integration.license.bibliotheca import BibliothecaAPI
from palace.manager.integration.license.opds.opds1.api import OPDSAPI
from palace.manager.service.redis.models.patron_activity import PatronActivity
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    LicensePoolDeliveryMechanism,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.patron import Hold, Loan, Patron
from palace.manager.sqlalchemy.model.resource import Representation
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.sqlalchemy.util import get_one, get_one_or_create
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.flask_util import OPDSEntryResponse, Response
from palace.manager.util.http.exception import RemoteIntegrationException
from palace.manager.util.opds_writer import AtomFeed, OPDSFeed
from palace.manager.util.problem_detail import ProblemDetail
from tests.fixtures.api_controller import CirculationControllerFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.http import MockHttpClientFixture
from tests.fixtures.library import LibraryFixture
from tests.fixtures.redis import RedisFixture
from tests.fixtures.services import ServicesFixture
from tests.mocks.circulation import MockPatronActivityCirculationAPI


class LoanFixture(CirculationControllerFixture):
    identifier: Identifier
    data_source: DataSource
    mech2: LicensePoolDeliveryMechanism
    mech1: LicensePoolDeliveryMechanism
    pool: LicensePool

    def __init__(
        self, db: DatabaseTransactionFixture, services_fixture: ServicesFixture
    ):
        super().__init__(db, services_fixture)
        self.pool = self.english_1.license_pools[0]
        assert self.pool.id is not None
        self.pool_id = self.pool.id
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

        assert self.identifier is not None
        assert self.identifier.identifier is not None
        assert self.identifier.type is not None

        self.identifier_identifier = self.identifier.identifier
        self.identifier_type = self.identifier.type

        # Make sure our collection has a PatronActivityCirculationAPI setup, so we can test the
        # patron activity sync tasks.
        self.manager.d_circulation.add_remote_api(
            self.pool, MockPatronActivityCirculationAPI(db.session, self.collection)
        )


@pytest.fixture(scope="function")
def loan_fixture(
    db: DatabaseTransactionFixture, services_fixture: ServicesFixture
) -> LoanFixture:
    return LoanFixture(db, services_fixture)


class OPDSSerializationTestHelper:
    PARAMETRIZED_SINGLE_ENTRY_ACCEPT_HEADERS = (
        "accept_header,expected_content_type",
        [
            (None, OPDSFeed.ENTRY_TYPE),
            ("default-foo-bar", OPDSFeed.ENTRY_TYPE),
            (AtomFeed.ATOM_TYPE, OPDSFeed.ENTRY_TYPE),
            (OPDS2Serializer.CONTENT_TYPE, OPDS2Serializer.CONTENT_TYPE),
        ],
    )

    def __init__(
        self,
        accept_header: str | None = None,
        expected_content_type: str | None = None,
    ):
        self.accept_header = accept_header
        self.expected_content_type = expected_content_type

    def merge_accept_header(self, headers):
        return headers | ({"Accept": self.accept_header} if self.accept_header else {})

    def verify_and_get_single_entry_feed_links(self, response):
        assert response.content_type == self.expected_content_type
        if self.expected_content_type == OPDSFeed.ENTRY_TYPE:
            feed = feedparser.parse(response.get_data())
            [entry] = feed["entries"]
        elif self.expected_content_type == OPDS2Serializer.CONTENT_TYPE:
            entry = response.get_json()
        else:
            assert (
                False
            ), f"Unexpected content type prefix: {self.expected_content_type}"

        # Ensure that the response content parsed correctly.
        assert "links" in entry
        return entry["links"]


class TestLoanController:
    def test_can_fulfill_without_loan(self, loan_fixture: LoanFixture):
        """Test the circumstances under which a title can be fulfilled
        in the absence of an active loan for that title.
        """
        m = loan_fixture.manager.loans.can_fulfill_without_loan

        # If the library has a way of authenticating patrons (as the
        # default library does), then fulfilling a title always
        # requires an active loan.
        patron = MagicMock()
        pool = MagicMock()
        lpdm = MagicMock()
        assert False == m(loan_fixture.db.default_library(), patron, pool, lpdm)

        # If the library does not authenticate patrons, then this
        # _may_ be possible, but
        # CirculationAPI.can_fulfill_without_loan also has to say it's
        # okay.
        class MockLibraryAuthenticator:
            identifies_individuals = False

        short_name = loan_fixture.db.default_library().short_name
        assert short_name is not None
        loan_fixture.manager.auth.library_authenticators[short_name] = (
            MockLibraryAuthenticator()
        )

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
            loan_fixture.identifier_type,
            loan_fixture.identifier_identifier,
        )
        assert not isinstance(pools, ProblemDetail)

        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            loan_fixture.manager.loans.authenticated_patron_from_request()

            # Without a loan or a hold, nothing is returned.
            # No loans.
            result_loan_call = loan_fixture.manager.loans.get_patron_loan(
                loan_fixture.default_patron, pools
            )
            assert (None, None) == result_loan_call

            # No holds.
            result_hold_call = loan_fixture.manager.loans.get_patron_hold(
                loan_fixture.default_patron, pools
            )
            assert (None, None) == result_hold_call

            # When there's a loan, we retrieve it.
            loan, newly_created = loan_fixture.pool.loan_to(loan_fixture.default_patron)
            result_loan_call = loan_fixture.manager.loans.get_patron_loan(
                loan_fixture.default_patron, pools
            )
            assert (loan, loan_fixture.pool) == result_loan_call

            # When there's a hold, we retrieve it.
            hold, newly_created = other_pool.on_hold_to(loan_fixture.default_patron)
            result_hold_call = loan_fixture.manager.loans.get_patron_hold(
                loan_fixture.default_patron, pools
            )
            assert (hold, other_pool) == result_hold_call

    @pytest.mark.parametrize(
        *OPDSSerializationTestHelper.PARAMETRIZED_SINGLE_ENTRY_ACCEPT_HEADERS
    )
    def test_borrow_success(
        self,
        loan_fixture: LoanFixture,
        http_client: MockHttpClientFixture,
        accept_header: str | None,
        expected_content_type: str,
    ):
        # Create a loanable LicensePool.
        work = loan_fixture.db.work(
            with_license_pool=True, with_open_access_download=False
        )
        pool = work.license_pools[0]
        loan_fixture.manager.d_circulation.queue_checkout(
            pool,
            LoanInfo.from_license_pool(
                pool,
                start_date=utc_now(),
                end_date=utc_now() + datetime.timedelta(seconds=3600),
            ),
        )

        serialization_helper = OPDSSerializationTestHelper(
            accept_header, expected_content_type
        )
        headers = serialization_helper.merge_accept_header(
            {"Authorization": loan_fixture.valid_auth}
        )

        # Create a new loan.
        with loan_fixture.request_context_with_library("/", headers=headers):
            patron = loan_fixture.manager.loans.authenticated_patron_from_request()
            with patch(
                "palace.manager.api.controller.loan.sync_patron_activity"
            ) as sync_task:
                borrow_response = loan_fixture.manager.loans.borrow(
                    loan_fixture.identifier_type, loan_fixture.identifier_identifier
                )
            loan = get_one(
                loan_fixture.db.session, Loan, license_pool=loan_fixture.pool
            )

            # A new loan should return a 201 status.
            assert isinstance(borrow_response, FlaskResponse)
            assert 201 == borrow_response.status_code

            # And queue up a task to sync the patron's activity.
            assert isinstance(patron, Patron)
            sync_task.apply_async.assert_called_once_with(
                (
                    loan_fixture.pool.collection.id,
                    patron.id,
                    loan_fixture.valid_credentials["password"],
                ),
                {"force": True},
                countdown=5,
            )

            # A loan has been created for this license pool.
            assert loan is not None
            # The loan has yet to be fulfilled.
            assert loan.fulfillment is None

            # We've been given an OPDS feed with one entry, which tells us how
            # to fulfill the license.
            new_feed_content = borrow_response.get_data()

        # Borrow again with an existing loan.
        with loan_fixture.request_context_with_library("/", headers=headers):
            loan_fixture.manager.loans.authenticated_patron_from_request()
            with patch(
                "palace.manager.api.controller.loan.sync_patron_activity"
            ) as sync_task:
                borrow_response = loan_fixture.manager.loans.borrow(
                    loan_fixture.identifier_type, loan_fixture.identifier_identifier
                )

            # A loan has been created for this license pool.
            loan = get_one(
                loan_fixture.db.session, Loan, license_pool=loan_fixture.pool
            )
            # An existing loan should return a 200 status.
            assert isinstance(borrow_response, OPDSEntryResponse)
            assert 200 == borrow_response.status_code

            # Because the loan was existing, we didn't queue up a task to sync the patron's activity.
            sync_task.apply_async.assert_not_called()

            # There is still a loan that has not yet been fulfilled.
            assert loan is not None
            assert loan.fulfillment is None

            # We've been given an OPDS feed with one entry, which tells us how
            # to fulfill the license.
            existing_feed_content = borrow_response.get_data()

            # The new loan feed should look the same as the existing loan feed.
            assert new_feed_content == existing_feed_content

            feed_links = serialization_helper.verify_and_get_single_entry_feed_links(
                borrow_response
            )

            fulfillment_links = [
                x["href"] for x in feed_links if x["rel"] == OPDSFeed.ACQUISITION_REL
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
            redirect = RedirectFulfillment(
                content_link=fulfillable_mechanism.resource.representation.public_url,
                content_type=fulfillable_mechanism.resource.representation.media_type,
            )
            loan_fixture.manager.d_circulation.queue_fulfill(
                loan_fixture.pool, redirect
            )

            fulfill_response = loan_fixture.manager.loans.fulfill(
                loan_fixture.pool_id,
                fulfillable_mechanism.delivery_mechanism.id,
            )
            if isinstance(fulfill_response, ProblemDetail):
                j, status, headers = fulfill_response.response
                raise Exception(repr(j))
            assert 302 == fulfill_response.status_code
            assert (
                fulfillable_mechanism.resource.representation.public_url
                == fulfill_response.headers.get("Location")
            )

            # The mechanism we used has been registered with the loan.
            assert fulfillable_mechanism == loan.fulfillment

            # Set the pool to be non-open-access, so we have to make an
            # external request to obtain the book.
            loan_fixture.pool.open_access = False

            assert fulfillable_mechanism.resource.url is not None
            fetch = FetchFulfillment(
                content_link=fulfillable_mechanism.resource.url,
                content_type=fulfillable_mechanism.resource.representation.media_type,
            )

            # Now that we've set a mechanism, we can fulfill the loan
            # again without specifying a mechanism.
            loan_fixture.manager.d_circulation.queue_fulfill(loan_fixture.pool, fetch)
            http_client.queue_response(200, content="I am an ACSM file")

            fulfill_response = loan_fixture.manager.loans.fulfill(loan_fixture.pool_id)
            assert isinstance(fulfill_response, wkResponse)
            assert 200 == fulfill_response.status_code
            assert "I am an ACSM file" == fulfill_response.get_data(as_text=True)
            assert http_client.requests == [fulfillable_mechanism.resource.url]

            # But we can't use some other mechanism -- we're stuck with
            # the first one we chose.
            fulfill_response = loan_fixture.manager.loans.fulfill(
                loan_fixture.pool_id, loan_fixture.mech2.delivery_mechanism.id
            )
            assert isinstance(fulfill_response, ProblemDetail)
            assert 409 == fulfill_response.status_code
            assert fulfill_response.detail is not None
            assert (
                "You already fulfilled this loan as application/epub+zip (DRM Scheme 1), you can't also do it as application/pdf (DRM Scheme 2)"
                in fulfill_response.detail
            )

            # If the remote server fails, we get a problem detail.
            doomed_fulfillment = create_autospec(Fulfillment)
            doomed_fulfillment.response.side_effect = RemoteIntegrationException(
                "fulfill service", "Error!"
            )

            loan_fixture.manager.d_circulation.queue_fulfill(
                loan_fixture.pool, doomed_fulfillment
            )

            fulfill_response = loan_fixture.manager.loans.fulfill(loan_fixture.pool_id)
            assert isinstance(fulfill_response, ProblemDetail)
            assert 502 == fulfill_response.status_code

    def test_borrow_and_fulfill_with_streaming_delivery_mechanism(
        self,
        loan_fixture: LoanFixture,
        http_client: MockHttpClientFixture,
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
                LoanInfo.from_license_pool(
                    pool,
                    start_date=utc_now(),
                    end_date=utc_now() + datetime.timedelta(seconds=3600),
                ),
            )
            with patch(
                "palace.manager.api.controller.loan.sync_patron_activity"
            ) as sync_task:
                borrow_response = loan_fixture.manager.loans.borrow(
                    identifier.type, identifier.identifier
                )
            assert isinstance(borrow_response, Response)

            sync_task.apply_async.assert_called_once()

            # A loan has been created for this license pool.
            loan = get_one(loan_fixture.db.session, Loan, license_pool=pool)
            assert loan is not None
            # The loan has yet to be fulfilled.
            assert None == loan.fulfillment

            # We've been given an OPDS feed with two delivery mechanisms, which tell us how
            # to fulfill the license.
            assert 201 == borrow_response.status_code
            feed = feedparser.parse(borrow_response.get_data())
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
                RedirectFulfillment(
                    "http://streaming-content-link",
                    Representation.TEXT_HTML_MEDIA_TYPE
                    + DeliveryMechanism.STREAMING_PROFILE,
                ),
            )
            fulfill_response = loan_fixture.manager.loans.fulfill(
                pool.id, streaming_mechanism.delivery_mechanism.id
            )
            assert isinstance(fulfill_response, Response)

            # We get an OPDS entry.
            assert 200 == fulfill_response.status_code
            opds_entries = feedparser.parse(fulfill_response.get_data())["entries"]
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
            http_client.queue_response(200, content="I am an ACSM file")

            loan_fixture.manager.d_circulation.queue_fulfill(
                pool,
                FetchFulfillment(
                    "http://other-content-link",
                    Representation.TEXT_HTML_MEDIA_TYPE,
                ),
            )
            fulfill_response = loan_fixture.manager.loans.fulfill(
                pool.id, mech1.delivery_mechanism.id
            )
            assert isinstance(fulfill_response, wkResponse)
            assert 200 == fulfill_response.status_code

            # Now the fulfillment has been set to the other mechanism.
            assert mech1 == loan.fulfillment

            # But we can still fulfill the streaming mechanism again.
            loan_fixture.manager.d_circulation.queue_fulfill(
                pool,
                RedirectFulfillment(
                    "http://streaming-content-link",
                    Representation.TEXT_HTML_MEDIA_TYPE
                    + DeliveryMechanism.STREAMING_PROFILE,
                ),
            )

            fulfill_response = loan_fixture.manager.loans.fulfill(
                pool.id, streaming_mechanism.delivery_mechanism.id
            )
            assert isinstance(fulfill_response, Response)
            assert 200 == fulfill_response.status_code
            opds_entries = feedparser.parse(fulfill_response.get_data())["entries"]
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
                loan_fixture.identifier_type, loan_fixture.identifier_identifier, -100
            )
            assert BAD_DELIVERY_MECHANISM == response

    def test_borrow_creates_hold_when_no_available_copies(
        self, loan_fixture: LoanFixture
    ):
        threem_edition, pool = loan_fixture.db.edition(
            with_open_access_download=False,
            data_source_name=DataSource.BIBLIOTHECA,
            identifier_type=Identifier.BIBLIOTHECA_ID,
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
                HoldInfo.from_license_pool(
                    pool,
                    start_date=utc_now(),
                    end_date=utc_now() + datetime.timedelta(seconds=3600),
                    hold_position=1,
                ),
            )
            response = loan_fixture.manager.loans.borrow(
                pool.identifier.type, pool.identifier.identifier
            )
            assert isinstance(response, wkResponse)
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
            assert isinstance(response, ProblemDetail)
            assert 404 == response.status_code
            assert NO_LICENSES == response

    def test_borrow_creates_local_hold_if_remote_hold_exists(
        self, loan_fixture: LoanFixture
    ):
        """We try to check out a book, but turns out we already have it
        on hold.
        """
        threem_edition, pool = loan_fixture.db.edition(
            with_open_access_download=False,
            data_source_name=DataSource.BIBLIOTHECA,
            identifier_type=Identifier.BIBLIOTHECA_ID,
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
                HoldInfo.from_license_pool(
                    pool,
                    start_date=utc_now(),
                    end_date=utc_now() + datetime.timedelta(seconds=3600),
                    hold_position=1,
                ),
            )
            response = loan_fixture.manager.loans.borrow(
                pool.identifier.type, pool.identifier.identifier
            )
            assert isinstance(response, wkResponse)
            assert 201 == response.status_code

            # A hold has been created for this license pool.
            hold = get_one(loan_fixture.db.session, Hold, license_pool=pool)
            assert hold != None

    def test_borrow_fails_when_work_not_present_on_remote(
        self, loan_fixture: LoanFixture
    ):
        threem_edition, pool = loan_fixture.db.edition(
            with_open_access_download=False,
            data_source_name=DataSource.BIBLIOTHECA,
            identifier_type=Identifier.BIBLIOTHECA_ID,
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
            assert isinstance(response, ProblemDetail)
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
                loan_fixture.identifier_type, loan_fixture.identifier_identifier
            )

            # No checkout request was actually made to the remote.
            assert 1 == len(mock_remote.responses["checkout"])

            # We got an OPDS entry that includes at least one
            # fulfillment link, which is what we expect when we ask
            # about an active loan.
            assert isinstance(response, wkResponse)
            assert 200 == response.status_code
            [entry] = feedparser.parse(response.get_data())["entries"]
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
        mock = MagicMock(spec=CirculationApiDispatcher)
        controller.manager.circulation_apis[loan_fixture.db.default_library().id] = mock

        with loan_fixture.request_context_with_library(
            "/?modulus=modulus&exponent=exponent&device_id=device_id",
            headers={
                "Authorization": loan_fixture.valid_auth,
                "X-Forwarded-For": "1.1.1.1, 0.0.0.0",
            },
            environ_base={"REMOTE_ADDR": "5.5.5.5"},
        ):
            authenticated = controller.authenticated_patron_from_request()
            assert isinstance(authenticated, Patron)
            loan, ignore = loan_fixture.pool.loan_to(authenticated)

            # Try to fulfill the loan.
            controller.fulfill(
                loan_fixture.pool_id, loan_fixture.mech2.delivery_mechanism.id
            )

            # Verify that the right arguments were passed into
            # CirculationAPI.
            mock.fulfill.assert_called_once_with(
                authenticated,
                loan_fixture.valid_credentials["password"],
                loan_fixture.pool,
                loan_fixture.mech2,
                modulus="modulus",
                exponent="exponent",
                device_id="device_id",
                client_ip="1.1.1.1",
            )

        # Test with no x-forwarded-for header set.
        mock.reset_mock()
        with loan_fixture.request_context_with_library(
            "/?modulus=modulus&exponent=exponent&device_id=device_id",
            headers={
                "Authorization": loan_fixture.valid_auth,
            },
            environ_base={"REMOTE_ADDR": "5.5.5.5"},
        ):
            # Try to fulfill the loan.
            controller.fulfill(
                loan_fixture.pool_id, loan_fixture.mech2.delivery_mechanism.id
            )

            # Verify that the right arguments were passed into
            # CirculationAPI.
            mock.fulfill.assert_called_once_with(
                authenticated,
                loan_fixture.valid_credentials["password"],
                loan_fixture.pool,
                loan_fixture.mech2,
                modulus="modulus",
                exponent="exponent",
                device_id="device_id",
                client_ip="5.5.5.5",
            )

    @pytest.mark.parametrize(
        "response_value",
        [
            Response(status=200, response="Here's your response"),
            Response(status=401, response="Error"),
            Response(status=500, response="Fault"),
        ],
    )
    def test_fulfill_returns_fulfillment(
        self, response_value: Response, loan_fixture: LoanFixture
    ):
        # When CirculationAPI.fulfill returns a Fulfillment, we
        # simply return the result of Fulfillment.response()
        class MockFulfillment(Fulfillment):
            def __init__(self):
                self.response_called = False

            def response(self) -> Response:
                self.response_called = True
                return response_value

        fulfillment = MockFulfillment()

        class MockCirculationAPI:
            def fulfill(self, *args, **kwargs):
                return fulfillment

        controller = loan_fixture.manager.loans
        mock = MockCirculationAPI()
        controller.manager.circulation_apis[loan_fixture.db.default_library().id] = mock

        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            authenticated = controller.authenticated_patron_from_request()
            assert isinstance(authenticated, Patron)
            loan, ignore = loan_fixture.pool.loan_to(authenticated)

            # Fulfill the loan.
            result = controller.fulfill(
                loan_fixture.pool_id, loan_fixture.mech2.delivery_mechanism.id
            )

            # The result of MockFulfillment.response was
            # returned directly.
            assert response_value == result

    def test_fulfill_without_active_loan(self, loan_fixture: LoanFixture):
        controller = loan_fixture.manager.loans

        # Most of the time, it is not possible to fulfill a title if the
        # patron has no active loan for the title. This might be
        # because the patron never checked out the book...
        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            controller.authenticated_patron_from_request()
            response = controller.fulfill(
                loan_fixture.pool_id, loan_fixture.mech2.delivery_mechanism.id
            )
            assert isinstance(response, ProblemDetail)
            assert NO_ACTIVE_LOAN.uri == response.uri

        # ...or it might be because there is no authenticated patron.
        with loan_fixture.request_context_with_library("/"):
            response = controller.fulfill(
                loan_fixture.pool_id, loan_fixture.mech2.delivery_mechanism.id
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
                loan_fixture.pool_id, loan_fixture.mech2.delivery_mechanism.id
            )
            assert INTEGRATION_ERROR == problem
        controller.authenticated_patron_from_request = old_authenticated_patron

        # However, if can_fulfill_without_loan returns True, then
        # fulfill() will be called. If fulfill() returns a
        # Fulfillment, then the title is fulfilled, with no loan
        # having been created.
        #
        # To that end, we'll mock can_fulfill_without_loan and fulfill.
        def mock_can_fulfill_without_loan(*args, **kwargs):
            return True

        def mock_fulfill(*args, **kwargs):
            return DirectFulfillment(
                content_type="text/html",
                content="here's your book",
            )

        # Now we're able to fulfill the book even without
        # authenticating a patron.
        with loan_fixture.request_context_with_library("/"):
            controller.can_fulfill_without_loan = mock_can_fulfill_without_loan
            controller.circulation.fulfill = mock_fulfill
            response = controller.fulfill(
                loan_fixture.pool_id, loan_fixture.mech2.delivery_mechanism.id
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
            assert isinstance(authenticated, Patron)
            loan_fixture.pool.loan_to(authenticated)
            with (
                patch(
                    "palace.manager.api.controller.opds_feed.OPDSAcquisitionFeed.single_entry_loans_feed"
                ) as feed,
                patch.object(circulation, "fulfill") as fulfill,
            ):
                fulfill.return_value = MagicMock(spec=RedirectFulfillment)
                # The single_item_feed must return this error
                feed.return_value = NOT_FOUND_ON_REMOTE
                # The content type needs to be streaming
                loan_fixture.mech1.delivery_mechanism.content_type = (
                    DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE
                )

                response = controller.fulfill(
                    loan_fixture.pool_id, loan_fixture.mech1.delivery_mechanism.id
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
        api.fulfill.return_value = RedirectFulfillment(
            "https://example.org/redirect_to_epub",
            MediaTypes.EPUB_MEDIA_TYPE,
        )
        controller.can_fulfill_without_loan = MagicMock(return_value=False)
        controller.authenticated_patron_from_request = MagicMock(return_value=patron)

        with loan_fixture.request_context_with_library(
            "/",
            library=loan_fixture.db.default_library(),
            headers=dict(Authorization=loan_fixture.valid_auth),
        ):
            controller.circulation.api_for_license_pool = MagicMock(return_value=api)
            assert isinstance(pool.id, int)
            response = controller.fulfill(pool.id, lpdm.delivery_mechanism.id)

        assert isinstance(response, wkResponse)
        assert response.status_code == 302
        assert response.location == "https://example.org/redirect_to_epub"

    @pytest.mark.parametrize(
        *OPDSSerializationTestHelper.PARAMETRIZED_SINGLE_ENTRY_ACCEPT_HEADERS
    )
    def test_revoke_loan(
        self,
        loan_fixture: LoanFixture,
        accept_header: str | None,
        expected_content_type: str,
    ):
        serialization_helper = OPDSSerializationTestHelper(
            accept_header, expected_content_type
        )
        headers = serialization_helper.merge_accept_header(
            {"Authorization": loan_fixture.valid_auth}
        )

        # Create a loan and revoke it
        with loan_fixture.request_context_with_library("/", headers=headers):
            patron = loan_fixture.manager.loans.authenticated_patron_from_request()
            assert isinstance(patron, Patron)
            loan, newly_created = loan_fixture.pool.loan_to(patron)

            loan_fixture.manager.d_circulation.queue_checkin(loan_fixture.pool)

            with patch(
                "palace.manager.api.controller.loan.sync_patron_activity"
            ) as sync_task:
                response = loan_fixture.manager.loans.revoke(loan_fixture.pool_id)

        assert 200 == response.status_code
        serialization_helper.verify_and_get_single_entry_feed_links(response)

        # We queued up a sync_patron_activity_collection task
        sync_task.apply_async.assert_called_once_with(
            (
                loan_fixture.pool.collection.id,
                patron.id,
                loan_fixture.valid_credentials["password"],
            ),
            {"force": True},
            countdown=5,
        )

    @pytest.mark.parametrize(
        *OPDSSerializationTestHelper.PARAMETRIZED_SINGLE_ENTRY_ACCEPT_HEADERS
    )
    def test_revoke_loan_no_patron_activity_support(
        self,
        loan_fixture: LoanFixture,
        accept_header: str | None,
        expected_content_type: str,
    ):
        serialization_helper = OPDSSerializationTestHelper(
            accept_header, expected_content_type
        )
        headers = serialization_helper.merge_accept_header(
            {"Authorization": loan_fixture.valid_auth}
        )

        # Create a loan and revoke it
        with loan_fixture.request_context_with_library("/", headers=headers):
            patron = loan_fixture.manager.loans.authenticated_patron_from_request()
            assert isinstance(patron, Patron)
            loan, newly_created = loan_fixture.pool.loan_to(patron)

            mock_supports_patron_activity = create_autospec(
                loan_fixture.manager.d_circulation.supports_patron_activity,
                return_value=False,
            )
            loan_fixture.manager.d_circulation.supports_patron_activity = (
                mock_supports_patron_activity
            )
            loan_fixture.manager.d_circulation.queue_checkin(loan_fixture.pool)

            with patch(
                "palace.manager.api.controller.loan.sync_patron_activity"
            ) as sync_task:
                response = loan_fixture.manager.loans.revoke(loan_fixture.pool_id)

        assert 200 == response.status_code
        serialization_helper.verify_and_get_single_entry_feed_links(response)

        mock_supports_patron_activity.assert_called_once_with(loan_fixture.pool)
        sync_task.apply_async.assert_not_called()

    def test_revoke_loan_exception(
        self,
        loan_fixture: LoanFixture,
    ):
        # Revoke loan where an exception is raised from the circulation api
        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            loan_fixture.manager.d_circulation.revoke_loan = MagicMock(
                side_effect=CannotReturn()
            )
            patron = loan_fixture.manager.loans.authenticated_patron_from_request()
            assert isinstance(patron, Patron)
            loan_fixture.pool.loan_to(patron)
            with patch(
                "palace.manager.api.controller.loan.sync_patron_activity"
            ) as sync_task:
                response = loan_fixture.manager.loans.revoke(loan_fixture.pool_id)

        assert isinstance(response, ProblemDetail)
        assert response == COULD_NOT_MIRROR_TO_REMOTE

        # Because of the error we did not queue up a sync_patron_activity_collection task
        sync_task.apply_async.assert_not_called()

    def test_revoke_loan_licensepool_no_work(
        self,
        loan_fixture: LoanFixture,
    ):
        # Revoke loan where the license pool has no work
        with (
            loan_fixture.request_context_with_library(
                "/", headers=dict(Authorization=loan_fixture.valid_auth)
            ),
            patch(
                "palace.manager.api.controller.loan.sync_patron_activity"
            ) as sync_task,
        ):
            patron = loan_fixture.manager.loans.authenticated_patron_from_request()
            loan_fixture.manager.d_circulation.queue_checkin(loan_fixture.pool)
            assert isinstance(patron, Patron)
            loan_fixture.pool.loan_to(patron)
            loan_fixture.pool.work = None
            response = loan_fixture.manager.loans.revoke(loan_fixture.pool_id)

        assert isinstance(response, ProblemDetail)
        assert response == NOT_FOUND_ON_REMOTE

        # Because of the error we did not queue up a sync_patron_activity_collection task
        sync_task.apply_async.assert_not_called()

    @pytest.mark.parametrize(
        *OPDSSerializationTestHelper.PARAMETRIZED_SINGLE_ENTRY_ACCEPT_HEADERS
    )
    def test_revoke_hold(
        self,
        loan_fixture: LoanFixture,
        accept_header: str | None,
        expected_content_type: str,
    ):
        serialization_helper = OPDSSerializationTestHelper(
            accept_header, expected_content_type
        )
        headers = serialization_helper.merge_accept_header(
            {"Authorization": loan_fixture.valid_auth}
        )

        with loan_fixture.request_context_with_library("/", headers=headers):
            patron = loan_fixture.manager.loans.authenticated_patron_from_request()
            assert isinstance(patron, Patron)
            hold, newly_created = loan_fixture.pool.on_hold_to(patron, position=0)

            loan_fixture.manager.d_circulation.queue_release_hold(loan_fixture.pool)

            with patch(
                "palace.manager.api.controller.loan.sync_patron_activity"
            ) as sync_task:
                response = loan_fixture.manager.loans.revoke(loan_fixture.pool_id)

        assert 200 == response.status_code
        _ = serialization_helper.verify_and_get_single_entry_feed_links(response)

        # We queued up a sync_patron_activity_collection task
        sync_task.apply_async.assert_called_once_with(
            (
                loan_fixture.pool.collection.id,
                patron.id,
                loan_fixture.valid_credentials["password"],
            ),
            {"force": True},
            countdown=5,
        )

    def test_revoke_hold_exception(
        self,
        loan_fixture: LoanFixture,
    ):
        # Revoke hold where an exception is raised from the circulation api
        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            loan_fixture.manager.d_circulation.release_hold = MagicMock(
                side_effect=CannotReleaseHold()
            )
            patron = loan_fixture.manager.loans.authenticated_patron_from_request()
            assert isinstance(patron, Patron)
            loan_fixture.pool.on_hold_to(patron, position=0)
            with patch(
                "palace.manager.api.controller.loan.sync_patron_activity"
            ) as sync_task:
                response = loan_fixture.manager.loans.revoke(loan_fixture.pool_id)

        assert isinstance(response, ProblemDetail)
        assert response == CANNOT_RELEASE_HOLD

        # Because of the error we did not queue up a sync_patron_activity_collection task
        sync_task.apply_async.assert_not_called()

    def test_revoke_hold_nonexistent_licensepool(self, loan_fixture: LoanFixture):
        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            patron = loan_fixture.manager.loans.authenticated_patron_from_request()
            response = loan_fixture.manager.loans.revoke(-10)
            assert isinstance(response, ProblemDetail)
            assert INVALID_INPUT.uri == response.uri

    def test_hold_fails_when_holds_disallowed(self, loan_fixture: LoanFixture):
        edition, pool = loan_fixture.db.edition(with_license_pool=True)
        pool.open_access = False
        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            patron = loan_fixture.manager.loans.authenticated_patron_from_request()
            loan_fixture.manager.d_circulation.queue_checkout(pool, NoAvailableCopies())
            loan_fixture.manager.d_circulation.queue_hold(pool, HoldsNotPermitted())
            response = loan_fixture.manager.loans.borrow(
                pool.identifier.type, pool.identifier.identifier
            )
            assert isinstance(response, ProblemDetail)
            assert HOLDS_NOT_PERMITTED.uri == response.uri

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
            data_source_name=DataSource.BIBLIOTHECA,
            identifier_type=Identifier.BIBLIOTHECA_ID,
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
            assert isinstance(patron, Patron)
            patron.fines = Decimal("12345678.90")
            response = loan_fixture.manager.loans.borrow(
                pool.identifier.type, pool.identifier.identifier
            )
            assert isinstance(response, ProblemDetail)
            assert 403 == response.status_code
            assert OUTSTANDING_FINES.uri == response.uri
            assert response.detail is not None
            assert "$12345678.90 outstanding" in response.detail

        # Reduce the patron's fines, and there's no problem.
        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            patron = loan_fixture.manager.loans.authenticated_patron_from_request()
            assert isinstance(patron, Patron)
            patron.fines = Decimal("0.49")
            loan_fixture.manager.d_circulation.queue_checkout(
                pool,
                LoanInfo.from_license_pool(
                    pool,
                    start_date=utc_now(),
                    end_date=utc_now() + datetime.timedelta(seconds=3600),
                ),
            )
            response = loan_fixture.manager.loans.borrow(
                pool.identifier.type, pool.identifier.identifier
            )
            assert response is not None
            assert 201 == response.status_code

    def test_3m_cant_revoke_hold_if_reserved(self, loan_fixture: LoanFixture):
        threem_edition, pool = loan_fixture.db.edition(
            with_open_access_download=False,
            data_source_name=DataSource.BIBLIOTHECA,
            identifier_type=Identifier.BIBLIOTHECA_ID,
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
            assert isinstance(response, ProblemDetail)
            assert 400 == response.status_code
            assert CANNOT_RELEASE_HOLD.uri == response.uri
            assert (
                "Cannot release a hold once it enters reserved state."
                == response.detail
            )

    def test_active_loans(
        self,
        db: DatabaseTransactionFixture,
        loan_fixture: LoanFixture,
        redis_fixture: RedisFixture,
    ):
        # An OPDS feed is returned. This feed is empty because the patron has no loans.
        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            patron = loan_fixture.manager.loans.authenticated_patron_from_request()
            with patch(
                "palace.manager.api.controller.loan.sync_patron_activity"
            ) as sync_task:
                response = loan_fixture.manager.loans.sync()
        assert not "<entry>" in response.get_data(as_text=True)
        assert response.headers["Cache-Control"].startswith("private,")

        # We queued up a sync_patron_activity task to go sync the patrons information,
        # Only active collections are synced.
        assert isinstance(patron, Patron)
        patron_collections = patron.library.active_collections
        assert sync_task.apply_async.call_count == len(patron_collections)
        for collection in patron_collections:
            sync_task.apply_async.assert_any_call(
                (collection.id, patron.id, loan_fixture.valid_credentials["password"]),
            )

        # Set up some loans and holds
        overdrive_edition, overdrive_pool = db.edition(
            with_open_access_download=False,
            data_source_name=DataSource.OVERDRIVE,
            identifier_type=Identifier.OVERDRIVE_ID,
            with_license_pool=True,
        )
        overdrive_book = db.work(
            presentation_edition=overdrive_edition,
        )
        overdrive_pool.open_access = False
        now = utc_now()
        overdrive_pool.loan_to(patron, now, now + datetime.timedelta(seconds=3600))

        bibliotheca_edition, bibliotheca_pool = db.edition(
            with_open_access_download=False,
            data_source_name=DataSource.BIBLIOTHECA,
            identifier_type=Identifier.BIBLIOTHECA_ID,
            with_license_pool=True,
        )
        bibliotheca_book = db.work(
            presentation_edition=bibliotheca_edition,
        )
        bibliotheca_pool.licenses_available = 0
        bibliotheca_pool.open_access = False
        bibliotheca_pool.on_hold_to(
            patron, now, now + datetime.timedelta(seconds=3600), 0
        )

        # Add a collection, that doesn't need to be synced
        collection_already_synced = db.collection(library=patron.library)
        patron_activity = PatronActivity(
            redis_client=redis_fixture.client,
            collection=collection_already_synced,
            patron=patron,
            task_id="test",
        )
        patron_activity.lock()
        patron_activity.success()

        # The loans are returned in the feed.
        with loan_fixture.request_context_with_library(
            "/", headers=dict(Authorization=loan_fixture.valid_auth)
        ):
            loan_fixture.manager.loans.authenticated_patron_from_request()
            with patch(
                "palace.manager.api.controller.loan.sync_patron_activity"
            ) as sync_task:
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
            x for x in overdrive_links if x["rel"] == "http://opds-spec.org/acquisition"
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

        assert quote("%s/fulfill" % overdrive_pool.id) in fulfill_link
        assert quote("%s/revoke" % overdrive_pool.id) in revoke_link
        assert (
            quote(
                "%s/%s/borrow"
                % (
                    bibliotheca_pool.identifier.type,
                    bibliotheca_pool.identifier.identifier,
                )
            )
            in borrow_link
        )
        assert 0 == len(bibliotheca_revoke_links)

        # We queued up a sync_patron_activity task to go sync the patrons information,
        # but only for the collections that needed to be synced
        assert sync_task.apply_async.call_count == 1
        sync_task.apply_async.assert_any_call(
            (
                loan_fixture.collection.id,
                patron.id,
                loan_fixture.valid_credentials["password"],
            ),
        )

    @pytest.mark.parametrize(
        "refresh,expected_sync_call_count",
        [
            ["true", 1],
            ["abc", 1],
            ["t", 1],
            ["1", 1],
            [None, 1],
            ["false", 0],
            ["f", 0],
            ["0", 0],
        ],
    )
    def test_loans_refresh(
        self,
        loan_fixture: LoanFixture,
        redis_fixture: RedisFixture,
        refresh: str | None,
        expected_sync_call_count: int,
    ):
        url = f"loans/?refresh={refresh}" if refresh is not None else "/"
        with (
            loan_fixture.request_context_with_library(
                url, headers=dict(Authorization=loan_fixture.valid_auth)
            ),
            patch(
                "palace.manager.api.controller.loan.sync_patron_activity"
            ) as sync_task,
        ):
            loan_fixture.manager.loans.authenticated_patron_from_request()
            loan_fixture.manager.loans.sync()
            assert sync_task.apply_async.call_count == expected_sync_call_count

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
                OPDSAPI,
                None,
                None,
            ],  # DB and OPDS response loan duration mismatch
            [
                # Loan duration < CM STANDARD_DEFAULT_LOAN_PERIOD, collection without configured loan period
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 1,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 1,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 1,
                OPDSAPI,
                None,
                None,
            ],
            [
                # Loan duration > CM STANDARD_DEFAULT_LOAN_PERIOD, collection without configured loan period
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 1,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 1,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 1,
                OPDSAPI,
                None,
                None,
            ],
            [
                # Loan without duration, collection loan period < CM STANDARD_DEFAULT_LOAN_PERIOD
                None,
                None,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 2,
                BibliothecaAPI,
                DataSource.BIBLIOTHECA,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 2,
            ],  # DB and OPDS response loan duration mismatch
            [
                # Loan duration < collection loan period < CM STANDARD_DEFAULT_LOAN_PERIOD
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 3,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 3,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 3,
                BibliothecaAPI,
                DataSource.BIBLIOTHECA,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 2,
            ],
            [
                # Collection loan period < loan duration < CM STANDARD_DEFAULT_LOAN_PERIOD
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 1,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 1,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 1,
                BibliothecaAPI,
                DataSource.BIBLIOTHECA,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 2,
            ],
            [
                # Collection loan period < CM STANDARD_DEFAULT_LOAN_PERIOD < loan duration
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 1,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 1,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 1,
                BibliothecaAPI,
                DataSource.BIBLIOTHECA,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 2,
            ],
            [
                # Loan without duration, CM STANDARD_DEFAULT_LOAN_PERIOD < collection loan period
                None,
                None,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 2,
                BibliothecaAPI,
                DataSource.BIBLIOTHECA,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 2,
            ],  # DB and OPDS response loan duration mismatch
            [
                # Loan duration < CM STANDARD_DEFAULT_LOAN_PERIOD < collection loan period
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 1,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 1,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD - 1,
                BibliothecaAPI,
                DataSource.BIBLIOTHECA,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 2,
            ],
            [
                # CM STANDARD_DEFAULT_LOAN_PERIOD < loan duration < collection loan period
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 1,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 1,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 1,
                BibliothecaAPI,
                DataSource.BIBLIOTHECA,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 2,
            ],
            [
                # CM STANDARD_DEFAULT_LOAN_PERIOD < collection loan period < loan duration
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 3,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 3,
                Collection.STANDARD_DEFAULT_LOAN_PERIOD + 3,
                BibliothecaAPI,
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
        collection_protocol: type[BaseCirculationAPI[Any, Any]],
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
            )

            collection.associated_libraries.append(loan_fixture.db.default_library())
            if collection_default_loan_period:
                loan_fixture.db.integration_library_configuration(
                    collection.integration_configuration,
                    library=loan_fixture.db.default_library(),
                    settings=collection_protocol.library_settings_class()(
                        ebook_loan_duration=collection_default_loan_period
                    ),
                )

            def create_work_and_return_license_pool_and_loan_info(**kwargs):
                loan_start = kwargs.pop("loan_start", utc_now())
                loan_end = kwargs.pop("loan_end", None)

                work = loan_fixture.db.work(
                    with_license_pool=True, with_open_access_download=False, **kwargs
                )
                license_pool = work.license_pools[0]

                loan_info = LoanInfo.from_license_pool(
                    license_pool,
                    start_date=loan_start,
                    end_date=loan_end,
                )

                return license_pool, loan_info

            license_pool, loan_info = create_work_and_return_license_pool_and_loan_info(
                loan_start=loan_start,
                loan_end=loan_end,
                data_source_name=collection_data_source_name,
                collection=collection,
            )

            loan_fixture.manager.d_circulation.queue_checkout(license_pool, loan_info)

            with patch("palace.manager.api.controller.loan.sync_patron_activity"):
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
