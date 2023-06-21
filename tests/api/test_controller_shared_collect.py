import datetime
import json
from contextlib import contextmanager

import feedparser
import flask
import pytest
from werkzeug.datastructures import ImmutableMultiDict

from api.circulation import FulfillmentInfo
from api.circulation_exceptions import (
    AuthorizationFailedException,
    CannotFulfill,
    CannotLoan,
    CannotReleaseHold,
    CannotReturn,
    InvalidInputException,
    NoAvailableCopies,
    NotCheckedOut,
    NotOnHold,
    RemoteInitiatedServerError,
)
from api.problem_details import (
    BAD_DELIVERY_MECHANISM,
    CANNOT_FULFILL,
    CANNOT_RELEASE_HOLD,
    CHECKOUT_FAILED,
    COULD_NOT_MIRROR_TO_REMOTE,
    HOLD_NOT_FOUND,
    INVALID_CREDENTIALS,
    INVALID_REGISTRATION,
    LOAN_NOT_FOUND,
    NO_ACTIVE_HOLD,
    NO_ACTIVE_LOAN,
    NO_AVAILABLE_LICENSE,
    NO_LICENSES,
    NO_SUCH_COLLECTION,
)
from core.model import (
    Collection,
    Hold,
    IntegrationClient,
    LicensePool,
    LicensePoolDeliveryMechanism,
    Loan,
    Work,
    create,
)
from core.problem_details import INTEGRATION_ERROR
from core.util.datetime_helpers import utc_now
from core.util.http import RemoteIntegrationException
from core.util.string_helpers import base64
from tests.core.mock import MockRequestsResponse
from tests.fixtures.api_controller import ControllerFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.vendor_id import VendorIDFixture


class SharedCollectionFixture(ControllerFixture):
    collection: Collection
    client: IntegrationClient
    work: Work
    pool: LicensePool
    delivery_mechanism: LicensePoolDeliveryMechanism

    def __init__(
        self, db: DatabaseTransactionFixture, vendor_id_fixture: VendorIDFixture
    ):
        super().__init__(db, vendor_id_fixture, setup_cm=False)
        from api.odl import ODLAPI

        self.collection = db.collection(protocol=ODLAPI.NAME)
        self.collection.integration_configuration.settings = dict(
            username="username",
            password="password",
            data_source="data_source",
            passphrase_hint="Try Me!",
            passphrase_hint_url="http://hint.url",
        )
        db.default_library().collections = [self.collection]
        self.client, ignore = IntegrationClient.register(
            db.session, "http://library.org"
        )
        self.app.manager = self.circulation_manager_setup()
        self.work = db.work(with_license_pool=True, collection=self.collection)
        self.pool = self.work.license_pools[0]
        [self.delivery_mechanism] = self.pool.delivery_mechanisms


@pytest.fixture(scope="function")
def shared_fixture(db: DatabaseTransactionFixture, vendor_id_fixture: VendorIDFixture):
    return SharedCollectionFixture(db, vendor_id_fixture)


class TestSharedCollectionController:
    """Test that other circ managers can register to borrow books
    from a shared collection."""

    @contextmanager
    def request_context_with_client(
        self, shared_fixture: SharedCollectionFixture, route, *args, **kwargs
    ):
        if "client" in kwargs:
            client = kwargs.pop("client")
        else:
            client = shared_fixture.client
        if "headers" in kwargs:
            headers = kwargs.pop("headers")
        else:
            headers = dict()
        headers["Authorization"] = "Bearer " + base64.b64encode(client.shared_secret)
        kwargs["headers"] = headers
        with shared_fixture.app.test_request_context(route, *args, **kwargs) as c:
            yield c

    def test_info(self, shared_fixture: SharedCollectionFixture):
        with shared_fixture.app.test_request_context("/"):
            collection = shared_fixture.manager.shared_collection_controller.info(
                shared_fixture.db.fresh_str()
            )
            assert NO_SUCH_COLLECTION == collection

            response = shared_fixture.manager.shared_collection_controller.info(
                shared_fixture.collection.name
            )
            assert 200 == response.status_code
            assert response.headers.get("Content-Type").startswith(
                "application/opds+json"
            )
            links = json.loads(response.get_data(as_text=True)).get("links")
            [register_link] = [link for link in links if link.get("rel") == "register"]
            assert (
                "/collections/%s/register" % shared_fixture.collection.name
                in register_link.get("href")
            )

    def test_load_collection(self, shared_fixture: SharedCollectionFixture):
        with shared_fixture.app.test_request_context("/"):
            collection = (
                shared_fixture.manager.shared_collection_controller.load_collection(
                    shared_fixture.db.fresh_str()
                )
            )
            assert NO_SUCH_COLLECTION == collection

            collection = (
                shared_fixture.manager.shared_collection_controller.load_collection(
                    shared_fixture.collection.name
                )
            )
            assert shared_fixture.collection == collection

    def test_register(self, shared_fixture: SharedCollectionFixture):
        with shared_fixture.app.test_request_context("/"):
            api = (
                shared_fixture.app.manager.shared_collection_controller.shared_collection
            )
            flask.request.form = ImmutableMultiDict([("url", "http://test")])

            api.queue_register(InvalidInputException())
            response = shared_fixture.manager.shared_collection_controller.register(
                shared_fixture.collection.name
            )
            assert 400 == response.status_code
            assert INVALID_REGISTRATION.uri == response.uri

            api.queue_register(AuthorizationFailedException())
            response = shared_fixture.manager.shared_collection_controller.register(
                shared_fixture.collection.name
            )
            assert 401 == response.status_code
            assert INVALID_CREDENTIALS.uri == response.uri

            api.queue_register(RemoteInitiatedServerError("Error", "Service"))
            response = shared_fixture.manager.shared_collection_controller.register(
                shared_fixture.collection.name
            )
            assert 502 == response.status_code
            assert INTEGRATION_ERROR.uri == response.uri

            api.queue_register(dict(shared_secret="secret"))
            response = shared_fixture.manager.shared_collection_controller.register(
                shared_fixture.collection.name
            )
            assert 200 == response.status_code
            assert "secret" == json.loads(response.get_data(as_text=True)).get(
                "shared_secret"
            )

    def test_loan_info(self, shared_fixture: SharedCollectionFixture):
        now = utc_now()
        tomorrow = utc_now() + datetime.timedelta(days=1)

        other_client, ignore = IntegrationClient.register(
            shared_fixture.db.session, "http://otherlibrary"
        )
        other_client_loan, ignore = create(
            shared_fixture.db.session,
            Loan,
            license_pool=shared_fixture.pool,
            integration_client=other_client,
        )

        ignore, other_pool = shared_fixture.db.edition(
            with_license_pool=True,
            collection=shared_fixture.db.collection(),
        )
        other_pool_loan, ignore = create(
            shared_fixture.db.session,
            Loan,
            license_pool=other_pool,
            integration_client=shared_fixture.client,
        )

        loan, ignore = create(
            shared_fixture.db.session,
            Loan,
            license_pool=shared_fixture.pool,
            integration_client=shared_fixture.client,
            start=now,
            end=tomorrow,
        )
        with self.request_context_with_client(shared_fixture, "/"):
            # This loan doesn't exist.
            response = shared_fixture.manager.shared_collection_controller.loan_info(
                shared_fixture.collection.name, 1234567
            )
            assert LOAN_NOT_FOUND == response

            # This loan belongs to a different library.
            response = shared_fixture.manager.shared_collection_controller.loan_info(
                shared_fixture.collection.name, other_client_loan.id
            )
            assert LOAN_NOT_FOUND == response

            # This loan's pool belongs to a different collection.
            response = shared_fixture.manager.shared_collection_controller.loan_info(
                shared_fixture.collection.name, other_pool_loan.id
            )
            assert LOAN_NOT_FOUND == response

            # This loan is ours.
            response = shared_fixture.manager.shared_collection_controller.loan_info(
                shared_fixture.collection.name, loan.id
            )
            assert 200 == response.status_code
            feed = feedparser.parse(response.data)
            [entry] = feed.get("entries")
            availability = entry.get("opds_availability")
            since = availability.get("since")
            until = availability.get("until")
            assert datetime.datetime.strftime(now, "%Y-%m-%dT%H:%M:%S+00:00") == since
            assert (
                datetime.datetime.strftime(tomorrow, "%Y-%m-%dT%H:%M:%S+00:00") == until
            )
            [revoke_url] = [
                link.get("href")
                for link in entry.get("links")
                if link.get("rel") == "http://librarysimplified.org/terms/rel/revoke"
            ]
            assert (
                f"/collections/{shared_fixture.collection.name}/loans/{loan.id}/revoke"
                in revoke_url
            )
            [fulfill_url] = [
                link.get("href")
                for link in entry.get("links")
                if link.get("rel") == "http://opds-spec.org/acquisition"
            ]
            assert (
                "/collections/%s/loans/%s/fulfill/%s"
                % (
                    shared_fixture.collection.name,
                    loan.id,
                    shared_fixture.delivery_mechanism.delivery_mechanism.id,
                )
                in fulfill_url
            )
            [self_url] = [
                link.get("href")
                for link in entry.get("links")
                if link.get("rel") == "self"
            ]
            assert f"/collections/{shared_fixture.collection.name}/loans/{loan.id}"

    def test_borrow(self, shared_fixture: SharedCollectionFixture):
        now = utc_now()
        tomorrow = utc_now() + datetime.timedelta(days=1)
        loan, ignore = create(
            shared_fixture.db.session,
            Loan,
            license_pool=shared_fixture.pool,
            integration_client=shared_fixture.client,
            start=now,
            end=tomorrow,
        )

        hold, ignore = create(
            shared_fixture.db.session,
            Hold,
            license_pool=shared_fixture.pool,
            integration_client=shared_fixture.client,
            start=now,
            end=tomorrow,
        )

        no_pool = shared_fixture.db.identifier()
        with self.request_context_with_client(shared_fixture, "/"):
            response = shared_fixture.manager.shared_collection_controller.borrow(
                shared_fixture.collection.name, no_pool.type, no_pool.identifier, None
            )
            assert NO_LICENSES.uri == response.uri

            api = (
                shared_fixture.app.manager.shared_collection_controller.shared_collection
            )

            # Attempt to borrow without a previous hold.
            api.queue_borrow(AuthorizationFailedException())
            response = shared_fixture.manager.shared_collection_controller.borrow(
                shared_fixture.collection.name,
                shared_fixture.pool.identifier.type,
                shared_fixture.pool.identifier.identifier,
                None,
            )
            assert INVALID_CREDENTIALS.uri == response.uri

            api.queue_borrow(CannotLoan())
            response = shared_fixture.manager.shared_collection_controller.borrow(
                shared_fixture.collection.name,
                shared_fixture.pool.identifier.type,
                shared_fixture.pool.identifier.identifier,
                None,
            )
            assert CHECKOUT_FAILED.uri == response.uri

            api.queue_borrow(NoAvailableCopies())
            response = shared_fixture.manager.shared_collection_controller.borrow(
                shared_fixture.collection.name,
                shared_fixture.pool.identifier.type,
                shared_fixture.pool.identifier.identifier,
                None,
            )
            assert NO_AVAILABLE_LICENSE.uri == response.uri

            api.queue_borrow(RemoteIntegrationException("error!", "service"))
            response = shared_fixture.manager.shared_collection_controller.borrow(
                shared_fixture.collection.name,
                shared_fixture.pool.identifier.type,
                shared_fixture.pool.identifier.identifier,
                None,
            )
            assert INTEGRATION_ERROR.uri == response.uri

            api.queue_borrow(loan)
            response = shared_fixture.manager.shared_collection_controller.borrow(
                shared_fixture.collection.name,
                shared_fixture.pool.identifier.type,
                shared_fixture.pool.identifier.identifier,
                None,
            )
            assert 201 == response.status_code
            feed = feedparser.parse(response.data)
            [entry] = feed.get("entries")
            availability = entry.get("opds_availability")
            since = availability.get("since")
            until = availability.get("until")
            assert datetime.datetime.strftime(now, "%Y-%m-%dT%H:%M:%S+00:00") == since
            assert (
                datetime.datetime.strftime(tomorrow, "%Y-%m-%dT%H:%M:%S+00:00") == until
            )
            assert "available" == availability.get("status")
            [revoke_url] = [
                link.get("href")
                for link in entry.get("links")
                if link.get("rel") == "http://librarysimplified.org/terms/rel/revoke"
            ]
            assert (
                f"/collections/{shared_fixture.collection.name}/loans/{loan.id}/revoke"
                in revoke_url
            )
            [fulfill_url] = [
                link.get("href")
                for link in entry.get("links")
                if link.get("rel") == "http://opds-spec.org/acquisition"
            ]
            assert (
                "/collections/%s/loans/%s/fulfill/%s"
                % (
                    shared_fixture.collection.name,
                    loan.id,
                    shared_fixture.delivery_mechanism.delivery_mechanism.id,
                )
                in fulfill_url
            )
            [self_url] = [
                link.get("href")
                for link in entry.get("links")
                if link.get("rel") == "self"
            ]
            assert f"/collections/{shared_fixture.collection.name}/loans/{loan.id}"

            # Now try to borrow when we already have a previous hold.
            api.queue_borrow(AuthorizationFailedException())
            response = shared_fixture.manager.shared_collection_controller.borrow(
                shared_fixture.collection.name,
                shared_fixture.pool.identifier.type,
                shared_fixture.pool.identifier.identifier,
                hold.id,
            )
            assert INVALID_CREDENTIALS.uri == response.uri

            api.queue_borrow(CannotLoan())
            response = shared_fixture.manager.shared_collection_controller.borrow(
                shared_fixture.collection.name, None, None, hold.id
            )
            assert CHECKOUT_FAILED.uri == response.uri

            api.queue_borrow(NoAvailableCopies())
            response = shared_fixture.manager.shared_collection_controller.borrow(
                shared_fixture.collection.name, None, None, hold.id
            )
            assert NO_AVAILABLE_LICENSE.uri == response.uri

            api.queue_borrow(RemoteIntegrationException("error!", "service"))
            response = shared_fixture.manager.shared_collection_controller.borrow(
                shared_fixture.collection.name, None, None, hold.id
            )
            assert INTEGRATION_ERROR.uri == response.uri

            api.queue_borrow(loan)
            response = shared_fixture.manager.shared_collection_controller.borrow(
                shared_fixture.collection.name, None, None, hold.id
            )
            assert 201 == response.status_code
            feed = feedparser.parse(response.data)
            [entry] = feed.get("entries")
            availability = entry.get("opds_availability")
            since = availability.get("since")
            until = availability.get("until")
            assert "available" == availability.get("status")
            assert datetime.datetime.strftime(now, "%Y-%m-%dT%H:%M:%S+00:00") == since
            assert (
                datetime.datetime.strftime(tomorrow, "%Y-%m-%dT%H:%M:%S+00:00") == until
            )
            [revoke_url] = [
                link.get("href")
                for link in entry.get("links")
                if link.get("rel") == "http://librarysimplified.org/terms/rel/revoke"
            ]
            assert (
                f"/collections/{shared_fixture.collection.name}/loans/{loan.id}/revoke"
                in revoke_url
            )
            [fulfill_url] = [
                link.get("href")
                for link in entry.get("links")
                if link.get("rel") == "http://opds-spec.org/acquisition"
            ]
            assert (
                "/collections/%s/loans/%s/fulfill/%s"
                % (
                    shared_fixture.collection.name,
                    loan.id,
                    shared_fixture.delivery_mechanism.delivery_mechanism.id,
                )
                in fulfill_url
            )
            [self_url] = [
                link.get("href")
                for link in entry.get("links")
                if link.get("rel") == "self"
            ]
            assert f"/collections/{shared_fixture.collection.name}/loans/{loan.id}"

            # Now try to borrow, but actually get a hold.
            api.queue_borrow(hold)
            response = shared_fixture.manager.shared_collection_controller.borrow(
                shared_fixture.collection.name,
                shared_fixture.pool.identifier.type,
                shared_fixture.pool.identifier.identifier,
                None,
            )
            assert 201 == response.status_code
            feed = feedparser.parse(response.data)
            [entry] = feed.get("entries")
            availability = entry.get("opds_availability")
            since = availability.get("since")
            until = availability.get("until")
            assert datetime.datetime.strftime(now, "%Y-%m-%dT%H:%M:%S+00:00") == since
            assert (
                datetime.datetime.strftime(tomorrow, "%Y-%m-%dT%H:%M:%S+00:00") == until
            )
            assert "reserved" == availability.get("status")
            [revoke_url] = [
                link.get("href")
                for link in entry.get("links")
                if link.get("rel") == "http://librarysimplified.org/terms/rel/revoke"
            ]
            assert (
                f"/collections/{shared_fixture.collection.name}/holds/{hold.id}/revoke"
                in revoke_url
            )
            assert [] == [
                link.get("href")
                for link in entry.get("links")
                if link.get("rel") == "http://opds-spec.org/acquisition"
            ]
            [self_url] = [
                link.get("href")
                for link in entry.get("links")
                if link.get("rel") == "self"
            ]
            assert f"/collections/{shared_fixture.collection.name}/holds/{hold.id}"

    def test_revoke_loan(self, shared_fixture: SharedCollectionFixture):
        now = utc_now()
        tomorrow = utc_now() + datetime.timedelta(days=1)
        loan, ignore = create(
            shared_fixture.db.session,
            Loan,
            license_pool=shared_fixture.pool,
            integration_client=shared_fixture.client,
            start=now,
            end=tomorrow,
        )

        other_client, ignore = IntegrationClient.register(
            shared_fixture.db.session, "http://otherlibrary"
        )
        other_client_loan, ignore = create(
            shared_fixture.db.session,
            Loan,
            license_pool=shared_fixture.pool,
            integration_client=other_client,
        )

        ignore, other_pool = shared_fixture.db.edition(
            with_license_pool=True,
            collection=shared_fixture.db.collection(),
        )
        other_pool_loan, ignore = create(
            shared_fixture.db.session,
            Loan,
            license_pool=other_pool,
            integration_client=shared_fixture.client,
        )

        with self.request_context_with_client(shared_fixture, "/"):
            response = shared_fixture.manager.shared_collection_controller.revoke_loan(
                shared_fixture.collection.name, other_pool_loan.id
            )
            assert LOAN_NOT_FOUND.uri == response.uri

            response = shared_fixture.manager.shared_collection_controller.revoke_loan(
                shared_fixture.collection.name, other_client_loan.id
            )
            assert LOAN_NOT_FOUND.uri == response.uri

            api = (
                shared_fixture.app.manager.shared_collection_controller.shared_collection
            )

            api.queue_revoke_loan(AuthorizationFailedException())
            response = shared_fixture.manager.shared_collection_controller.revoke_loan(
                shared_fixture.collection.name, loan.id
            )
            assert INVALID_CREDENTIALS.uri == response.uri

            api.queue_revoke_loan(CannotReturn())
            response = shared_fixture.manager.shared_collection_controller.revoke_loan(
                shared_fixture.collection.name, loan.id
            )
            assert COULD_NOT_MIRROR_TO_REMOTE.uri == response.uri

            api.queue_revoke_loan(NotCheckedOut())
            response = shared_fixture.manager.shared_collection_controller.revoke_loan(
                shared_fixture.collection.name, loan.id
            )
            assert NO_ACTIVE_LOAN.uri == response.uri

    def test_fulfill(self, shared_fixture: SharedCollectionFixture):
        now = utc_now()
        tomorrow = utc_now() + datetime.timedelta(days=1)
        loan, ignore = create(
            shared_fixture.db.session,
            Loan,
            license_pool=shared_fixture.pool,
            integration_client=shared_fixture.client,
            start=now,
            end=tomorrow,
        )

        ignore, other_pool = shared_fixture.db.edition(
            with_license_pool=True,
            collection=shared_fixture.db.collection(),
        )
        other_pool_loan, ignore = create(
            shared_fixture.db.session,
            Loan,
            license_pool=other_pool,
            integration_client=shared_fixture.client,
        )

        with self.request_context_with_client(shared_fixture, "/"):
            response = shared_fixture.manager.shared_collection_controller.fulfill(
                shared_fixture.collection.name, other_pool_loan.id, None
            )
            assert LOAN_NOT_FOUND.uri == response.uri

            api = (
                shared_fixture.app.manager.shared_collection_controller.shared_collection
            )

            # If the loan doesn't have a mechanism set, we need to specify one.
            response = shared_fixture.manager.shared_collection_controller.fulfill(
                shared_fixture.collection.name, loan.id, None
            )
            assert BAD_DELIVERY_MECHANISM.uri == response.uri

            loan.fulfillment = shared_fixture.delivery_mechanism

            api.queue_fulfill(AuthorizationFailedException())
            response = shared_fixture.manager.shared_collection_controller.fulfill(
                shared_fixture.collection.name, loan.id, None
            )
            assert INVALID_CREDENTIALS.uri == response.uri

            api.queue_fulfill(CannotFulfill())
            response = shared_fixture.manager.shared_collection_controller.fulfill(
                shared_fixture.collection.name, loan.id, None
            )
            assert CANNOT_FULFILL.uri == response.uri

            api.queue_fulfill(RemoteIntegrationException("error!", "service"))
            response = shared_fixture.manager.shared_collection_controller.fulfill(
                shared_fixture.collection.name,
                loan.id,
                shared_fixture.delivery_mechanism.delivery_mechanism.id,
            )
            assert INTEGRATION_ERROR.uri == response.uri

            fulfillment_info = FulfillmentInfo(
                shared_fixture.collection,
                shared_fixture.pool.data_source.name,
                shared_fixture.pool.identifier.type,
                shared_fixture.pool.identifier.identifier,
                "http://content",
                "text/html",
                None,
                utc_now(),
            )

            api.queue_fulfill(fulfillment_info)

            def do_get_error(url):
                raise RemoteIntegrationException("error!", "service")

            response = shared_fixture.manager.shared_collection_controller.fulfill(
                shared_fixture.collection.name,
                loan.id,
                shared_fixture.delivery_mechanism.delivery_mechanism.id,
                do_get=do_get_error,
            )
            assert INTEGRATION_ERROR.uri == response.uri

            api.queue_fulfill(fulfillment_info)

            def do_get_success(url):
                return MockRequestsResponse(200, content="Content")

            response = shared_fixture.manager.shared_collection_controller.fulfill(
                shared_fixture.collection.name,
                loan.id,
                shared_fixture.delivery_mechanism.delivery_mechanism.id,
                do_get=do_get_success,
            )
            assert 200 == response.status_code
            assert "Content" == response.get_data(as_text=True)
            assert "text/html" == response.headers.get("Content-Type")

            fulfillment_info.content_link = None
            fulfillment_info.content = "Content"
            api.queue_fulfill(fulfillment_info)
            response = shared_fixture.manager.shared_collection_controller.fulfill(
                shared_fixture.collection.name,
                loan.id,
                shared_fixture.delivery_mechanism.delivery_mechanism.id,
            )
            assert 200 == response.status_code
            assert "Content" == response.get_data(as_text=True)
            assert "text/html" == response.headers.get("Content-Type")

    def test_hold_info(self, shared_fixture: SharedCollectionFixture):
        now = utc_now()
        tomorrow = utc_now() + datetime.timedelta(days=1)

        other_client, ignore = IntegrationClient.register(
            shared_fixture.db.session, "http://otherlibrary"
        )
        other_client_hold, ignore = create(
            shared_fixture.db.session,
            Hold,
            license_pool=shared_fixture.pool,
            integration_client=other_client,
        )

        ignore, other_pool = shared_fixture.db.edition(
            with_license_pool=True,
            collection=shared_fixture.db.collection(),
        )
        other_pool_hold, ignore = create(
            shared_fixture.db.session,
            Hold,
            license_pool=other_pool,
            integration_client=shared_fixture.client,
        )

        hold, ignore = create(
            shared_fixture.db.session,
            Hold,
            license_pool=shared_fixture.pool,
            integration_client=shared_fixture.client,
            start=now,
            end=tomorrow,
        )
        with self.request_context_with_client(shared_fixture, "/"):
            # This hold doesn't exist.
            response = shared_fixture.manager.shared_collection_controller.hold_info(
                shared_fixture.collection.name, 1234567
            )
            assert HOLD_NOT_FOUND == response

            # This hold belongs to a different library.
            response = shared_fixture.manager.shared_collection_controller.hold_info(
                shared_fixture.collection.name, other_client_hold.id
            )
            assert HOLD_NOT_FOUND == response

            # This hold's pool belongs to a different collection.
            response = shared_fixture.manager.shared_collection_controller.hold_info(
                shared_fixture.collection.name, other_pool_hold.id
            )
            assert HOLD_NOT_FOUND == response

            # This hold is ours.
            response = shared_fixture.manager.shared_collection_controller.hold_info(
                shared_fixture.collection.name, hold.id
            )
            assert 200 == response.status_code
            feed = feedparser.parse(response.data)
            [entry] = feed.get("entries")
            availability = entry.get("opds_availability")
            since = availability.get("since")
            until = availability.get("until")
            assert datetime.datetime.strftime(now, "%Y-%m-%dT%H:%M:%S+00:00") == since
            assert (
                datetime.datetime.strftime(tomorrow, "%Y-%m-%dT%H:%M:%S+00:00") == until
            )
            [revoke_url] = [
                link.get("href")
                for link in entry.get("links")
                if link.get("rel") == "http://librarysimplified.org/terms/rel/revoke"
            ]
            assert (
                f"/collections/{shared_fixture.collection.name}/holds/{hold.id}/revoke"
                in revoke_url
            )
            assert [] == [
                link.get("href")
                for link in entry.get("links")
                if link.get("rel") == "http://opds-spec.org/acquisition"
            ]
            [self_url] = [
                link.get("href")
                for link in entry.get("links")
                if link.get("rel") == "self"
            ]
            assert f"/collections/{shared_fixture.collection.name}/holds/{hold.id}"

    def test_revoke_hold(self, shared_fixture: SharedCollectionFixture):
        now = utc_now()
        tomorrow = utc_now() + datetime.timedelta(days=1)
        hold, ignore = create(
            shared_fixture.db.session,
            Hold,
            license_pool=shared_fixture.pool,
            integration_client=shared_fixture.client,
            start=now,
            end=tomorrow,
        )

        other_client, ignore = IntegrationClient.register(
            shared_fixture.db.session, "http://otherlibrary"
        )
        other_client_hold, ignore = create(
            shared_fixture.db.session,
            Hold,
            license_pool=shared_fixture.pool,
            integration_client=other_client,
        )

        ignore, other_pool = shared_fixture.db.edition(
            with_license_pool=True,
            collection=shared_fixture.db.collection(),
        )
        other_pool_hold, ignore = create(
            shared_fixture.db.session,
            Hold,
            license_pool=other_pool,
            integration_client=shared_fixture.client,
        )

        with self.request_context_with_client(shared_fixture, "/"):
            response = shared_fixture.manager.shared_collection_controller.revoke_hold(
                shared_fixture.collection.name, other_pool_hold.id
            )
            assert HOLD_NOT_FOUND.uri == response.uri

            response = shared_fixture.manager.shared_collection_controller.revoke_hold(
                shared_fixture.collection.name, other_client_hold.id
            )
            assert HOLD_NOT_FOUND.uri == response.uri

            api = (
                shared_fixture.app.manager.shared_collection_controller.shared_collection
            )

            api.queue_revoke_hold(AuthorizationFailedException())
            response = shared_fixture.manager.shared_collection_controller.revoke_hold(
                shared_fixture.collection.name, hold.id
            )
            assert INVALID_CREDENTIALS.uri == response.uri

            api.queue_revoke_hold(CannotReleaseHold())
            response = shared_fixture.manager.shared_collection_controller.revoke_hold(
                shared_fixture.collection.name, hold.id
            )
            assert CANNOT_RELEASE_HOLD.uri == response.uri

            api.queue_revoke_hold(NotOnHold())
            response = shared_fixture.manager.shared_collection_controller.revoke_hold(
                shared_fixture.collection.name, hold.id
            )
            assert NO_ACTIVE_HOLD.uri == response.uri
