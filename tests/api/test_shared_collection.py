import base64
import json

import pytest
from Crypto.Cipher import PKCS1_OAEP
from Crypto.PublicKey import RSA

from api.circulation import FulfillmentInfo
from api.circulation_exceptions import *
from api.odl import ODLAPI
from api.shared_collection import BaseSharedCollectionAPI, SharedCollectionAPI
from core.config import CannotLoadConfiguration
from core.model import Hold, IntegrationClient, Loan, create, get_one
from tests.core.mock import MockRequestsResponse
from tests.fixtures.database import DatabaseTransactionFixture


class MockAPI(BaseSharedCollectionAPI):
    def __init__(self, _db, collection):
        self.checkouts = []
        self.returns = []
        self.fulfills = []
        self.holds = []
        self.released_holds = []
        self.fulfillment = None

    def checkout_to_external_library(self, client, pool, hold=None):
        self.checkouts.append((client, pool))

    def checkin_from_external_library(self, client, loan):
        self.returns.append((client, loan))

    def fulfill_for_external_library(self, client, loan, mechanism):
        self.fulfills.append((client, loan, mechanism))
        return self.fulfillment

    def release_hold_from_external_library(self, client, hold):
        self.released_holds.append((client, hold))


class SharedCollectionFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.collection = db.collection(protocol="Mock")
        self.collection.integration_configuration.settings = dict(
            username="username", password="password", data_source="data_source"
        )
        self.shared_collection = SharedCollectionAPI(
            db.session, api_map={"Mock": MockAPI}
        )
        self.api = self.shared_collection.api(self.collection)
        self.collection.integration_configuration.set(
            BaseSharedCollectionAPI.EXTERNAL_LIBRARY_URLS, ["http://library.org"]
        )
        self.client, ignore = IntegrationClient.register(
            db.session, "http://library.org"
        )
        edition, self.pool = db.edition(
            with_license_pool=True, collection=self.collection
        )
        [self.delivery_mechanism] = self.pool.delivery_mechanisms


@pytest.fixture(scope="function")
def shared_collection_fixture(
    db: DatabaseTransactionFixture,
) -> SharedCollectionFixture:
    return SharedCollectionFixture(db)


class TestSharedCollectionAPI:
    def test_initialization_exception(
        self, shared_collection_fixture: SharedCollectionFixture
    ):
        db = shared_collection_fixture.db

        class MisconfiguredAPI:
            def __init__(self, _db, collection):
                raise CannotLoadConfiguration("doomed!")

        api_map = {db.default_collection().protocol: MisconfiguredAPI}
        shared_collection = SharedCollectionAPI(db.session, api_map=api_map)
        # Although the SharedCollectionAPI was created, it has no functioning
        # APIs.
        assert {} == shared_collection.api_for_collection

        # Instead, the CannotLoadConfiguration exception raised by the
        # constructor has been stored in initialization_exceptions.
        e = shared_collection.initialization_exceptions[db.default_collection().id]
        assert isinstance(e, CannotLoadConfiguration)
        assert "doomed!" == str(e)

    def test_api_for_licensepool(
        self, shared_collection_fixture: SharedCollectionFixture
    ):
        db = shared_collection_fixture.db

        collection = db.collection(protocol=ODLAPI.NAME)
        collection.integration_configuration.settings = dict(
            username="username", password="password", data_source="data_source"
        )
        edition, pool = db.edition(with_license_pool=True, collection=collection)
        shared_collection = SharedCollectionAPI(db.session)
        assert isinstance(shared_collection.api_for_licensepool(pool), ODLAPI)

    def test_api_for_collection(
        self, shared_collection_fixture: SharedCollectionFixture
    ):
        db = shared_collection_fixture.db

        collection = db.collection()
        collection.integration_configuration.settings = dict(
            username="username", password="password", data_source="data_source"
        )
        shared_collection = SharedCollectionAPI(db.session)
        # The collection isn't a shared collection, so looking up its API
        # raises an exception.
        pytest.raises(CirculationException, shared_collection.api, collection)

        collection.protocol = ODLAPI.NAME
        shared_collection = SharedCollectionAPI(db.session)
        assert isinstance(shared_collection.api(collection), ODLAPI)

    def test_register(self, shared_collection_fixture: SharedCollectionFixture):
        db = shared_collection_fixture.db

        # An auth document URL is required to register.
        pytest.raises(
            InvalidInputException,
            shared_collection_fixture.shared_collection.register,
            shared_collection_fixture.collection,
            None,
        )

        # If the url doesn't return a valid auth document, there's an exception.
        auth_response = "not json"

        def do_get(*args, **kwargs):
            return MockRequestsResponse(200, content=auth_response)

        pytest.raises(
            RemoteInitiatedServerError,
            shared_collection_fixture.shared_collection.register,
            shared_collection_fixture.collection,
            "http://library.org/auth",
            do_get=do_get,
        )

        # The auth document also must have a link to the library's catalog.
        auth_response = json.dumps({"links": []})
        pytest.raises(
            RemoteInitiatedServerError,
            shared_collection_fixture.shared_collection.register,
            shared_collection_fixture.collection,
            "http://library.org/auth",
            do_get=do_get,
        )

        # If no external library URLs are configured, no one can register.
        auth_response = json.dumps(
            {"links": [{"href": "http://library.org", "rel": "start"}]}
        )
        shared_collection_fixture.collection.integration_configuration.set(
            BaseSharedCollectionAPI.EXTERNAL_LIBRARY_URLS, None
        )
        pytest.raises(
            AuthorizationFailedException,
            shared_collection_fixture.shared_collection.register,
            shared_collection_fixture.collection,
            "http://library.org/auth",
            do_get=do_get,
        )

        # If the library's URL isn't in the configuration, it can't register.
        auth_response = json.dumps(
            {"links": [{"href": "http://differentlibrary.org", "rel": "start"}]}
        )
        shared_collection_fixture.collection.integration_configuration.set(
            BaseSharedCollectionAPI.EXTERNAL_LIBRARY_URLS, ["http://library.org"]
        )
        pytest.raises(
            AuthorizationFailedException,
            shared_collection_fixture.shared_collection.register,
            shared_collection_fixture.collection,
            "http://differentlibrary.org/auth",
            do_get=do_get,
        )

        # Or if the public key is missing from the auth document.
        auth_response = json.dumps(
            {"links": [{"href": "http://library.org", "rel": "start"}]}
        )
        pytest.raises(
            RemoteInitiatedServerError,
            shared_collection_fixture.shared_collection.register,
            shared_collection_fixture.collection,
            "http://library.org/auth",
            do_get=do_get,
        )

        auth_response = json.dumps(
            {
                "public_key": {"type": "not RSA", "value": "123"},
                "links": [{"href": "http://library.org", "rel": "start"}],
            }
        )
        pytest.raises(
            RemoteInitiatedServerError,
            shared_collection_fixture.shared_collection.register,
            shared_collection_fixture.collection,
            "http://library.org/auth",
            do_get=do_get,
        )

        auth_response = json.dumps(
            {
                "public_key": {"type": "RSA"},
                "links": [{"href": "http://library.org", "rel": "start"}],
            }
        )
        pytest.raises(
            RemoteInitiatedServerError,
            shared_collection_fixture.shared_collection.register,
            shared_collection_fixture.collection,
            "http://library.org/auth",
            do_get=do_get,
        )

        # Here's an auth document with a valid key.
        key = RSA.generate(2048)
        public_key = key.publickey().exportKey().decode("utf-8")
        encryptor = PKCS1_OAEP.new(key)
        auth_response = json.dumps(
            {
                "public_key": {"type": "RSA", "value": public_key},
                "links": [{"href": "http://library.org", "rel": "start"}],
            }
        )
        response = shared_collection_fixture.shared_collection.register(
            shared_collection_fixture.collection,
            "http://library.org/auth",
            do_get=do_get,
        )

        # An IntegrationClient has been created.
        client = get_one(
            db.session,
            IntegrationClient,
            url=IntegrationClient.normalize_url("http://library.org/"),
        )
        decrypted_secret = encryptor.decrypt(
            base64.b64decode(response.get("metadata", {}).get("shared_secret"))
        )
        assert client is not None
        assert client.shared_secret == decrypted_secret.decode("utf-8")

    def test_borrow(self, shared_collection_fixture: SharedCollectionFixture):
        db = shared_collection_fixture.db

        # This client is registered, but isn't one of the allowed URLs for the collection
        # (maybe it was registered for a different shared collection).
        other_client, ignore = IntegrationClient.register(
            db.session, "http://other_library.org"
        )

        # Trying to borrow raises an exception.
        pytest.raises(
            AuthorizationFailedException,
            shared_collection_fixture.shared_collection.borrow,
            shared_collection_fixture.collection,
            other_client,
            shared_collection_fixture.pool,
        )

        # A client that's registered with the collection can borrow.
        shared_collection_fixture.shared_collection.borrow(
            shared_collection_fixture.collection,
            shared_collection_fixture.client,
            shared_collection_fixture.pool,
        )
        assert [
            (shared_collection_fixture.client, shared_collection_fixture.pool)
        ] == shared_collection_fixture.api.checkouts

        # If the client's checking out an existing hold, the hold must be for that client.
        hold, ignore = create(
            db.session,
            Hold,
            integration_client=other_client,
            license_pool=shared_collection_fixture.pool,
        )
        pytest.raises(
            CannotLoan,
            shared_collection_fixture.shared_collection.borrow,
            shared_collection_fixture.collection,
            shared_collection_fixture.client,
            shared_collection_fixture.pool,
            hold=hold,
        )

        hold.integration_client = shared_collection_fixture.client
        shared_collection_fixture.shared_collection.borrow(
            shared_collection_fixture.collection,
            shared_collection_fixture.client,
            shared_collection_fixture.pool,
            hold=hold,
        )
        assert [
            (shared_collection_fixture.client, shared_collection_fixture.pool)
        ] == shared_collection_fixture.api.checkouts[1:]

    def test_revoke_loan(self, shared_collection_fixture: SharedCollectionFixture):
        db = shared_collection_fixture.db

        other_client, ignore = IntegrationClient.register(
            db.session, "http://other_library.org"
        )
        loan, ignore = create(
            db.session,
            Loan,
            integration_client=other_client,
            license_pool=shared_collection_fixture.pool,
        )
        pytest.raises(
            NotCheckedOut,
            shared_collection_fixture.shared_collection.revoke_loan,
            shared_collection_fixture.collection,
            shared_collection_fixture.client,
            loan,
        )

        loan.integration_client = shared_collection_fixture.client
        shared_collection_fixture.shared_collection.revoke_loan(
            shared_collection_fixture.collection, shared_collection_fixture.client, loan
        )
        assert [
            (shared_collection_fixture.client, loan)
        ] == shared_collection_fixture.api.returns

    def test_fulfill(self, shared_collection_fixture: SharedCollectionFixture):
        db = shared_collection_fixture.db

        other_client, ignore = IntegrationClient.register(
            db.session, "http://other_library.org"
        )
        loan, ignore = create(
            db.session,
            Loan,
            integration_client=other_client,
            license_pool=shared_collection_fixture.pool,
        )
        pytest.raises(
            CannotFulfill,
            shared_collection_fixture.shared_collection.fulfill,
            shared_collection_fixture.collection,
            shared_collection_fixture.client,
            loan,
            shared_collection_fixture.delivery_mechanism,
        )

        loan.integration_client = shared_collection_fixture.client

        # If the API does not return content or a content link, the loan can't be fulfilled.
        pytest.raises(
            CannotFulfill,
            shared_collection_fixture.shared_collection.fulfill,
            shared_collection_fixture.collection,
            shared_collection_fixture.client,
            loan,
            shared_collection_fixture.delivery_mechanism,
        )
        assert [
            (
                shared_collection_fixture.client,
                loan,
                shared_collection_fixture.delivery_mechanism,
            )
        ] == shared_collection_fixture.api.fulfills

        shared_collection_fixture.api.fulfillment = FulfillmentInfo(
            shared_collection_fixture.collection,
            shared_collection_fixture.pool.data_source.name,
            shared_collection_fixture.pool.identifier.type,
            shared_collection_fixture.pool.identifier.identifier,
            "http://content",
            "text/html",
            None,
            None,
        )
        fulfillment = shared_collection_fixture.shared_collection.fulfill(
            shared_collection_fixture.collection,
            shared_collection_fixture.client,
            loan,
            shared_collection_fixture.delivery_mechanism,
        )
        assert [
            (
                shared_collection_fixture.client,
                loan,
                shared_collection_fixture.delivery_mechanism,
            )
        ] == shared_collection_fixture.api.fulfills[1:]
        assert shared_collection_fixture.delivery_mechanism == loan.fulfillment

    def test_revoke_hold(self, shared_collection_fixture: SharedCollectionFixture):
        db = shared_collection_fixture.db

        other_client, ignore = IntegrationClient.register(
            db.session, "http://other_library.org"
        )
        hold, ignore = create(
            db.session,
            Hold,
            integration_client=other_client,
            license_pool=shared_collection_fixture.pool,
        )

        pytest.raises(
            CannotReleaseHold,
            shared_collection_fixture.shared_collection.revoke_hold,
            shared_collection_fixture.collection,
            shared_collection_fixture.client,
            hold,
        )

        hold.integration_client = shared_collection_fixture.client
        shared_collection_fixture.shared_collection.revoke_hold(
            shared_collection_fixture.collection, shared_collection_fixture.client, hold
        )
        assert [
            (shared_collection_fixture.client, hold)
        ] == shared_collection_fixture.api.released_holds
