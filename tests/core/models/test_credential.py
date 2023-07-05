import datetime

import pytest
from sqlalchemy.exc import IntegrityError

from core.model import Collection, Patron
from core.model.credential import Credential
from core.model.datasource import DataSource
from core.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture


class TestCredentials:
    def test_temporary_token(self, db: DatabaseTransactionFixture):
        # Create a temporary token good for one hour.
        duration = datetime.timedelta(hours=1)
        data_source = DataSource.lookup(db.session, DataSource.ADOBE)
        patron = db.patron()
        now = utc_now()
        expect_expires = now + duration
        token, is_new = Credential.temporary_token_create(
            db.session, data_source, "some random type", patron, duration
        )
        assert data_source == token.data_source
        assert "some random type" == token.type
        assert patron == token.patron
        expires_difference = abs((token.expires - expect_expires).seconds)
        assert expires_difference < 2

        # Now try to look up the credential based solely on the UUID.
        new_token = Credential.lookup_by_token(
            db.session, data_source, token.type, token.credential
        )
        assert new_token == token

        # When we call lookup_and_expire_temporary_token, the token is automatically
        # expired and we cannot use it anymore.
        new_token = Credential.lookup_and_expire_temporary_token(
            db.session, data_source, token.type, token.credential
        )
        assert new_token == token
        assert new_token.expires < now

        new_token = Credential.lookup_by_token(
            db.session, data_source, token.type, token.credential
        )
        assert None == new_token

        new_token = Credential.lookup_and_expire_temporary_token(
            db.session, data_source, token.type, token.credential
        )
        assert None == new_token

        # A token with no expiration date is treated as expired...
        token.expires = None
        db.session.commit()
        no_expiration_token = Credential.lookup_by_token(
            db.session, data_source, token.type, token.credential
        )
        assert None == no_expiration_token

        # ...unless we specifically say we're looking for a persistent token.
        no_expiration_token = Credential.lookup_by_token(
            db.session,
            data_source,
            token.type,
            token.credential,
            allow_persistent_token=True,
        )
        assert token == no_expiration_token

    def test_specify_value_of_temporary_token(self, db: DatabaseTransactionFixture):
        """By default, a temporary token has a randomly generated value, but
        you can give a specific value to represent a temporary token you got
        from somewhere else.
        """
        patron = db.patron()
        duration = datetime.timedelta(hours=1)
        data_source = DataSource.lookup(db.session, DataSource.ADOBE)
        token, is_new = Credential.temporary_token_create(
            db.session,
            data_source,
            "some random type",
            patron,
            duration,
            "Some random value",
        )
        assert "Some random value" == token.credential

    def test_temporary_token_overwrites_old_token(self, db: DatabaseTransactionFixture):
        duration = datetime.timedelta(hours=1)
        data_source = DataSource.lookup(db.session, DataSource.ADOBE)
        patron = db.patron()
        old_token, is_new = Credential.temporary_token_create(
            db.session, data_source, "some random type", patron, duration
        )
        assert True == is_new
        old_credential = old_token.credential

        # Creating a second temporary token overwrites the first.
        token, is_new = Credential.temporary_token_create(
            db.session, data_source, "some random type", patron, duration
        )
        assert False == is_new
        assert token.id == old_token.id
        assert old_credential != token.credential

    def test_persistent_token(self, db: DatabaseTransactionFixture):
        # Create a persistent token.
        data_source = DataSource.lookup(db.session, DataSource.ADOBE)
        patron = db.patron()
        token, is_new = Credential.persistent_token_create(
            db.session, data_source, "some random type", patron
        )
        assert data_source == token.data_source
        assert "some random type" == token.type
        assert patron == token.patron

        # Now try to look up the credential based solely on the UUID.
        new_token = Credential.lookup_by_token(
            db.session,
            data_source,
            token.type,
            token.credential,
            allow_persistent_token=True,
        )
        assert new_token == token
        credential = new_token.credential

        # We can keep calling lookup_by_token and getting the same
        # Credential object with the same .credential -- it doesn't
        # expire.
        again_token = Credential.lookup_by_token(
            db.session,
            data_source,
            token.type,
            token.credential,
            allow_persistent_token=True,
        )
        assert again_token == new_token
        assert again_token.credential == credential

    def test_cannot_look_up_nonexistent_token(self, db: DatabaseTransactionFixture):
        data_source = DataSource.lookup(db.session, DataSource.ADOBE)
        new_token = Credential.lookup_by_token(
            db.session, data_source, "no such type", "no such credential"
        )
        assert None == new_token

    def test_empty_token(self, db: DatabaseTransactionFixture):
        # Test the behavior when a credential is empty.
        # First, create a token with an empty credential.
        data_source = DataSource.lookup(db.session, DataSource.ADOBE)
        token, is_new = Credential.persistent_token_create(
            db.session, data_source, "i am empty", None
        )
        token.credential = None

        # If allow_empty_token is true, the token is returned as-is
        # and the refresher method is not called.
        def refresher(self):
            raise Exception("Refresher method was called")

        args = (
            db.session,
            data_source,
            token.type,
            None,
            refresher,
        )
        again_token = Credential.lookup(
            *args, allow_persistent_token=True, allow_empty_token=True
        )
        assert again_token == token

        # If allow_empty_token is False, the refresher method is
        # created.
        with pytest.raises(Exception) as excinfo:
            Credential.lookup(
                *args, allow_persistent_token=True, allow_empty_token=False
            )
        assert "Refresher method was called" in str(excinfo.value)

    def test_force_refresher_method(self, db: DatabaseTransactionFixture):
        # Ensure that passing `force_refresh=True` triggers the
        # refresher method, even when none of the usual conditions
        # are satisfied.

        def refresher(self):
            raise Exception("Refresher method was called")

        # Create a persistent token and ensure that it's present
        data_source = DataSource.lookup(db.session, DataSource.ADOBE)
        patron = db.patron()
        token, is_new = Credential.persistent_token_create(
            db.session, data_source, "some random type", patron
        )
        assert data_source == token.data_source
        assert "some random type" == token.type
        assert patron == token.patron

        # We'll vary the `force_refresh` setting, but otherwise
        # use the same parameters for the next to calls to `lookup`.
        args = (db.session), data_source, token.type, patron, refresher

        # This call should should not run the refresher method.
        again_token = Credential.lookup(
            *args, allow_persistent_token=True, force_refresh=False
        )
        assert again_token == token

        # This call should run the refresher method.
        with pytest.raises(Exception) as excinfo:
            Credential.lookup(*args, allow_persistent_token=True, force_refresh=True)
        assert "Refresher method was called" in str(excinfo.value)

    def test_collection_token(self, db: DatabaseTransactionFixture):
        # Make sure we can have two tokens from the same data_source with
        # different collections.
        data_source = DataSource.lookup(db.session, DataSource.FEEDBOOKS)
        collection1 = db.collection("test collection 1")
        collection2 = db.collection("test collection 2")
        patron = db.patron()
        type = "super secret"

        # Create our credentials
        credential1 = Credential.lookup(
            db.session, data_source, type, patron, None, collection=collection1
        )
        credential2 = Credential.lookup(
            db.session, data_source, type, patron, None, collection=collection2
        )
        credential1.credential = "test1"
        credential2.credential = "test2"

        # Make sure the text matches what we expect
        assert (
            "test1"
            == Credential.lookup(
                db.session, data_source, type, patron, None, collection=collection1
            ).credential
        )
        assert (
            "test2"
            == Credential.lookup(
                db.session, data_source, type, patron, None, collection=collection2
            ).credential
        )

        # Make sure we don't get anything if we don't pass a collection
        assert (
            None
            == Credential.lookup(db.session, data_source, type, patron, None).credential
        )


class TestUniquenessConstraintsFixture:
    data_source: DataSource
    type: str
    patron: Patron
    col1: Collection
    col2: Collection
    transaction: DatabaseTransactionFixture


@pytest.fixture()
def test_uniqueness_fixture(
    db: DatabaseTransactionFixture,
) -> TestUniquenessConstraintsFixture:
    fix = TestUniquenessConstraintsFixture()
    fix.transaction = db
    fix.type = "a credential type"
    fix.data_source = DataSource.lookup(db.session, DataSource.OVERDRIVE)
    fix.patron = db.patron()
    fix.col1 = db.default_collection()
    fix.col2 = db.collection()
    return fix


class TestUniquenessConstraints:
    def test_duplicate_sitewide_credential(
        self, test_uniqueness_fixture: TestUniquenessConstraintsFixture
    ):
        data = test_uniqueness_fixture
        session = data.transaction.session

        # You can't create two credentials with the same data source,
        # type, and token value.
        token = "a token"

        c1 = Credential(data_source=data.data_source, type=data.type, credential=token)
        session.flush()
        c2 = Credential(data_source=data.data_source, type=data.type, credential=token)
        pytest.raises(IntegrityError, session.flush)

    def test_duplicate_patron_credential(
        self, test_uniqueness_fixture: TestUniquenessConstraintsFixture
    ):
        data = test_uniqueness_fixture
        session = data.transaction.session

        # A given patron can't have two global credentials with the same data
        # source and type.
        patron = data.transaction.patron()

        c1 = Credential(
            data_source=data.data_source, type=data.type, patron=data.patron
        )
        session.flush()
        c2 = Credential(
            data_source=data.data_source, type=data.type, patron=data.patron
        )
        pytest.raises(IntegrityError, session.flush)

    def test_duplicate_patron_collection_credential(
        self, test_uniqueness_fixture: TestUniquenessConstraintsFixture
    ):
        data = test_uniqueness_fixture
        session = data.transaction.session

        # A given patron can have two collection-scoped credentials
        # with the same data source and type, but only if the two
        # collections are different.
        c1 = Credential(
            data_source=data.data_source,
            type=data.type,
            patron=data.patron,
            collection=data.col1,
        )
        c2 = Credential(
            data_source=data.data_source,
            type=data.type,
            patron=data.patron,
            collection=data.col2,
        )
        session.flush()
        c3 = Credential(
            data_source=data.data_source,
            type=data.type,
            patron=data.patron,
            collection=data.col1,
        )
        pytest.raises(IntegrityError, session.flush)

    def test_duplicate_collection_credential(
        self, test_uniqueness_fixture: TestUniquenessConstraintsFixture
    ):
        data = test_uniqueness_fixture
        session = data.transaction.session

        # A given collection can't have two global credentials with
        # the same data source and type.
        c1 = Credential(
            data_source=data.data_source, type=data.type, collection=data.col1
        )
        session.flush()
        c2 = Credential(
            data_source=data.data_source, type=data.type, collection=data.col1
        )
        pytest.raises(IntegrityError, session.flush)
