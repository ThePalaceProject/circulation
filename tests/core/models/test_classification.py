import pytest
from psycopg2.extras import NumericRange
from sqlalchemy.exc import IntegrityError

from core.classifier import Classifier
from core.model import create
from core.model.classification import Genre, Subject
from tests.fixtures.database import DatabaseTransactionFixture


class TestSubject:
    def test_lookup_errors(self, database_transaction: DatabaseTransactionFixture):
        """Subject.lookup will complain if you don't give it
        enough information to find a Subject.
        """
        session = database_transaction.session()
        with pytest.raises(ValueError) as excinfo:
            Subject.lookup(session, None, "identifier", "name")
        assert "Cannot look up Subject with no type." in str(excinfo.value)
        with pytest.raises(ValueError) as excinfo:
            Subject.lookup(session, Subject.TAG, None, None)
        assert (
            "Cannot look up Subject when neither identifier nor name is provided."
            in str(excinfo.value)
        )

    def test_lookup_autocreate(self, database_transaction: DatabaseTransactionFixture):
        # By default, Subject.lookup creates a Subject that doesn't exist.
        identifier = database_transaction.fresh_str()
        name = database_transaction.fresh_str()
        session = database_transaction.session()
        subject, was_new = Subject.lookup(session, Subject.TAG, identifier, name)
        assert True == was_new
        assert identifier == subject.identifier
        assert name == subject.name

        # But you can tell it not to autocreate.
        identifier2 = database_transaction.fresh_str()
        subject, was_new = Subject.lookup(
            session, Subject.TAG, identifier2, None, autocreate=False
        )
        assert False == was_new
        assert None == subject

    def test_lookup_by_name(self, database_transaction: DatabaseTransactionFixture):
        """We can look up a subject by its name, without providing an
        identifier."""
        s1 = database_transaction.subject(Subject.TAG, "i1")
        s1.name = "A tag"
        session = database_transaction.session()
        assert (s1, False) == Subject.lookup(session, Subject.TAG, None, "A tag")

        # If we somehow get into a state where there are two Subjects
        # with the same name, Subject.lookup treats them as interchangeable.
        s2 = database_transaction.subject(Subject.TAG, "i2")
        s2.name = "A tag"

        subject, is_new = Subject.lookup(session, Subject.TAG, None, "A tag")
        assert subject in [s1, s2]
        assert False == is_new

    def test_assign_to_genre_can_remove_genre(
        self, database_transaction: DatabaseTransactionFixture
    ):
        # Here's a Subject that identifies children's books.
        session = database_transaction.session()
        subject, was_new = Subject.lookup(
            session, Subject.TAG, "Children's books", None
        )

        # The genre and audience data for this Subject is totally wrong.
        subject.audience = Classifier.AUDIENCE_ADULT
        subject.target_age = NumericRange(1, 10)
        subject.fiction = False
        sf, ignore = Genre.lookup(session, "Science Fiction")
        subject.genre = sf

        # But calling assign_to_genre() will fix it.
        subject.assign_to_genre()
        assert Classifier.AUDIENCE_CHILDREN == subject.audience
        assert NumericRange(None, None, "[]") == subject.target_age
        assert None == subject.genre
        assert None == subject.fiction


class TestGenre:
    def test_name_is_unique(self, database_transaction: DatabaseTransactionFixture):
        session = database_transaction.session()
        g1, ignore = Genre.lookup(session, "A Genre", autocreate=True)
        g2, ignore = Genre.lookup(session, "A Genre", autocreate=True)
        assert g1 == g2

        pytest.raises(IntegrityError, create, session, Genre, name="A Genre")

    def test_default_fiction(self, database_transaction: DatabaseTransactionFixture):
        session = database_transaction.session()
        sf, ignore = Genre.lookup(session, "Science Fiction")
        nonfiction, ignore = Genre.lookup(session, "History")
        assert True == sf.default_fiction
        assert False == nonfiction.default_fiction

        # Create a previously unknown genre.
        genre, ignore = Genre.lookup(session, "Some Weird Genre", autocreate=True)

        # We don't know its default fiction status.
        assert None == genre.default_fiction
