# encoding: utf-8
import pytest
from psycopg2.extras import NumericRange
from sqlalchemy.exc import IntegrityError

from ...classifier import Classifier
from ...model import create, get_one, get_one_or_create
from ...model.classification import Genre, Subject
from ...testing import DatabaseTest


class TestSubject(DatabaseTest):
    def test_lookup_errors(self):
        """Subject.lookup will complain if you don't give it
        enough information to find a Subject.
        """
        with pytest.raises(ValueError) as excinfo:
            Subject.lookup(self._db, None, "identifier", "name")
        assert "Cannot look up Subject with no type." in str(excinfo.value)
        with pytest.raises(ValueError) as excinfo:
            Subject.lookup(self._db, Subject.TAG, None, None)
        assert (
            "Cannot look up Subject when neither identifier nor name is provided."
            in str(excinfo.value)
        )

    def test_lookup_autocreate(self):
        # By default, Subject.lookup creates a Subject that doesn't exist.
        identifier = self._str
        name = self._str
        subject, was_new = Subject.lookup(self._db, Subject.TAG, identifier, name)
        assert True == was_new
        assert identifier == subject.identifier
        assert name == subject.name

        # But you can tell it not to autocreate.
        identifier2 = self._str
        subject, was_new = Subject.lookup(
            self._db, Subject.TAG, identifier2, None, autocreate=False
        )
        assert False == was_new
        assert None == subject

    def test_lookup_by_name(self):
        """We can look up a subject by its name, without providing an
        identifier."""
        s1 = self._subject(Subject.TAG, "i1")
        s1.name = "A tag"
        assert (s1, False) == Subject.lookup(self._db, Subject.TAG, None, "A tag")

        # If we somehow get into a state where there are two Subjects
        # with the same name, Subject.lookup treats them as interchangeable.
        s2 = self._subject(Subject.TAG, "i2")
        s2.name = "A tag"

        subject, is_new = Subject.lookup(self._db, Subject.TAG, None, "A tag")
        assert subject in [s1, s2]
        assert False == is_new

    def test_assign_to_genre_can_remove_genre(self):
        # Here's a Subject that identifies children's books.
        subject, was_new = Subject.lookup(
            self._db, Subject.TAG, "Children's books", None
        )

        # The genre and audience data for this Subject is totally wrong.
        subject.audience = Classifier.AUDIENCE_ADULT
        subject.target_age = NumericRange(1, 10)
        subject.fiction = False
        sf, ignore = Genre.lookup(self._db, "Science Fiction")
        subject.genre = sf

        # But calling assign_to_genre() will fix it.
        subject.assign_to_genre()
        assert Classifier.AUDIENCE_CHILDREN == subject.audience
        assert NumericRange(None, None, "[]") == subject.target_age
        assert None == subject.genre
        assert None == subject.fiction


class TestGenre(DatabaseTest):
    def test_name_is_unique(self):
        g1, ignore = Genre.lookup(self._db, "A Genre", autocreate=True)
        g2, ignore = Genre.lookup(self._db, "A Genre", autocreate=True)
        assert g1 == g2

        pytest.raises(IntegrityError, create, self._db, Genre, name="A Genre")

    def test_default_fiction(self):
        sf, ignore = Genre.lookup(self._db, "Science Fiction")
        nonfiction, ignore = Genre.lookup(self._db, "History")
        assert True == sf.default_fiction
        assert False == nonfiction.default_fiction

        # Create a previously unknown genre.
        genre, ignore = Genre.lookup(self._db, "Some Weird Genre", autocreate=True)

        # We don't know its default fiction status.
        assert None == genre.default_fiction
