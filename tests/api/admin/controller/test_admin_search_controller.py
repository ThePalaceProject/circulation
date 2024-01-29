from unittest.mock import MagicMock

import pytest

from api.admin.controller.admin_search import AdminSearchController
from core.model.classification import Subject
from core.model.datasource import DataSource
from core.model.licensing import LicensePool
from core.model.work import Work
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.flask import FlaskAppFixture


class AdminSearchFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        mock_manager = MagicMock()
        mock_manager._db = db.session
        self.controller = AdminSearchController(mock_manager)

        # Setup works with subjects, languages, audiences etc...
        gutenberg = DataSource.lookup(db.session, DataSource.GUTENBERG)
        w: Work = db.work(
            title="work1",
            genre="Education",
            language="eng",
            audience="Adult",
            with_license_pool=True,
        )
        w.presentation_edition.publisher = "Publisher 1"
        s: Subject = db.subject("subject1", "subjectid1")
        s.genre = w.genres[0]
        s.name = "subject 1"
        s.audience = "Adult"
        db.classification(w.presentation_edition.primary_identifier, s, gutenberg)

        w = db.work(
            title="work2",
            genre="Education",
            language="eng",
            audience="Adult",
            with_license_pool=True,
        )
        w.presentation_edition.publisher = "Publisher 1"
        s2: Subject = db.subject("subject2", "subjectid2")
        s2.genre = w.genres[0]
        s2.name = "subject 2"
        s2.audience = "Adult"
        db.classification(w.presentation_edition.primary_identifier, s2, gutenberg)

        w = db.work(
            title="work3",
            genre="Horror",
            language="spa",
            audience="Adult",
            with_license_pool=True,
        )
        w.presentation_edition.publisher = "Publisher 1"
        s3: Subject = db.subject("subject3", "subjectid3")
        s3.genre = w.genres[0]
        s3.name = "subject 3"
        s3.audience = "Adult"
        db.classification(w.presentation_edition.primary_identifier, s3, gutenberg)

        for _ in range(10):
            w = db.work(
                genre="Drama",
                language="man",
                audience="Young Adult",
                data_source_name=DataSource.GUTENBERG,
                with_license_pool=True,
            )
            w.presentation_edition.publisher = "Publisher 10"
            s10: Subject = db.subject("subject10", "subjectid10")
            s10.genre = w.genres[0]
            s10.name = "subject 10"
            s10.audience = "Young Adult"
            db.classification(w.presentation_edition.primary_identifier, s10, gutenberg)


@pytest.fixture(scope="function")
def admin_search_fixture(
    db: DatabaseTransactionFixture,
) -> AdminSearchFixture:
    return AdminSearchFixture(db)


class TestAdminSearchController:
    def test_search_field_values(
        self,
        admin_search_fixture: AdminSearchFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        with flask_app_fixture.test_request_context(
            "/",
            library=db.default_library(),
        ):
            response = admin_search_fixture.controller.search_field_values()

        assert response["subjects"] == {
            "subject 1": 1,
            "subject 2": 1,
            "subject 3": 1,
            "subject 10": 10,
        }
        assert response["audiences"] == {"Adult": 3, "Young Adult": 10}
        assert response["genres"] == {"Education": 2, "Horror": 1, "Drama": 10}
        assert response["languages"] == {"English": 2, "Spanish": 1, "Mandingo": 10}
        assert response["publishers"] == {"Publisher 1": 3, "Publisher 10": 10}
        assert response["distributors"] == {"Gutenberg": 13}

    def test_different_license_types(
        self,
        admin_search_fixture: AdminSearchFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        # Remove the cache
        admin_search_fixture.controller.__class__._search_field_values_cached.ttls = (  # type: ignore
            0
        )

        w = (
            db.session.query(Work)
            .filter(Work.presentation_edition.has(title="work3"))
            .first()
        )
        assert isinstance(w, Work)

        pool = w.active_license_pool()
        assert isinstance(pool, LicensePool)

        # A pool without licenses should not attribute to the count
        pool.licenses_owned = 0
        with flask_app_fixture.test_request_context(
            "/",
            library=db.default_library(),
        ):
            response = admin_search_fixture.controller.search_field_values()
            assert "Horror" not in response["genres"]
            assert "Spanish" not in response["languages"]

        # An open access license should get counted even without owned licenses
        pool.open_access = True
        with flask_app_fixture.test_request_context(
            "/",
            library=db.default_library(),
        ):
            response = admin_search_fixture.controller.search_field_values()
            assert "Horror" in response["genres"]
            assert "Spanish" in response["languages"]
