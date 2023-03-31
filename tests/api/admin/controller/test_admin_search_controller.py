import pytest

from core.model.classification import Subject
from core.model.datasource import DataSource
from core.model.work import Work
from tests.fixtures.api_admin import AdminControllerFixture


class AdminSearchFixture:
    def __init__(self, admin_ctrl_fixture: AdminControllerFixture):
        self.admin_ctrl_fixture = admin_ctrl_fixture
        self.manager = admin_ctrl_fixture.manager

        db = self.admin_ctrl_fixture.ctrl.db

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
        s: Subject = db.subject("subject2", "subjectid2")
        s.genre = w.genres[0]
        s.name = "subject 2"
        s.audience = "Adult"
        db.classification(w.presentation_edition.primary_identifier, s, gutenberg)

        w = db.work(
            title="work3",
            genre="Horror",
            language="spa",
            audience="Adult",
            with_license_pool=True,
        )
        w.presentation_edition.publisher = "Publisher 1"
        s: Subject = db.subject("subject3", "subjectid3")
        s.genre = w.genres[0]
        s.name = "subject 3"
        s.audience = "Adult"
        db.classification(w.presentation_edition.primary_identifier, s, gutenberg)

        for _ in range(10):
            w = db.work(
                genre="Drama",
                language="man",
                audience="Young Adult",
                data_source_name=DataSource.GUTENBERG,
                with_license_pool=True,
            )
            w.presentation_edition.publisher = "Publisher 10"
            s: Subject = db.subject("subject10", "subjectid10")
            s.genre = w.genres[0]
            s.name = "subject 10"
            s.audience = "Young Adult"
            db.classification(w.presentation_edition.primary_identifier, s, gutenberg)


@pytest.fixture(scope="function")
def admin_search_fixture(
    admin_ctrl_fixture: AdminControllerFixture,
) -> AdminSearchFixture:
    return AdminSearchFixture(admin_ctrl_fixture)


class TestAdminSearchController:
    def test_search_field_values(self, admin_search_fixture: AdminSearchFixture):
        with admin_search_fixture.admin_ctrl_fixture.request_context_with_library_and_admin(
            "/",
            library=admin_search_fixture.admin_ctrl_fixture.ctrl.db.default_library(),
        ):
            response = (
                admin_search_fixture.manager.admin_search_controller.search_field_values()
            )

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
