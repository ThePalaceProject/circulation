from palace.core.model.classification import Subject
from palace.core.model.datasource import DataSource
from palace.core.model.work import Work
from tests.api.admin.controller.test_controller import AdminControllerTest


class TestAdminSearchController(AdminControllerTest):
    def setup_method(self):
        super().setup_method()

        # Setup works with subjects, languages, audiences etc...
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        w: Work = self._work(
            title="work1",
            genre="Education",
            language="eng",
            audience="Adult",
            with_license_pool=True,
        )
        w.presentation_edition.publisher = "Publisher 1"
        s: Subject = self._subject("subject1", "subjectid1")
        s.genre = w.genres[0]
        s.name = "subject 1"
        s.audience = "Adult"
        self._classification(w.presentation_edition.primary_identifier, s, gutenberg)

        w = self._work(
            title="work2",
            genre="Education",
            language="eng",
            audience="Adult",
            with_license_pool=True,
        )
        w.presentation_edition.publisher = "Publisher 1"
        s: Subject = self._subject("subject2", "subjectid2")
        s.genre = w.genres[0]
        s.name = "subject 2"
        s.audience = "Adult"
        self._classification(w.presentation_edition.primary_identifier, s, gutenberg)

        w = self._work(
            title="work3",
            genre="Horror",
            language="spa",
            audience="Adult",
            with_license_pool=True,
        )
        w.presentation_edition.publisher = "Publisher 1"
        s: Subject = self._subject("subject3", "subjectid3")
        s.genre = w.genres[0]
        s.name = "subject 3"
        s.audience = "Adult"
        self._classification(w.presentation_edition.primary_identifier, s, gutenberg)

        for _ in range(10):
            w = self._work(
                genre="Drama",
                language="man",
                audience="Young Adult",
                data_source_name=DataSource.GUTENBERG,
                with_license_pool=True,
            )
            w.presentation_edition.publisher = "Publisher 10"
            s: Subject = self._subject("subject10", "subjectid10")
            s.genre = w.genres[0]
            s.name = "subject 10"
            s.audience = "Young Adult"
            self._classification(
                w.presentation_edition.primary_identifier, s, gutenberg
            )

    def test_search_field_values(self):
        with self.request_context_with_library_and_admin(
            "/", library=self._default_library
        ):
            response = self.manager.admin_search_controller.search_field_values()

        assert response["subjects"] == {
            "subject 1": 1,
            "subject 2": 1,
            "subject 3": 1,
            "subject 10": 10,
        }
        assert response["audiences"] == {"Adult": 3, "Young Adult": 10}
        assert response["genres"] == {"Education": 2, "Horror": 1, "Drama": 10}
        assert response["languages"] == {"eng": 2, "spa": 1, "man": 10}
        assert response["publishers"] == {"Publisher 1": 3, "Publisher 10": 10}
        assert response["distributors"] == {"Gutenberg": 13}
