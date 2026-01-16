import datetime
import json
from typing import Any

import feedparser
import flask
import pytest
from werkzeug.datastructures import ImmutableMultiDict

from palace.manager.api.admin.exceptions import AdminNotAuthorized
from palace.manager.api.admin.problem_details import (
    EROTICA_FOR_ADULTS_ONLY,
    INCOMPATIBLE_GENRE,
    INVALID_DATE_FORMAT,
    INVALID_EDIT,
    INVALID_RATING,
    INVALID_SERIES_POSITION,
    METADATA_REFRESH_FAILURE,
    MISSING_CUSTOM_LIST,
    UNKNOWN_LANGUAGE,
    UNKNOWN_MEDIUM,
    UNKNOWN_ROLE,
)
from palace.manager.api.problem_details import LIBRARY_NOT_FOUND, NO_LICENSES
from palace.manager.core.classifier.simplified import SimplifiedGenreClassifier
from palace.manager.feed.annotator.admin.suppressed import AdminSuppressedAnnotator
from palace.manager.sqlalchemy.constants import IdentifierType
from palace.manager.sqlalchemy.model.admin import AdminRole
from palace.manager.sqlalchemy.model.classification import (
    Classification,
    Genre,
    Subject,
)
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.coverage import CoverageRecord
from palace.manager.sqlalchemy.model.customlist import CustomList
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import RightsStatus
from palace.manager.sqlalchemy.util import create
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException
from tests.fixtures.api_admin import AdminControllerFixture
from tests.fixtures.api_controller import ControllerFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.problem_detail import raises_problem_detail
from tests.mocks.mock import (
    AlwaysSuccessfulCoverageProvider,
    NeverSuccessfulCoverageProvider,
)


class WorkFixture(AdminControllerFixture):
    def __init__(
        self,
        controller_fixture: ControllerFixture,
    ):
        super().__init__(controller_fixture)

        self.english_1 = self.ctrl.db.work(
            "Quite British",
            "John Bull",
            language="eng",
            fiction=True,
            with_open_access_download=True,
        )
        self.english_1.license_pools[0].collection = self.ctrl.collection
        self.works = [self.english_1]

        self.manager.external_search.mock_query_works(self.works)

        self.admin.add_role(AdminRole.LIBRARIAN, self.ctrl.db.default_library())


@pytest.fixture(scope="function")
def work_fixture(
    controller_fixture: ControllerFixture,
) -> WorkFixture:
    return WorkFixture(controller_fixture)


class TestWorkController:
    def test_details(self, work_fixture: WorkFixture):
        def _links_for_rel(entry, rel: str) -> list[dict[str, Any]]:
            return [link for link in entry["links"] if link["rel"] == rel]

        [lp] = work_fixture.english_1.license_pools
        work = lp.work
        library = work_fixture.ctrl.db.default_library()

        lp.suppressed = False
        assert library not in work.suppressed_for

        with work_fixture.request_context_with_library_and_admin("/"):
            response = work_fixture.manager.admin_work_controller.details(
                lp.identifier.type, lp.identifier.identifier
            )
            assert 200 == response.status_code
            feed = feedparser.parse(response.get_data())
            [entry] = feed["entries"]
            hide_for_library = _links_for_rel(
                entry, AdminSuppressedAnnotator.REL_SUPPRESS_FOR_LIBRARY
            )
            unhide_for_library = _links_for_rel(
                entry, AdminSuppressedAnnotator.REL_UNSUPPRESS_FOR_LIBRARY
            )
            assert 1 == len(hide_for_library)
            assert 0 == len(unhide_for_library)
            assert lp.identifier.identifier in hide_for_library[0]["href"]

        work.suppressed_for.append(library)
        with work_fixture.request_context_with_library_and_admin("/"):
            response = work_fixture.manager.admin_work_controller.details(
                lp.identifier.type, lp.identifier.identifier
            )
            assert 200 == response.status_code
            feed = feedparser.parse(response.get_data())
            [entry] = feed["entries"]
            hide_for_library = _links_for_rel(
                entry, AdminSuppressedAnnotator.REL_SUPPRESS_FOR_LIBRARY
            )
            unhide_for_library = _links_for_rel(
                entry, AdminSuppressedAnnotator.REL_UNSUPPRESS_FOR_LIBRARY
            )
            assert 0 == len(hide_for_library)
            assert 1 == len(unhide_for_library)
            assert lp.identifier.identifier in unhide_for_library[0]["href"]

        lp.suppressed = True
        with work_fixture.request_context_with_library_and_admin("/"):
            response = work_fixture.manager.admin_work_controller.details(
                lp.identifier.type, lp.identifier.identifier
            )
            assert 200 == response.status_code
            feed = feedparser.parse(response.get_data())
            [entry] = feed["entries"]
            hide_for_library = _links_for_rel(
                entry, AdminSuppressedAnnotator.REL_SUPPRESS_FOR_LIBRARY
            )
            unhide_for_library = _links_for_rel(
                entry, AdminSuppressedAnnotator.REL_UNSUPPRESS_FOR_LIBRARY
            )
            assert 0 == len(hide_for_library)
            assert 0 == len(unhide_for_library)

        work_fixture.admin.remove_role(
            AdminRole.LIBRARIAN, work_fixture.ctrl.db.default_library()
        )
        with work_fixture.request_context_with_library_and_admin("/"):
            pytest.raises(
                AdminNotAuthorized,
                work_fixture.manager.admin_work_controller.details,
                lp.identifier.type,
                lp.identifier.identifier,
            )

    def test_roles(self, work_fixture: WorkFixture):
        roles = work_fixture.manager.admin_work_controller.roles()
        assert Contributor.Role.ILLUSTRATOR in list(roles.values())
        assert Contributor.Role.NARRATOR in list(roles.values())
        assert (
            Contributor.Role.ILLUSTRATOR
            == roles[Contributor.MARC_ROLE_CODES[Contributor.Role.ILLUSTRATOR]]
        )
        assert (
            Contributor.Role.NARRATOR
            == roles[Contributor.MARC_ROLE_CODES[Contributor.Role.NARRATOR]]
        )

    def test_languages(self, work_fixture: WorkFixture):
        languages = work_fixture.manager.admin_work_controller.languages()
        assert "en" in list(languages.keys())
        assert "fre" in list(languages.keys())
        names = [name for sublist in list(languages.values()) for name in sublist]
        assert "English" in names
        assert "French" in names

    def test_media(self, work_fixture: WorkFixture):
        media = work_fixture.manager.admin_work_controller.media()
        assert Edition.BOOK_MEDIUM in list(media.values())
        assert Edition.medium_to_additional_type[Edition.BOOK_MEDIUM] in list(
            media.keys()
        )

    def test_rights_status(self, work_fixture: WorkFixture):
        rights_status = work_fixture.manager.admin_work_controller.rights_status()

        public_domain = rights_status.get(RightsStatus.PUBLIC_DOMAIN_USA)
        assert RightsStatus.NAMES.get(
            RightsStatus.PUBLIC_DOMAIN_USA
        ) == public_domain.get("name")
        assert True == public_domain.get("open_access")
        assert True == public_domain.get("allows_derivatives")

        cc_by = rights_status.get(RightsStatus.CC_BY)
        assert RightsStatus.NAMES.get(RightsStatus.CC_BY) == cc_by.get("name")
        assert True == cc_by.get("open_access")
        assert True == cc_by.get("allows_derivatives")

        cc_by_nd = rights_status.get(RightsStatus.CC_BY_ND)
        assert RightsStatus.NAMES.get(RightsStatus.CC_BY_ND) == cc_by_nd.get("name")
        assert True == cc_by_nd.get("open_access")
        assert False == cc_by_nd.get("allows_derivatives")

        copyright = rights_status.get(RightsStatus.IN_COPYRIGHT)
        assert RightsStatus.NAMES.get(RightsStatus.IN_COPYRIGHT) == copyright.get(
            "name"
        )
        assert False == copyright.get("open_access")
        assert False == copyright.get("allows_derivatives")

    def _make_test_edit_request(self, fixture, data):
        [lp] = fixture.english_1.license_pools
        with fixture.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict(data)
            return fixture.manager.admin_work_controller.edit(
                lp.identifier.type, lp.identifier.identifier
            )

    def test_edit_unknown_role(self, work_fixture: WorkFixture):
        response = self._make_test_edit_request(
            work_fixture,
            [
                ("contributor-role", work_fixture.ctrl.db.fresh_str()),
                ("contributor-name", work_fixture.ctrl.db.fresh_str()),
            ],
        )
        assert 400 == response.status_code
        assert UNKNOWN_ROLE.uri == response.uri

    def test_edit_invalid_series_position(self, work_fixture: WorkFixture):
        response = self._make_test_edit_request(
            work_fixture,
            [("series", work_fixture.ctrl.db.fresh_str()), ("series_position", "five")],
        )
        assert 400 == response.status_code
        assert INVALID_SERIES_POSITION.uri == response.uri

    def test_edit_unknown_medium(self, work_fixture: WorkFixture):
        response = self._make_test_edit_request(
            work_fixture, [("medium", work_fixture.ctrl.db.fresh_str())]
        )
        assert 400 == response.status_code
        assert UNKNOWN_MEDIUM.uri == response.uri

    def test_edit_unknown_language(self, work_fixture: WorkFixture):
        response = self._make_test_edit_request(
            work_fixture, [("language", work_fixture.ctrl.db.fresh_str())]
        )
        assert 400 == response.status_code
        assert UNKNOWN_LANGUAGE.uri == response.uri

    def test_edit_invalid_date_format(self, work_fixture: WorkFixture):
        response = self._make_test_edit_request(
            work_fixture, [("issued", work_fixture.ctrl.db.fresh_str())]
        )
        assert 400 == response.status_code
        assert INVALID_DATE_FORMAT.uri == response.uri

    def test_edit_invalid_rating_not_number(self, work_fixture: WorkFixture):
        response = self._make_test_edit_request(work_fixture, [("rating", "abc")])
        assert 400 == response.status_code
        assert INVALID_RATING.uri == response.uri

    def test_edit_invalid_rating_above_scale(self, work_fixture: WorkFixture):
        response = self._make_test_edit_request(work_fixture, [("rating", 9999)])
        assert 400 == response.status_code
        assert INVALID_RATING.uri == response.uri

    def test_edit_invalid_rating_below_scale(self, work_fixture: WorkFixture):
        response = self._make_test_edit_request(work_fixture, [("rating", -3)])
        assert 400 == response.status_code
        assert INVALID_RATING.uri == response.uri

    def test_edit(self, work_fixture: WorkFixture):
        [lp] = work_fixture.english_1.license_pools

        staff_data_source = DataSource.lookup(
            work_fixture.ctrl.db.session, DataSource.LIBRARY_STAFF
        )

        def staff_edition_count():
            return (
                work_fixture.ctrl.db.session.query(Edition)
                .filter(
                    Edition.data_source == staff_data_source,
                    Edition.primary_identifier_id
                    == work_fixture.english_1.presentation_edition.primary_identifier.id,
                )
                .count()
            )

        with work_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("title", "New title"),
                    ("subtitle", "New subtitle"),
                    ("contributor-role", "Author"),
                    ("contributor-name", "New Author"),
                    ("contributor-role", "Narrator"),
                    ("contributor-name", "New Narrator"),
                    ("series", "New series"),
                    ("series_position", "144"),
                    ("medium", "Audio"),
                    ("language", "French"),
                    ("publisher", "New Publisher"),
                    ("imprint", "New Imprint"),
                    ("issued", "2017-11-05"),
                    ("rating", "2"),
                    ("summary", "<p>New summary</p>"),
                ]
            )
            response = work_fixture.manager.admin_work_controller.edit(
                lp.identifier.type, lp.identifier.identifier
            )
            assert 200 == response.status_code
            assert "New title" == work_fixture.english_1.title
            assert "New subtitle" == work_fixture.english_1.subtitle
            assert "New Author" == work_fixture.english_1.author
            [author, narrator] = sorted(
                work_fixture.english_1.presentation_edition.contributions,
                key=lambda x: x.contributor.display_name,
            )
            assert "New Author" == author.contributor.display_name
            assert "Author, New" == author.contributor.sort_name
            assert "Primary Author" == author.role
            assert "New Narrator" == narrator.contributor.display_name
            assert "Narrator, New" == narrator.contributor.sort_name
            assert "Narrator" == narrator.role
            assert "New series" == work_fixture.english_1.series
            assert 144 == work_fixture.english_1.series_position
            assert "Audio" == work_fixture.english_1.presentation_edition.medium
            assert "fre" == work_fixture.english_1.presentation_edition.language
            assert "New Publisher" == work_fixture.english_1.publisher
            assert "New Imprint" == work_fixture.english_1.presentation_edition.imprint
            assert (
                datetime.date(2017, 11, 5)
                == work_fixture.english_1.presentation_edition.issued
            )
            assert 0.25 == work_fixture.english_1.quality
            assert "<p>New summary</p>" == work_fixture.english_1.summary_text
            assert 1 == staff_edition_count()

        with work_fixture.request_context_with_library_and_admin("/"):
            # Change the summary again and add an author.
            flask.request.form = ImmutableMultiDict(
                [
                    ("title", "New title"),
                    ("subtitle", "New subtitle"),
                    ("contributor-role", "Author"),
                    ("contributor-name", "New Author"),
                    ("contributor-role", "Narrator"),
                    ("contributor-name", "New Narrator"),
                    ("contributor-role", "Author"),
                    ("contributor-name", "Second Author"),
                    ("series", "New series"),
                    ("series_position", "144"),
                    ("medium", "Audio"),
                    ("language", "French"),
                    ("publisher", "New Publisher"),
                    ("imprint", "New Imprint"),
                    ("issued", "2017-11-05"),
                    ("rating", "2"),
                    ("summary", "abcd"),
                ]
            )
            response = work_fixture.manager.admin_work_controller.edit(
                lp.identifier.type, lp.identifier.identifier
            )
            assert 200 == response.status_code
            assert "abcd" == work_fixture.english_1.summary_text
            [author, narrator, author2] = sorted(
                work_fixture.english_1.presentation_edition.contributions,
                key=lambda x: x.contributor.display_name,
            )
            assert "New Author" == author.contributor.display_name
            assert "Author, New" == author.contributor.sort_name
            assert "Primary Author" == author.role
            assert "New Narrator" == narrator.contributor.display_name
            assert "Narrator, New" == narrator.contributor.sort_name
            assert "Narrator" == narrator.role
            assert "Second Author" == author2.contributor.display_name
            assert "Author" == author2.role
            assert 1 == staff_edition_count()

        with work_fixture.request_context_with_library_and_admin("/"):
            # Now delete the subtitle, narrator, series, and summary entirely
            flask.request.form = ImmutableMultiDict(
                [
                    ("title", "New title"),
                    ("contributor-role", "Author"),
                    ("contributor-name", "New Author"),
                    ("subtitle", ""),
                    ("series", ""),
                    ("series_position", ""),
                    ("medium", "Audio"),
                    ("language", "French"),
                    ("publisher", "New Publisher"),
                    ("imprint", "New Imprint"),
                    ("issued", "2017-11-05"),
                    ("rating", "2"),
                    ("summary", ""),
                ]
            )
            response = work_fixture.manager.admin_work_controller.edit(
                lp.identifier.type, lp.identifier.identifier
            )
            assert 200 == response.status_code
            assert None == work_fixture.english_1.subtitle
            [author] = work_fixture.english_1.presentation_edition.contributions
            assert "New Author" == author.contributor.display_name
            assert None == work_fixture.english_1.series
            assert None == work_fixture.english_1.series_position
            assert "" == work_fixture.english_1.summary_text
            assert 1 == staff_edition_count()

        with work_fixture.request_context_with_library_and_admin("/"):
            # Set the fields one more time
            flask.request.form = ImmutableMultiDict(
                [
                    ("title", "New title"),
                    ("subtitle", "Final subtitle"),
                    ("series", "Final series"),
                    ("series_position", "169"),
                    ("summary", "<p>Final summary</p>"),
                ]
            )
            response = work_fixture.manager.admin_work_controller.edit(
                lp.identifier.type, lp.identifier.identifier
            )
            assert 200 == response.status_code
            assert "Final subtitle" == work_fixture.english_1.subtitle
            assert "Final series" == work_fixture.english_1.series
            assert 169 == work_fixture.english_1.series_position
            assert "<p>Final summary</p>" == work_fixture.english_1.summary_text
            assert 1 == staff_edition_count()

        # Make sure a non-librarian of this library can't edit.
        work_fixture.admin.remove_role(
            AdminRole.LIBRARIAN, work_fixture.ctrl.db.default_library()
        )
        with work_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("title", "Another new title"),
                ]
            )
            pytest.raises(
                AdminNotAuthorized,
                work_fixture.manager.admin_work_controller.edit,
                lp.identifier.type,
                lp.identifier.identifier,
            )

    def test_edit_classifications(self, work_fixture: WorkFixture):
        # start with a couple genres based on BISAC classifications from Axis 360
        work = work_fixture.english_1
        [lp] = work.license_pools
        primary_identifier = work.presentation_edition.primary_identifier
        work.audience = "Adult"
        work.fiction = True
        axis_360 = DataSource.lookup(work_fixture.ctrl.db.session, DataSource.BOUNDLESS)
        classification1 = primary_identifier.classify(
            data_source=axis_360,
            subject_type=Subject.BISAC,
            subject_identifier="FICTION / Horror",
            weight=1,
        )
        classification2 = primary_identifier.classify(
            data_source=axis_360,
            subject_type=Subject.BISAC,
            subject_identifier="FICTION / Science Fiction / Time Travel",
            weight=1,
        )
        genre1, ignore = Genre.lookup(work_fixture.ctrl.db.session, "Horror")
        genre2, ignore = Genre.lookup(work_fixture.ctrl.db.session, "Science Fiction")
        work.genres = [genre1, genre2]

        # make no changes
        with work_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("audience", "Adult"),
                    ("fiction", "fiction"),
                    ("genres", "Horror"),
                    ("genres", "Science Fiction"),
                ]
            )
            requested_genres = flask.request.form.getlist("genres")
            response = work_fixture.manager.admin_work_controller.edit_classifications(
                lp.identifier.type, lp.identifier.identifier
            )
            assert response.status_code == 200

        staff_data_source = DataSource.lookup(
            work_fixture.ctrl.db.session, DataSource.LIBRARY_STAFF
        )
        genre_classifications = (
            work_fixture.ctrl.db.session.query(Classification)
            .join(Subject)
            .filter(
                Classification.identifier == primary_identifier,
                Classification.data_source == staff_data_source,
                Subject.genre_id != None,
            )
        )
        staff_genres = [
            c.subject.genre.name for c in genre_classifications if c.subject.genre
        ]
        assert staff_genres == []
        assert "Adult" == work.audience
        assert 18 == work.target_age.lower
        assert None == work.target_age.upper
        assert True == work.fiction

        # remove all genres
        with work_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict(
                [("audience", "Adult"), ("fiction", "fiction")]
            )
            response = work_fixture.manager.admin_work_controller.edit_classifications(
                lp.identifier.type, lp.identifier.identifier
            )
            assert response.status_code == 200

        primary_identifier = work.presentation_edition.primary_identifier
        staff_data_source = DataSource.lookup(
            work_fixture.ctrl.db.session, DataSource.LIBRARY_STAFF
        )
        none_classification_count = (
            work_fixture.ctrl.db.session.query(Classification)
            .join(Subject)
            .filter(
                Classification.identifier == primary_identifier,
                Classification.data_source == staff_data_source,
                Subject.identifier == SimplifiedGenreClassifier.NONE,
            )
            .all()
        )
        assert 1 == len(none_classification_count)
        assert "Adult" == work.audience
        assert 18 == work.target_age.lower
        assert None == work.target_age.upper
        assert True == work.fiction

        # completely change genres
        with work_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("audience", "Adult"),
                    ("fiction", "fiction"),
                    ("genres", "Drama"),
                    ("genres", "Urban Fantasy"),
                    ("genres", "Women's Fiction"),
                ]
            )
            requested_genres = flask.request.form.getlist("genres")
            response = work_fixture.manager.admin_work_controller.edit_classifications(
                lp.identifier.type, lp.identifier.identifier
            )
            assert response.status_code == 200

        new_genre_names = [work_genre.genre.name for work_genre in work.work_genres]

        assert sorted(new_genre_names) == sorted(requested_genres)
        assert "Adult" == work.audience
        assert 18 == work.target_age.lower
        assert None == work.target_age.upper
        assert True == work.fiction

        # remove some genres and change audience and target age
        with work_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("audience", "Young Adult"),
                    ("target_age_min", "16"),
                    ("target_age_max", "18"),
                    ("fiction", "fiction"),
                    ("genres", "Urban Fantasy"),
                ]
            )
            requested_genres = flask.request.form.getlist("genres")
            response = work_fixture.manager.admin_work_controller.edit_classifications(
                lp.identifier.type, lp.identifier.identifier
            )
            assert response.status_code == 200

        # new_genre_names = work_fixture.controller_fixture.db.session.query(WorkGenre).filter(WorkGenre.work_id == work.id).all()
        new_genre_names = [work_genre.genre.name for work_genre in work.work_genres]
        assert sorted(new_genre_names) == sorted(requested_genres)
        assert "Young Adult" == work.audience
        assert 16 == work.target_age.lower
        assert 19 == work.target_age.upper
        assert True == work.fiction

        previous_genres = new_genre_names

        # try to add a nonfiction genre
        with work_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("audience", "Young Adult"),
                    ("target_age_min", "16"),
                    ("target_age_max", "18"),
                    ("fiction", "fiction"),
                    ("genres", "Cooking"),
                    ("genres", "Urban Fantasy"),
                ]
            )
            response = work_fixture.manager.admin_work_controller.edit_classifications(
                lp.identifier.type, lp.identifier.identifier
            )

        assert response == INCOMPATIBLE_GENRE
        new_genre_names = [work_genre.genre.name for work_genre in work.work_genres]
        assert sorted(new_genre_names) == sorted(previous_genres)
        assert "Young Adult" == work.audience
        assert 16 == work.target_age.lower
        assert 19 == work.target_age.upper
        assert True == work.fiction

        # try to add Erotica
        with work_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("audience", "Young Adult"),
                    ("target_age_min", "16"),
                    ("target_age_max", "18"),
                    ("fiction", "fiction"),
                    ("genres", "Erotica"),
                    ("genres", "Urban Fantasy"),
                ]
            )
            response = work_fixture.manager.admin_work_controller.edit_classifications(
                lp.identifier.type, lp.identifier.identifier
            )
            assert response == EROTICA_FOR_ADULTS_ONLY

        new_genre_names = [work_genre.genre.name for work_genre in work.work_genres]
        assert sorted(new_genre_names) == sorted(previous_genres)
        assert "Young Adult" == work.audience
        assert 16 == work.target_age.lower
        assert 19 == work.target_age.upper
        assert True == work.fiction

        # try to set min target age greater than max target age
        # othe edits should not go through
        with work_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("audience", "Young Adult"),
                    ("target_age_min", "16"),
                    ("target_age_max", "14"),
                    ("fiction", "nonfiction"),
                    ("genres", "Cooking"),
                ]
            )
            response = work_fixture.manager.admin_work_controller.edit_classifications(
                lp.identifier.type, lp.identifier.identifier
            )
            assert 400 == response.status_code
            assert INVALID_EDIT.uri == response.uri

        new_genre_names = [work_genre.genre.name for work_genre in work.work_genres]
        assert sorted(new_genre_names) == sorted(previous_genres)
        assert True == work.fiction

        # change to nonfiction with nonfiction genres and new target age
        with work_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("audience", "Young Adult"),
                    ("target_age_min", "15"),
                    ("target_age_max", "17"),
                    ("fiction", "nonfiction"),
                    ("genres", "Cooking"),
                ]
            )
            requested_genres = flask.request.form.getlist("genres")
            response = work_fixture.manager.admin_work_controller.edit_classifications(
                lp.identifier.type, lp.identifier.identifier
            )

        new_genre_names = [work_genre.genre.name for work_genre in lp.work.work_genres]
        assert sorted(new_genre_names) == sorted(requested_genres)
        assert "Young Adult" == work.audience
        assert 15 == work.target_age.lower
        assert 18 == work.target_age.upper
        assert False == work.fiction

        # set to Adult and make sure that target ages is set automatically
        with work_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("audience", "Adult"),
                    ("fiction", "nonfiction"),
                    ("genres", "Cooking"),
                ]
            )
            requested_genres = flask.request.form.getlist("genres")
            response = work_fixture.manager.admin_work_controller.edit_classifications(
                lp.identifier.type, lp.identifier.identifier
            )

        assert "Adult" == work.audience
        assert 18 == work.target_age.lower
        assert None == work.target_age.upper

        # Make sure a non-librarian of this library can't edit.
        work_fixture.admin.remove_role(
            AdminRole.LIBRARIAN, work_fixture.ctrl.db.default_library()
        )
        with work_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("audience", "Children"),
                    ("fiction", "nonfiction"),
                    ("genres", "Biography"),
                ]
            )
            pytest.raises(
                AdminNotAuthorized,
                work_fixture.manager.admin_work_controller.edit_classifications,
                lp.identifier.type,
                lp.identifier.identifier,
            )

    def test_suppress(self, work_fixture: WorkFixture):
        work = work_fixture.english_1
        [lp] = work.license_pools
        library = work_fixture.ctrl.db.default_library()

        assert library not in work.suppressed_for

        work_fixture.admin.add_role(AdminRole.LIBRARY_MANAGER, library=library)

        # test success
        with work_fixture.request_context_with_library_and_admin("/"):
            response = work_fixture.manager.admin_work_controller.suppress(
                lp.identifier.type, lp.identifier.identifier
            )
            assert 200 == response.status_code
            assert library in work.suppressed_for

        # We should not fail if we suppress the already-suppressed work again.
        with work_fixture.request_context_with_library_and_admin("/"):
            response = work_fixture.manager.admin_work_controller.suppress(
                lp.identifier.type, lp.identifier.identifier
            )
            assert 200 == response.status_code
            assert library in work.suppressed_for

        # test non-existent id
        with work_fixture.request_context_with_library_and_admin("/"):
            response = work_fixture.manager.admin_work_controller.suppress(
                IdentifierType.URI.value, "http://non-existent-id"
            )
            assert isinstance(response, ProblemDetail)

        # test unauthorized
        work_fixture.admin.remove_role(AdminRole.LIBRARY_MANAGER, library=library)
        with work_fixture.request_context_with_library_and_admin("/"):
            pytest.raises(
                AdminNotAuthorized,
                work_fixture.manager.admin_work_controller.suppress,
                lp.identifier.type,
                lp.identifier.identifier,
            )

        # test no library
        with work_fixture.request_context_with_library_and_admin("/", library=None):
            with pytest.raises(ProblemDetailException) as exc:
                work_fixture.manager.admin_work_controller.suppress(
                    lp.identifier.type, lp.identifier.identifier
                )
            assert exc.value.problem_detail == LIBRARY_NOT_FOUND

    def test_unsuppress(self, work_fixture: WorkFixture):
        work = work_fixture.english_1
        [lp] = work.license_pools
        library = work_fixture.ctrl.db.default_library()

        assert library not in work.suppressed_for

        work.suppressed_for.append(library)
        work_fixture.admin.add_role(AdminRole.LIBRARY_MANAGER, library=library)

        with work_fixture.request_context_with_library_and_admin("/"):
            response = work_fixture.manager.admin_work_controller.unsuppress(
                lp.identifier.type, lp.identifier.identifier
            )
            assert 200 == response.status_code
            assert library not in work.suppressed_for

        # We should not fail if we unsuppress the already-unsuppressed work again.
        with work_fixture.request_context_with_library_and_admin("/"):
            response = work_fixture.manager.admin_work_controller.unsuppress(
                lp.identifier.type, lp.identifier.identifier
            )
            assert 200 == response.status_code
            assert library not in work.suppressed_for

        with work_fixture.request_context_with_library_and_admin("/"):
            response = work_fixture.manager.admin_work_controller.unsuppress(
                IdentifierType.URI.value, "http://non-existent-id"
            )
            assert isinstance(response, ProblemDetail)

        # test unauthorized
        work_fixture.admin.remove_role(AdminRole.LIBRARY_MANAGER, library=library)
        with work_fixture.request_context_with_library_and_admin("/"):
            pytest.raises(
                AdminNotAuthorized,
                work_fixture.manager.admin_work_controller.unsuppress,
                lp.identifier.type,
                lp.identifier.identifier,
            )

        # test no library
        with work_fixture.request_context_with_library_and_admin("/", library=None):
            with pytest.raises(ProblemDetailException) as exc:
                work_fixture.manager.admin_work_controller.unsuppress(
                    lp.identifier.type, lp.identifier.identifier
                )
            assert exc.value.problem_detail == LIBRARY_NOT_FOUND

    def test_refresh_metadata(self, work_fixture: WorkFixture):
        wrangler = DataSource.lookup(
            work_fixture.ctrl.db.session, DataSource.METADATA_WRANGLER
        )

        class AlwaysSuccessfulMetadataProvider(AlwaysSuccessfulCoverageProvider):
            DATA_SOURCE_NAME = wrangler.name

        success_provider = AlwaysSuccessfulMetadataProvider(
            work_fixture.ctrl.db.session
        )

        class NeverSuccessfulMetadataProvider(NeverSuccessfulCoverageProvider):
            DATA_SOURCE_NAME = wrangler.name

        failure_provider = NeverSuccessfulMetadataProvider(work_fixture.ctrl.db.session)

        with work_fixture.request_context_with_library_and_admin("/"):
            [lp] = work_fixture.english_1.license_pools
            response = work_fixture.manager.admin_work_controller.refresh_metadata(
                lp.identifier.type, lp.identifier.identifier, provider=success_provider
            )
            assert 200 == response.status_code
            # Also, the work has a coverage record now for the wrangler.
            assert CoverageRecord.lookup(lp.identifier, wrangler)

            response = work_fixture.manager.admin_work_controller.refresh_metadata(
                lp.identifier.type, lp.identifier.identifier, provider=failure_provider
            )
            assert METADATA_REFRESH_FAILURE.status_code == response.status_code
            assert METADATA_REFRESH_FAILURE.detail == response.detail

            # If we don't pass in a provider, it will also fail because there
            # isn't one connfigured.
            response = work_fixture.manager.admin_work_controller.refresh_metadata(
                lp.identifier.type, lp.identifier.identifier
            )
            assert METADATA_REFRESH_FAILURE.status_code == response.status_code
            assert METADATA_REFRESH_FAILURE.detail == response.detail

        work_fixture.admin.remove_role(
            AdminRole.LIBRARIAN, work_fixture.ctrl.db.default_library()
        )
        with work_fixture.request_context_with_library_and_admin("/"):
            pytest.raises(
                AdminNotAuthorized,
                work_fixture.manager.admin_work_controller.refresh_metadata,
                lp.identifier.type,
                lp.identifier.identifier,
                provider=success_provider,
            )

    def test_classifications(self, work_fixture: WorkFixture):
        e, pool = work_fixture.ctrl.db.edition(with_license_pool=True)
        work = work_fixture.ctrl.db.work(presentation_edition=e)
        identifier = work.presentation_edition.primary_identifier
        genres = work_fixture.ctrl.db.session.query(Genre).all()
        subject1 = work_fixture.ctrl.db.subject(type="type1", identifier="subject1")
        subject1.genre = genres[0]
        subject2 = work_fixture.ctrl.db.subject(type="type2", identifier="subject2")
        subject2.genre = genres[1]
        subject3 = work_fixture.ctrl.db.subject(type="type2", identifier="subject3")
        subject3.genre = None
        source = DataSource.lookup(work_fixture.ctrl.db.session, DataSource.BOUNDLESS)
        classification1 = work_fixture.ctrl.db.classification(
            identifier=identifier, subject=subject1, data_source=source, weight=1
        )
        classification2 = work_fixture.ctrl.db.classification(
            identifier=identifier, subject=subject2, data_source=source, weight=3
        )
        classification3 = work_fixture.ctrl.db.classification(
            identifier=identifier, subject=subject3, data_source=source, weight=2
        )

        [lp] = work.license_pools

        with work_fixture.request_context_with_library_and_admin("/"):
            response = work_fixture.manager.admin_work_controller.classifications(
                lp.identifier.type, lp.identifier.identifier
            )
            assert response["book"]["identifier_type"] == lp.identifier.type
            assert response["book"]["identifier"] == lp.identifier.identifier

            expected_results = [classification2, classification3, classification1]
            assert len(response["classifications"]) == len(expected_results)
            for i, classification in enumerate(expected_results):
                subject = classification.subject
                source = classification.data_source
                assert response["classifications"][i]["name"] == subject.identifier
                assert response["classifications"][i]["type"] == subject.type
                assert response["classifications"][i]["source"] == source.name
                assert response["classifications"][i]["weight"] == classification.weight

        work_fixture.admin.remove_role(
            AdminRole.LIBRARIAN, work_fixture.ctrl.db.default_library()
        )
        with work_fixture.request_context_with_library_and_admin("/"):
            pytest.raises(
                AdminNotAuthorized,
                work_fixture.manager.admin_work_controller.classifications,
                lp.identifier.type,
                lp.identifier.identifier,
            )

    def test_custom_lists_get(
        self, work_fixture: WorkFixture, db: DatabaseTransactionFixture
    ) -> None:
        # Test with non-existent identifier.
        with (
            work_fixture.request_context_with_library_and_admin("/"),
            raises_problem_detail(
                pd=NO_LICENSES.detailed(
                    "The item you're asking about (URI/http://non-existent-id) isn't in this collection."
                )
            ),
        ):
            work_fixture.manager.admin_work_controller.custom_lists(
                IdentifierType.URI.value, "http://non-existent-id"
            )

        # Test normal case. Only custom lists for the current library are returned.
        staff_data_source = DataSource.lookup(db.session, DataSource.LIBRARY_STAFF)
        custom_list, ignore = create(
            db.session,
            CustomList,
            name=db.fresh_str(),
            library=db.default_library(),
            data_source=staff_data_source,
        )
        other_library_custom_list, ignore = create(
            db.session,
            CustomList,
            name=db.fresh_str(),
            library=db.library(),
            data_source=staff_data_source,
        )
        work = db.work(with_license_pool=True)
        custom_list.add_entry(work)
        other_library_custom_list.add_entry(work)
        identifier = work.presentation_edition.primary_identifier

        with work_fixture.request_context_with_library_and_admin("/"):
            response = work_fixture.manager.admin_work_controller.custom_lists(
                identifier.type, identifier.identifier
            )
            assert isinstance(response, dict)
            lists = response.get("custom_lists")
            assert isinstance(lists, list)
            assert 1 == len(lists)
            [custom_list_response] = lists
            assert custom_list.id == custom_list_response.get("id")
            assert custom_list.name == custom_list_response.get("name")

        # Test lack permissions.
        work_fixture.admin.remove_role(AdminRole.LIBRARIAN, db.default_library())
        with (
            work_fixture.request_context_with_library_and_admin("/"),
            pytest.raises(AdminNotAuthorized),
        ):
            work_fixture.manager.admin_work_controller.custom_lists(
                identifier.type,
                identifier.identifier,
            )

    def test_custom_lists_post_error_non_existant(
        self, work_fixture: WorkFixture, db: DatabaseTransactionFixture
    ) -> None:
        staff_data_source = DataSource.lookup(db.session, DataSource.LIBRARY_STAFF)
        work = db.work(with_license_pool=True)
        identifier = work.presentation_edition.primary_identifier
        # Try adding the work to a list that doesn't exist.
        deleted_custom_list, _ = create(
            db.session,
            CustomList,
            name=db.fresh_str(),
            library=db.default_library(),
            data_source=staff_data_source,
        )
        deleted_list_id = deleted_custom_list.id
        db.session.delete(deleted_custom_list)
        with (
            work_fixture.request_context_with_library_and_admin("/", method="POST"),
            raises_problem_detail(
                pd=MISSING_CUSTOM_LIST.detailed(
                    'Could not find list "non-existent list"'
                )
            ),
        ):
            flask.request.form = ImmutableMultiDict(
                [
                    (
                        "lists",
                        json.dumps(
                            [{"name": "non-existent list", "id": deleted_list_id}]
                        ),
                    )
                ]
            )
            work_fixture.manager.admin_work_controller.custom_lists(
                identifier.type, identifier.identifier
            )

    def test_custom_lists_post_error_bad_data(
        self, work_fixture: WorkFixture, db: DatabaseTransactionFixture
    ) -> None:
        work = db.work(with_license_pool=True)
        identifier = work.presentation_edition.primary_identifier

        # Try sending bad data
        with (
            work_fixture.request_context_with_library_and_admin("/", method="POST"),
            raises_problem_detail(detail="Invalid form data"),
        ):
            flask.request.form = ImmutableMultiDict(
                [("lists", json.dumps("Complete garbage 🗑️"))]
            )
            work_fixture.manager.admin_work_controller.custom_lists(
                identifier.type, identifier.identifier
            )

    def test_custom_lists_post_error_no_access(
        self, work_fixture: WorkFixture, db: DatabaseTransactionFixture
    ) -> None:
        staff_data_source = DataSource.lookup(db.session, DataSource.LIBRARY_STAFF)
        work = db.work(with_license_pool=True)
        identifier = work.presentation_edition.primary_identifier

        # Try adding work to a list that the library doesn't have access to.
        other_libraries_list, _ = create(
            db.session,
            CustomList,
            name=db.fresh_str(),
            library=db.library(),
            data_source=staff_data_source,
        )
        with (
            work_fixture.request_context_with_library_and_admin("/", method="POST"),
            raises_problem_detail(
                pd=MISSING_CUSTOM_LIST.detailed(
                    f'Could not find list "{other_libraries_list.name}"'
                )
            ),
        ):
            flask.request.form = ImmutableMultiDict(
                [
                    (
                        "lists",
                        json.dumps(
                            [
                                {
                                    "id": str(other_libraries_list.id),
                                    "name": other_libraries_list.name,
                                }
                            ]
                        ),
                    )
                ]
            )
            work_fixture.manager.admin_work_controller.custom_lists(
                identifier.type, identifier.identifier
            )

    def test_custom_lists_post(
        self, work_fixture: WorkFixture, db: DatabaseTransactionFixture
    ) -> None:
        staff_data_source = DataSource.lookup(db.session, DataSource.LIBRARY_STAFF)
        custom_list, _ = create(
            db.session,
            CustomList,
            name=db.fresh_str(),
            library=db.default_library(),
            data_source=staff_data_source,
        )
        work = db.work(with_license_pool=True)
        identifier = work.presentation_edition.primary_identifier

        # Create a Lane that depends on this CustomList for its membership.
        lane = db.lane()
        lane.customlists.append(custom_list)
        lane.size = 300

        # Add the list to the work.
        with work_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    (
                        "lists",
                        json.dumps(
                            [{"id": str(custom_list.id), "name": custom_list.name}]
                        ),
                    )
                ]
            )
            response = work_fixture.manager.admin_work_controller.custom_lists(
                identifier.type, identifier.identifier
            )
            assert 200 == response.status_code
            assert 1 == len(work.custom_list_entries)
            assert 1 == len(custom_list.entries)
            assert custom_list == work.custom_list_entries[0].customlist
            assert True == work.custom_list_entries[0].featured

        # Now remove the work from the list.
        with work_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("lists", json.dumps([])),
                ]
            )
            response = work_fixture.manager.admin_work_controller.custom_lists(
                identifier.type, identifier.identifier
            )
        assert 200 == response.status_code
        assert 0 == len(work.custom_list_entries)
        assert 0 == len(custom_list.entries)

        # Add a list that didn't exist before.
        with work_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [("lists", json.dumps([{"name": "new list"}]))]
            )
            response = work_fixture.manager.admin_work_controller.custom_lists(
                identifier.type, identifier.identifier
            )
        assert 200 == response.status_code
        assert 1 == len(work.custom_list_entries)
        new_list = CustomList.find(
            db.session,
            "new list",
            staff_data_source,
            db.default_library(),
        )
        assert new_list == work.custom_list_entries[0].customlist
        assert True == work.custom_list_entries[0].featured

        work_fixture.admin.remove_role(AdminRole.LIBRARIAN, db.default_library())
        with (
            work_fixture.request_context_with_library_and_admin("/", method="POST"),
            pytest.raises(AdminNotAuthorized),
        ):
            flask.request.form = ImmutableMultiDict(
                [("lists", json.dumps([{"name": "another new list"}]))]
            )
            work_fixture.manager.admin_work_controller.custom_lists(
                identifier.type,
                identifier.identifier,
            )
