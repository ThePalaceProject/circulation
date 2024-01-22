import json
from collections.abc import Generator

import feedparser
import flask
import pytest
from werkzeug.datastructures import ImmutableMultiDict

from api.admin.controller.custom_lists import CustomListsController
from api.admin.exceptions import AdminNotAuthorized
from api.admin.problem_details import (
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
from core.classifier import SimplifiedGenreClassifier
from core.model import (
    AdminRole,
    Classification,
    Contributor,
    CoverageRecord,
    CustomList,
    DataSource,
    Edition,
    Genre,
    RightsStatus,
    Subject,
    create,
)
from core.util.datetime_helpers import datetime_utc
from tests.core.mock import (
    AlwaysSuccessfulCoverageProvider,
    NeverSuccessfulCoverageProvider,
)
from tests.core.util.test_flask_util import add_request_context
from tests.fixtures.api_admin import AdminControllerFixture
from tests.fixtures.api_controller import ControllerFixture


class WorkFixture(AdminControllerFixture):
    def __init__(self, controller_fixture: ControllerFixture):
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
) -> Generator[WorkFixture, None, None]:
    fixture = WorkFixture(controller_fixture)
    with fixture.ctrl.wired_container():
        yield fixture


class TestWorkController:
    def test_details(self, work_fixture: WorkFixture):
        [lp] = work_fixture.english_1.license_pools

        lp.suppressed = False
        with work_fixture.request_context_with_library_and_admin("/"):
            response = work_fixture.manager.admin_work_controller.details(
                lp.identifier.type, lp.identifier.identifier
            )
            assert 200 == response.status_code
            feed = feedparser.parse(response.get_data())
            [entry] = feed["entries"]
            suppress_links = [
                x["href"]
                for x in entry["links"]
                if x["rel"] == "http://librarysimplified.org/terms/rel/hide"
            ]
            unsuppress_links = [
                x["href"]
                for x in entry["links"]
                if x["rel"] == "http://librarysimplified.org/terms/rel/restore"
            ]
            assert 0 == len(unsuppress_links)
            assert 1 == len(suppress_links)
            assert lp.identifier.identifier in suppress_links[0]

        lp.suppressed = True
        with work_fixture.request_context_with_library_and_admin("/"):
            response = work_fixture.manager.admin_work_controller.details(
                lp.identifier.type, lp.identifier.identifier
            )
            assert 200 == response.status_code
            feed = feedparser.parse(response.get_data())
            [entry] = feed["entries"]
            suppress_links = [
                x["href"]
                for x in entry["links"]
                if x["rel"] == "http://librarysimplified.org/terms/rel/hide"
            ]
            unsuppress_links = [
                x["href"]
                for x in entry["links"]
                if x["rel"] == "http://librarysimplified.org/terms/rel/restore"
            ]
            assert 0 == len(suppress_links)
            assert 1 == len(unsuppress_links)
            assert lp.identifier.identifier in unsuppress_links[0]

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
        assert Contributor.ILLUSTRATOR_ROLE in list(roles.values())
        assert Contributor.NARRATOR_ROLE in list(roles.values())
        assert (
            Contributor.ILLUSTRATOR_ROLE
            == roles[Contributor.MARC_ROLE_CODES[Contributor.ILLUSTRATOR_ROLE]]
        )
        assert (
            Contributor.NARRATOR_ROLE
            == roles[Contributor.MARC_ROLE_CODES[Contributor.NARRATOR_ROLE]]
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
                datetime_utc(2017, 11, 5)
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
        axis_360 = DataSource.lookup(work_fixture.ctrl.db.session, DataSource.AXIS_360)
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
        [lp] = work_fixture.english_1.license_pools

        with work_fixture.request_context_with_library_and_admin("/"):
            response = work_fixture.manager.admin_work_controller.suppress(
                lp.identifier.type, lp.identifier.identifier
            )
            assert 200 == response.status_code
            assert True == lp.suppressed

        lp.suppressed = False
        work_fixture.admin.remove_role(
            AdminRole.LIBRARIAN, work_fixture.ctrl.db.default_library()
        )
        with work_fixture.request_context_with_library_and_admin("/"):
            pytest.raises(
                AdminNotAuthorized,
                work_fixture.manager.admin_work_controller.suppress,
                lp.identifier.type,
                lp.identifier.identifier,
            )

    def test_unsuppress(self, work_fixture: WorkFixture):
        [lp] = work_fixture.english_1.license_pools
        lp.suppressed = True

        broken_lp = work_fixture.ctrl.db.licensepool(
            work_fixture.english_1.presentation_edition,
            data_source_name=DataSource.OVERDRIVE,
        )
        work_fixture.english_1.license_pools.append(broken_lp)
        broken_lp.suppressed = True

        with work_fixture.request_context_with_library_and_admin("/"):
            response = work_fixture.manager.admin_work_controller.unsuppress(
                lp.identifier.type, lp.identifier.identifier
            )

            # Both LicensePools are unsuppressed, even though one of them
            # has a LicensePool-specific complaint.
            assert 200 == response.status_code
            assert False == lp.suppressed
            assert False == broken_lp.suppressed

        lp.suppressed = True
        work_fixture.admin.remove_role(
            AdminRole.LIBRARIAN, work_fixture.ctrl.db.default_library()
        )
        with work_fixture.request_context_with_library_and_admin("/"):
            pytest.raises(
                AdminNotAuthorized,
                work_fixture.manager.admin_work_controller.unsuppress,
                lp.identifier.type,
                lp.identifier.identifier,
            )

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
        source = DataSource.lookup(work_fixture.ctrl.db.session, DataSource.AXIS_360)
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

    def test_custom_lists_get(self, work_fixture: WorkFixture):
        staff_data_source = DataSource.lookup(
            work_fixture.ctrl.db.session, DataSource.LIBRARY_STAFF
        )
        list, ignore = create(
            work_fixture.ctrl.db.session,
            CustomList,
            name=work_fixture.ctrl.db.fresh_str(),
            library=work_fixture.ctrl.db.default_library(),
            data_source=staff_data_source,
        )
        work = work_fixture.ctrl.db.work(with_license_pool=True)
        list.add_entry(work)
        identifier = work.presentation_edition.primary_identifier

        with work_fixture.request_context_with_library_and_admin("/"):
            response = work_fixture.manager.admin_work_controller.custom_lists(
                identifier.type, identifier.identifier
            )
            lists = response.get("custom_lists")
            assert 1 == len(lists)
            assert list.id == lists[0].get("id")
            assert list.name == lists[0].get("name")

        work_fixture.admin.remove_role(
            AdminRole.LIBRARIAN, work_fixture.ctrl.db.default_library()
        )
        with work_fixture.request_context_with_library_and_admin("/"):
            pytest.raises(
                AdminNotAuthorized,
                work_fixture.manager.admin_work_controller.custom_lists,
                identifier.type,
                identifier.identifier,
            )

    def test_custom_lists_edit_with_missing_list(self, work_fixture: WorkFixture):
        work = work_fixture.ctrl.db.work(with_license_pool=True)
        identifier = work.presentation_edition.primary_identifier

        with work_fixture.request_context_with_library_and_admin("/", method="POST"):
            form = ImmutableMultiDict(
                [
                    ("id", "4"),
                    ("name", "name"),
                ]
            )
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )

            response = work_fixture.manager.admin_custom_lists_controller.custom_lists()
            assert MISSING_CUSTOM_LIST == response

    def test_custom_lists_edit_success(self, work_fixture: WorkFixture):
        staff_data_source = DataSource.lookup(
            work_fixture.ctrl.db.session, DataSource.LIBRARY_STAFF
        )
        list, ignore = create(
            work_fixture.ctrl.db.session,
            CustomList,
            name=work_fixture.ctrl.db.fresh_str(),
            library=work_fixture.ctrl.db.default_library(),
            data_source=staff_data_source,
        )
        work = work_fixture.ctrl.db.work(with_license_pool=True)
        identifier = work.presentation_edition.primary_identifier

        # Whenever the mocked search engine is asked how many
        # works are in a Lane, it will say there are two.
        work_fixture.ctrl.controller.search_engine.docs = dict(id1="doc1", id2="doc2")

        # Create a Lane that depends on this CustomList for its membership.
        lane = work_fixture.ctrl.db.lane()
        lane.customlists.append(list)
        lane.size = 300

        # Add the list to the work.
        with work_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [("lists", json.dumps([{"id": str(list.id), "name": list.name}]))]
            )
            response = work_fixture.manager.admin_work_controller.custom_lists(
                identifier.type, identifier.identifier
            )
            assert 200 == response.status_code
            assert 1 == len(work.custom_list_entries)
            assert 1 == len(list.entries)
            assert list == work.custom_list_entries[0].customlist
            assert True == work.custom_list_entries[0].featured

        # Now remove the work from the list.
        work_fixture.ctrl.controller.search_engine.docs = dict(id1="doc1")
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
        assert 0 == len(list.entries)

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
            work_fixture.ctrl.db.session,
            "new list",
            staff_data_source,
            work_fixture.ctrl.db.default_library(),
        )
        assert new_list == work.custom_list_entries[0].customlist
        assert True == work.custom_list_entries[0].featured

        work_fixture.admin.remove_role(
            AdminRole.LIBRARIAN, work_fixture.ctrl.db.default_library()
        )
        with work_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [("lists", json.dumps([{"name": "another new list"}]))]
            )
            pytest.raises(
                AdminNotAuthorized,
                work_fixture.manager.admin_work_controller.custom_lists,
                identifier.type,
                identifier.identifier,
            )
