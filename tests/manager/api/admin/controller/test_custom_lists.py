import json
from typing import Any, Literal
from unittest import mock

import feedparser
import flask
import pytest
from attr import define
from werkzeug.datastructures import ImmutableMultiDict

from palace.manager.api.admin.exceptions import AdminNotAuthorized
from palace.manager.api.admin.model.custom_lists import CustomListPostRequest
from palace.manager.api.admin.problem_details import (
    AUTO_UPDATE_CUSTOM_LIST_CANNOT_HAVE_ENTRIES,
    CANNOT_CHANGE_LIBRARY_FOR_CUSTOM_LIST,
    COLLECTION_NOT_ACTIVE_FOR_LIST_LIBRARY,
    CUSTOM_LIST_NAME_ALREADY_IN_USE,
    MISSING_COLLECTION,
    MISSING_CUSTOM_LIST,
)
from palace.manager.api.problem_details import CANNOT_DELETE_SHARED_LIST
from palace.manager.search.pagination import Pagination
from palace.manager.sqlalchemy.model.admin import Admin, AdminRole
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.customlist import CustomList, CustomListEntry
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.lane import Lane
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.util import create, get_one
from palace.manager.util.problem_detail import ProblemDetail
from tests.fixtures.api_admin import AdminLibrarianFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.mocks.flask import add_request_context
from tests.mocks.search import ExternalSearchIndexFake, SearchServiceFake


class TestCustomListsController:
    def test_custom_lists_get(self, admin_librarian_fixture: AdminLibrarianFixture):
        # This list has no associated Library and should not be included.
        no_library, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
        )

        auto_update_query = json.dumps(dict(query=dict(key="key", value="value")))
        one_entry, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name="One entry list",
            library=admin_librarian_fixture.ctrl.db.default_library(),
            auto_update_enabled=True,
            auto_update_query=auto_update_query,
        )
        edition = admin_librarian_fixture.ctrl.db.edition()
        one_entry.add_entry(edition)
        collection = admin_librarian_fixture.ctrl.db.collection()
        collection.customlists = [one_entry]

        no_entries, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name="A no entries list",
            library=admin_librarian_fixture.ctrl.db.default_library(),
            auto_update_enabled=False,
        )

        no_entries.shared_locally = True

        with admin_librarian_fixture.request_context_with_library_and_admin("/"):
            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert isinstance(response, dict)

            lists = response.get("custom_lists")
            assert isinstance(lists, list)
            assert 2 == len(lists)

            # The lists are returned in alphabetical order by name, so the first list
            # is the one with no entries.
            [no_entries_response, one_entry_response] = lists

            assert one_entry.id == one_entry_response.get("id")
            assert one_entry.name == one_entry_response.get("name")
            assert 1 == one_entry_response.get("entry_count")
            assert 1 == len(one_entry_response.get("collections"))
            [c] = one_entry_response.get("collections")
            assert collection.name == c.get("name")
            assert collection.id == c.get("id")
            assert collection.protocol == c.get("protocol")
            assert True == one_entry_response.get("auto_update")
            assert auto_update_query == one_entry_response.get("auto_update_query")
            assert CustomList.INIT == one_entry_response.get("auto_update_status")
            assert False == one_entry_response.get("is_shared")
            assert True == one_entry_response.get("is_owner")

            assert no_entries.id == no_entries_response.get("id")
            assert no_entries.name == no_entries_response.get("name")
            assert 0 == no_entries_response.get("entry_count")
            assert 0 == len(no_entries_response.get("collections"))
            assert False == no_entries_response.get("auto_update")
            assert None == no_entries_response.get("auto_update_query")
            assert CustomList.INIT == no_entries_response.get("auto_update_status")
            assert True == no_entries_response.get("is_shared")
            assert True == no_entries_response.get("is_owner")

        admin_librarian_fixture.admin.remove_role(
            AdminRole.LIBRARIAN, admin_librarian_fixture.ctrl.db.default_library()
        )
        with admin_librarian_fixture.request_context_with_library_and_admin("/"):
            pytest.raises(
                AdminNotAuthorized,
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists,
            )

    def test_custom_lists_post_errors(
        self, admin_librarian_fixture: AdminLibrarianFixture
    ):
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            form = ImmutableMultiDict(
                [
                    ("id", "4"),
                    ("name", "name"),
                ]
            )
            add_request_context(flask.request, CustomListPostRequest, form=form)
            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert MISSING_CUSTOM_LIST == response

        library = admin_librarian_fixture.ctrl.db.library()
        data_source = DataSource.lookup(
            admin_librarian_fixture.ctrl.db.session, DataSource.LIBRARY_STAFF
        )
        list, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            data_source=data_source,
        )
        list.library = library
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            assert isinstance(list, CustomList)
            assert isinstance(list.name, str)
            form = ImmutableMultiDict(
                [
                    ("id", str(list.id)),
                    ("name", list.name),
                ]
            )
            add_request_context(flask.request, CustomListPostRequest, form=form)
            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert CANNOT_CHANGE_LIBRARY_FOR_CUSTOM_LIST == response

        list, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            data_source=data_source,
            library=admin_librarian_fixture.ctrl.db.default_library(),
        )
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            assert isinstance(list.name, str)
            form = ImmutableMultiDict(
                [
                    ("name", list.name),
                ]
            )
            add_request_context(flask.request, CustomListPostRequest, form=form)
            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert CUSTOM_LIST_NAME_ALREADY_IN_USE == response

        l1, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            data_source=data_source,
            library=admin_librarian_fixture.ctrl.db.default_library(),
        )
        l2, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            data_source=data_source,
            library=admin_librarian_fixture.ctrl.db.default_library(),
        )
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            assert isinstance(l1.name, str)
            form = ImmutableMultiDict(
                [
                    ("id", str(l2.id)),
                    ("name", l1.name),
                ]
            )
            add_request_context(flask.request, CustomListPostRequest, form=form)
            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert CUSTOM_LIST_NAME_ALREADY_IN_USE == response

        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            form = ImmutableMultiDict(
                [
                    ("name", "name"),
                    ("collections", json.dumps([12345])),
                ]
            )
            add_request_context(flask.request, CustomListPostRequest, form=form)
            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert MISSING_COLLECTION == response

        admin, ignore = create(
            admin_librarian_fixture.ctrl.db.session, Admin, email="test@nypl.org"
        )
        library = admin_librarian_fixture.ctrl.db.library()
        with admin_librarian_fixture.request_context_with_admin(
            "/", method="POST", admin=admin
        ) as ctx:
            setattr(ctx.request, "library", library)
            form = ImmutableMultiDict(
                [
                    ("name", "name"),
                    ("collections", json.dumps([])),
                ]
            )
            add_request_context(flask.request, CustomListPostRequest, form=form)
            pytest.raises(
                AdminNotAuthorized,
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists,
            )

    @pytest.mark.parametrize(
        "collection_status, expect_problem",
        [
            pytest.param("active", False, id="active collection"),
            pytest.param("inactive", True, id="associated, inactive collection"),
            pytest.param("unassociated", True, id="unassociated collection"),
        ],
    )
    def test_custom_lists_post_collection_with_wrong_library(
        self,
        admin_librarian_fixture: AdminLibrarianFixture,
        collection_status: Literal["active", "inactive", "unassociated"],
        expect_problem: bool,
    ):
        # Get a collection with the right relationship to the library.
        collection = (
            admin_librarian_fixture.ctrl.db.collection()
            if collection_status == "unassociated"
            else (
                admin_librarian_fixture.ctrl.db.default_inactive_collection()
                if collection_status == "inactive"
                else admin_librarian_fixture.ctrl.db.default_collection()
            )
        )

        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            form = ImmutableMultiDict(
                [
                    ("name", "name"),
                    ("collections", json.dumps([collection.id])),
                ]
            )
            add_request_context(flask.request, CustomListPostRequest, form=form)
            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )

        if expect_problem:
            assert response == COLLECTION_NOT_ACTIVE_FOR_LIST_LIBRARY
        else:
            assert not isinstance(response, ProblemDetail)
            assert response.status_code == 201

    def test_custom_lists_create(self, admin_librarian_fixture: AdminLibrarianFixture):
        work = admin_librarian_fixture.ctrl.db.work(with_open_access_download=True)
        collection = admin_librarian_fixture.ctrl.db.collection()
        collection.associated_libraries = [
            admin_librarian_fixture.ctrl.db.default_library()
        ]

        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            form = ImmutableMultiDict(
                [
                    ("name", "List"),
                    (
                        "entries",
                        json.dumps(
                            [dict(id=work.presentation_edition.primary_identifier.urn)]
                        ),
                    ),
                    ("collections", json.dumps([collection.id])),
                ]
            )
            add_request_context(flask.request, CustomListPostRequest, form=form)

            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert isinstance(response, flask.Response)
            assert 201 == response.status_code

            [list] = admin_librarian_fixture.ctrl.db.session.query(CustomList).all()
            assert list.id == int(response.get_data(as_text=True))
            assert admin_librarian_fixture.ctrl.db.default_library() == list.library
            assert "List" == list.name
            assert 1 == len(list.entries)
            assert work == list.entries[0].work
            assert work.presentation_edition == list.entries[0].edition
            assert True == list.entries[0].featured
            assert [collection] == list.collections
            assert False == list.auto_update_enabled

        # On an error of auto_update, rollbacks should occur
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            form = ImmutableMultiDict(
                [
                    ("name", "400List"),
                    (
                        "entries",
                        "[]",
                    ),
                    ("collections", "[]"),
                    ("auto_update", "True"),
                ]
            )
            add_request_context(flask.request, CustomListPostRequest, form=form)

            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert isinstance(response, ProblemDetail)
            assert 400 == response.status_code
            # List was not created
            assert None == get_one(
                admin_librarian_fixture.ctrl.db.session, CustomList, name="400List"
            )

        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            form = ImmutableMultiDict(
                [
                    ("name", "400List"),
                    (
                        "entries",
                        json.dumps(
                            [dict(id=work.presentation_edition.primary_identifier.urn)]
                        ),
                    ),
                    ("collections", json.dumps([collection.id])),
                    ("auto_update", "True"),
                    (
                        "auto_update_query",
                        json.dumps({"query": {"key": "title", "value": "A Title"}}),
                    ),
                    ("auto_update_facets", json.dumps({})),
                ]
            )
            add_request_context(flask.request, CustomListPostRequest, form=form)

            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert isinstance(response, ProblemDetail)
            assert response == AUTO_UPDATE_CUSTOM_LIST_CANNOT_HAVE_ENTRIES
            assert 400 == response.status_code

        # Valid auto update query request
        with (
            admin_librarian_fixture.request_context_with_library_and_admin(
                "/", method="POST"
            ),
            mock.patch(
                "palace.manager.api.admin.controller.custom_lists.CustomListQueries"
            ) as mock_query,
        ):
            form = ImmutableMultiDict(
                [
                    ("name", "200List"),
                    ("collections", json.dumps([collection.id])),
                    ("auto_update", "True"),
                    (
                        "auto_update_query",
                        json.dumps({"query": {"key": "title", "value": "A Title"}}),
                    ),
                    ("auto_update_facets", json.dumps({})),
                ]
            )
            add_request_context(flask.request, CustomListPostRequest, form=form)

            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert isinstance(response, flask.Response)
            assert 201 == response.status_code
            [list] = (
                admin_librarian_fixture.ctrl.db.session.query(CustomList)
                .filter(CustomList.name == "200List")
                .all()
            )
            assert True == list.auto_update_enabled
            assert (
                json.dumps({"query": {"key": "title", "value": "A Title"}})
                == list.auto_update_query
            )
            assert json.dumps({}) == list.auto_update_facets
            assert mock_query.populate_query_pages.call_count == 1

    def test_custom_list_get(self, admin_librarian_fixture: AdminLibrarianFixture):
        data_source = DataSource.lookup(
            admin_librarian_fixture.ctrl.db.session, DataSource.LIBRARY_STAFF
        )
        list, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            library=admin_librarian_fixture.ctrl.db.default_library(),
            data_source=data_source,
        )

        work1 = admin_librarian_fixture.ctrl.db.work(
            title="Second", with_license_pool=True
        )
        work2 = admin_librarian_fixture.ctrl.db.work(
            title="First", with_license_pool=True
        )
        list.add_entry(work1)
        list.add_entry(work2)

        with admin_librarian_fixture.request_context_with_library_and_admin("/"):
            assert isinstance(list.id, int)
            response = admin_librarian_fixture.manager.admin_custom_lists_controller.custom_list(
                list.id
            )
            assert isinstance(response, flask.Response)
            feed = feedparser.parse(response.get_data())

            assert list.name == feed.feed.title
            assert 2 == len(feed.entries)

            [self_custom_list_link] = [
                x["href"] for x in feed.feed["links"] if x["rel"] == "self"
            ]
            assert self_custom_list_link == feed.feed.id

            # The feed entries are sorted by title, so the first entry is work2, and the
            # second entry is work1.
            [entry1, entry2] = feed.entries
            assert work2.title == entry1.get("title")
            assert work1.title == entry2.get("title")

            assert work2.presentation_edition.author == entry1.get("author")
            assert work1.presentation_edition.author == entry2.get("author")

    def test_custom_list_get_with_pagination(
        self,
        admin_librarian_fixture: AdminLibrarianFixture,
    ):
        data_source = DataSource.lookup(
            admin_librarian_fixture.ctrl.db.session, DataSource.LIBRARY_STAFF
        )
        list, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            library=admin_librarian_fixture.ctrl.db.default_library(),
            data_source=data_source,
        )

        pagination_size = Pagination.DEFAULT_SIZE

        for i in range(pagination_size + 1):
            work = admin_librarian_fixture.ctrl.db.work(with_license_pool=True)
            list.add_entry(work)

        with (admin_librarian_fixture.request_context_with_library_and_admin("/"),):
            assert isinstance(list.id, int)
            response = admin_librarian_fixture.manager.admin_custom_lists_controller.custom_list(
                list.id
            )
            assert isinstance(response, flask.Response)
            feed = feedparser.parse(response.get_data())

            assert list.name == feed.feed.title

            [next_custom_list_link] = [
                x["href"] for x in feed.feed["links"] if x["rel"] == "next"
            ]

            # We remove the list_name argument of the url so we can add the after keyword and build the pagination link
            custom_list_url = feed.feed.id.rsplit("?", maxsplit=1)[0]
            next_page_url = f"{custom_list_url}?after={pagination_size}"

            assert next_custom_list_link == next_page_url

    def test_custom_list_get_errors(
        self, admin_librarian_fixture: AdminLibrarianFixture
    ):
        with admin_librarian_fixture.request_context_with_library_and_admin("/"):
            response = admin_librarian_fixture.manager.admin_custom_lists_controller.custom_list(
                123
            )
            assert MISSING_CUSTOM_LIST == response

        data_source = DataSource.lookup(
            admin_librarian_fixture.ctrl.db.session, DataSource.LIBRARY_STAFF
        )
        list, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            library=admin_librarian_fixture.ctrl.db.default_library(),
            data_source=data_source,
        )

        admin_librarian_fixture.admin.remove_role(
            AdminRole.LIBRARIAN, admin_librarian_fixture.ctrl.db.default_library()
        )
        with admin_librarian_fixture.request_context_with_library_and_admin("/"):
            pytest.raises(
                AdminNotAuthorized,
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_list,
                list.id,
            )

    def test_custom_list_edit(self, admin_librarian_fixture: AdminLibrarianFixture):
        data_source = DataSource.lookup(
            admin_librarian_fixture.ctrl.db.session, DataSource.LIBRARY_STAFF
        )
        list, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            data_source=data_source,
        )
        list.library = admin_librarian_fixture.ctrl.db.default_library()

        # Create a Lane that depends on this CustomList for its membership.
        lane = admin_librarian_fixture.ctrl.db.lane()
        lane.customlists.append(list)

        w1 = admin_librarian_fixture.ctrl.db.work(
            title="Alpha", with_license_pool=True, language="eng"
        )
        w2 = admin_librarian_fixture.ctrl.db.work(
            title="Bravo", with_license_pool=True, language="fre"
        )
        w3 = admin_librarian_fixture.ctrl.db.work(
            title="Charlie", with_license_pool=True
        )
        w2.presentation_edition.medium = Edition.AUDIO_MEDIUM
        w3.presentation_edition.permanent_work_id = (
            w2.presentation_edition.permanent_work_id
        )
        w3.presentation_edition.medium = Edition.BOOK_MEDIUM

        list.add_entry(w1)
        list.add_entry(w2)

        # All asserts in this test case depend on the external search being mocked
        assert isinstance(
            admin_librarian_fixture.ctrl.controller.search_engine,
            ExternalSearchIndexFake,
        )

        search_service: SearchServiceFake = (
            admin_librarian_fixture.ctrl.controller.search_engine.search_service()  # type: ignore [assignment]
        )
        external_search = admin_librarian_fixture.ctrl.controller.search_engine

        new_entries = [
            dict(
                id=work.presentation_edition.primary_identifier.urn,
                medium=Edition.medium_to_additional_type[
                    work.presentation_edition.medium
                ],
            )
            for work in [w2, w3]
        ]
        deletedEntries = [
            dict(
                id=work.presentation_edition.primary_identifier.urn,
                medium=Edition.medium_to_additional_type[
                    work.presentation_edition.medium
                ],
            )
            for work in [w1]
        ]

        c1 = admin_librarian_fixture.ctrl.db.collection()
        c1.associated_libraries = [admin_librarian_fixture.ctrl.db.default_library()]
        c2 = admin_librarian_fixture.ctrl.db.collection()
        c2.associated_libraries = [admin_librarian_fixture.ctrl.db.default_library()]
        list.collections = [c1]
        new_collections = [c2]

        # Test fails without expiring the ORM cache
        admin_librarian_fixture.ctrl.db.session.expire_all()

        # Mock the right count
        external_search.mock_count_works(2)

        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            form = ImmutableMultiDict(
                [
                    ("id", str(list.id)),
                    ("name", "new name"),
                    ("entries", json.dumps(new_entries)),
                    ("deletedEntries", json.dumps(deletedEntries)),
                    ("collections", json.dumps([c.id for c in new_collections])),
                ]
            )
            add_request_context(flask.request, CustomListPostRequest, form=form)

            assert isinstance(list.id, int)
            response = admin_librarian_fixture.manager.admin_custom_lists_controller.custom_list(
                list.id
            )
            assert isinstance(response, flask.Response)

        # Two works are indexed again
        assert len(search_service.documents_all()) == 2

        assert 200 == response.status_code
        assert list.id == int(response.get_data(as_text=True))

        assert "new name" == list.name
        assert {w2, w3} == {entry.work for entry in list.entries}
        assert new_collections == list.collections

        # Edit for auto update values
        update_query = {"query": {"key": "title", "value": "title"}}
        update_facets = {"order": "title"}
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            form = ImmutableMultiDict(
                [
                    ("id", str(list.id)),
                    ("name", "new name"),
                    ("collections", json.dumps([c.id for c in new_collections])),
                    ("auto_update", "true"),
                    ("auto_update_query", json.dumps(update_query)),
                    ("auto_update_facets", json.dumps(update_facets)),
                ]
            )
            add_request_context(flask.request, CustomListPostRequest, form=form)

            response = admin_librarian_fixture.manager.admin_custom_lists_controller.custom_list(
                list.id
            )

        assert True == list.auto_update_enabled
        assert json.dumps(update_query) == list.auto_update_query
        assert json.dumps(update_facets) == list.auto_update_facets

        admin_librarian_fixture.admin.remove_role(
            AdminRole.LIBRARIAN, admin_librarian_fixture.ctrl.db.default_library()
        )
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            form = ImmutableMultiDict(
                [
                    ("id", str(list.id)),
                    ("name", "another new name"),
                    ("entries", json.dumps(new_entries)),
                    ("collections", json.dumps([c.id for c in new_collections])),
                ]
            )
            add_request_context(flask.request, CustomListPostRequest, form=form)

            pytest.raises(
                AdminNotAuthorized,
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_list,
                list.id,
            )

    def test_custom_list_auto_update_cases(
        self, admin_librarian_fixture: AdminLibrarianFixture
    ):
        list, _ = admin_librarian_fixture.ctrl.db.customlist(
            data_source_name=DataSource.LIBRARY_STAFF,
        )
        list.library = admin_librarian_fixture.ctrl.db.default_library()

        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            form = ImmutableMultiDict(
                [
                    ("id", str(list.id)),
                    ("name", "new name"),
                    ("entries", "[]"),
                    ("deletedEntries", "[]"),
                    ("collections", "[]"),
                    ("auto_update", "true"),
                    ("auto_update_query", None),
                ]
            )
            add_request_context(flask.request, CustomListPostRequest, form=form)

            response = admin_librarian_fixture.manager.admin_custom_lists_controller.custom_list(
                list.id
            )
            assert type(response) == ProblemDetail
            assert response.status_code == 400
            assert (
                response.detail
                == "auto_update_query must be present when auto_update is enabled"
            )

    def test_custom_list_delete_success(
        self, admin_librarian_fixture: AdminLibrarianFixture
    ):
        admin_librarian_fixture.admin.add_role(
            AdminRole.LIBRARY_MANAGER, admin_librarian_fixture.ctrl.db.default_library()
        )

        # Create a CustomList with two Works on it.
        library_staff = DataSource.lookup(
            admin_librarian_fixture.ctrl.db.session, DataSource.LIBRARY_STAFF
        )
        list, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            data_source=library_staff,
        )
        list.library = admin_librarian_fixture.ctrl.db.default_library()

        w1 = admin_librarian_fixture.ctrl.db.work(with_license_pool=True)
        w2 = admin_librarian_fixture.ctrl.db.work(with_license_pool=True)
        list.add_entry(w1)
        list.add_entry(w2)

        # Create a second CustomList, from another data source,
        # containing a single work.
        nyt = DataSource.lookup(admin_librarian_fixture.ctrl.db.session, DataSource.NYT)
        list2, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            data_source=nyt,
        )
        list2.library = admin_librarian_fixture.ctrl.db.default_library()
        list2.add_entry(w2)

        # Create a Lane which takes all of its contents from that
        # CustomList. When the CustomList is deleted, the Lane will
        # have no reason to exist, and it will be automatically
        # deleted as well.
        lane = admin_librarian_fixture.ctrl.db.lane(
            display_name="to be automatically removed"
        )
        lane.customlists.append(list)

        # This Lane is based on two different CustomLists. Its size
        # will be updated when the CustomList is deleted, but the Lane
        # itself will not be deleted, since it's still based on
        # something.
        lane2 = admin_librarian_fixture.ctrl.db.lane(
            display_name="to have size updated"
        )
        lane2.customlists.append(list)
        lane2.customlists.append(list2)

        # This lane is based on _all_ lists from a given data source.
        # It will also not be deleted when the CustomList is deleted,
        # because other lists from that data source might show up in
        # the future.
        lane3 = admin_librarian_fixture.ctrl.db.lane(
            display_name="All library staff lists"
        )
        lane3.list_datasource = list.data_source

        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="DELETE"
        ):
            assert isinstance(list.id, int)
            response = admin_librarian_fixture.manager.admin_custom_lists_controller.custom_list(
                list.id
            )
            assert isinstance(response, flask.Response)
            assert 200 == response.status_code

        # The first CustomList and all of its entries have been removed.
        # Only the second one remains.
        assert [list2] == admin_librarian_fixture.ctrl.db.session.query(
            CustomList
        ).all()
        assert (
            list2.entries
            == admin_librarian_fixture.ctrl.db.session.query(CustomListEntry).all()
        )

        # The first lane was automatically removed when it became
        # based on an empty set of CustomLists.
        assert None == get_one(
            admin_librarian_fixture.ctrl.db.session, Lane, id=lane.id
        )

    def test_custom_list_delete_errors(
        self, admin_librarian_fixture: AdminLibrarianFixture
    ):
        data_source = DataSource.lookup(
            admin_librarian_fixture.ctrl.db.session, DataSource.LIBRARY_STAFF
        )
        list, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            data_source=data_source,
        )
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="DELETE"
        ):
            pytest.raises(
                AdminNotAuthorized,
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_list,
                list.id,
            )

        admin_librarian_fixture.admin.add_role(
            AdminRole.LIBRARY_MANAGER, admin_librarian_fixture.ctrl.db.default_library()
        )
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="DELETE"
        ):
            response = admin_librarian_fixture.manager.admin_custom_lists_controller.custom_list(
                123
            )
            assert MISSING_CUSTOM_LIST == response

        library = admin_librarian_fixture.ctrl.db.library()
        admin_librarian_fixture.admin.add_role(AdminRole.LIBRARY_MANAGER, library)
        list.shared_locally = True
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="DELETE"
        ):
            assert isinstance(list.id, int)
            response = admin_librarian_fixture.manager.admin_custom_lists_controller.custom_list(
                list.id
            )
            assert response == CANNOT_DELETE_SHARED_LIST

    @define
    class ShareLocallySetup:
        shared_with: Library | None = None
        primary_library: Library | None = None
        collection1: Collection | None = None
        list: CustomList | None = None

    def _setup_share_locally(self, admin_librarian_fixture: AdminLibrarianFixture):
        shared_with = admin_librarian_fixture.ctrl.db.library("shared_with")
        primary_library = admin_librarian_fixture.ctrl.db.library("primary")
        collection1 = admin_librarian_fixture.ctrl.db.collection("c1")
        collection1.associated_libraries.append(primary_library)

        data_source = DataSource.lookup(
            admin_librarian_fixture.ctrl.db.session, DataSource.LIBRARY_STAFF
        )
        list, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            data_source=data_source,
            library=primary_library,
            collections=[collection1],
        )

        return self.ShareLocallySetup(
            shared_with=shared_with,
            primary_library=primary_library,
            collection1=collection1,
            list=list,
        )

    def _share_locally(
        self, customlist, library, admin_librarian_fixture: AdminLibrarianFixture
    ):
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", library=library, method="POST"
        ):
            response = admin_librarian_fixture.manager.admin_custom_lists_controller.share_locally(
                customlist.id
            )
        return response

    def test_share_locally_success(
        self, admin_librarian_fixture: AdminLibrarianFixture
    ):
        s = self._setup_share_locally(admin_librarian_fixture)
        response = self._share_locally(
            s.list, s.primary_library, admin_librarian_fixture
        )

        # The list is shared with every other library on this Palace Manager:
        # the one built by the fixture, plus the default library.
        assert response["successes"] == 2
        assert response["failures"] == 0

        admin_librarian_fixture.ctrl.db.session.refresh(s.list)
        assert s.list.shared_locally is True

        # Sharing again is not an error and does not change anything.
        response = self._share_locally(
            s.list, s.primary_library, admin_librarian_fixture
        )
        assert response["successes"] == 2
        assert response["failures"] == 0
        assert s.list.shared_locally is True

    def test_share_locally_without_licensed_works(
        self, admin_librarian_fixture: AdminLibrarianFixture
    ):
        # A library that does not have the list's collection, and so cannot
        # license its works, is still shared with. Sharing is all or nothing;
        # the library's own lanes and feeds stay scoped to its collections.
        s = self._setup_share_locally(admin_librarian_fixture)
        w = admin_librarian_fixture.ctrl.db.work(collection=s.collection1)
        s.list.add_entry(w)
        assert s.collection1 not in s.shared_with.active_collections

        response = self._share_locally(
            s.list, s.primary_library, admin_librarian_fixture
        )

        assert response["successes"] == 2
        assert response["failures"] == 0
        assert s.list.shared_locally is True

    def test_share_locally_includes_libraries_created_later(
        self, admin_librarian_fixture: AdminLibrarianFixture
    ):
        """A library added after a list is shared can use it, with no re-share."""
        s = self._setup_share_locally(admin_librarian_fixture)

        unshared, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            data_source=s.list.data_source,
            library=s.primary_library,
        )

        self._share_locally(s.list, s.primary_library, admin_librarian_fixture)

        # Only now does the library come into existence.
        latecomer = admin_librarian_fixture.ctrl.db.library("latecomer")
        admin_librarian_fixture.admin.add_role(AdminRole.LIBRARIAN, latecomer)

        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="GET", library=latecomer
        ):
            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )

        assert isinstance(response, dict)
        listed = {list["id"]: list for list in response["custom_lists"]}

        # The shared list is available, and behaves like any other shared list.
        assert s.list.id in listed
        assert listed[s.list.id]["is_owner"] is False
        assert listed[s.list.id]["is_shared"] is True

        # The list that was never shared remains unavailable.
        assert unshared.id not in listed

    def test_share_locally_get(self, admin_librarian_fixture: AdminLibrarianFixture):
        """Does the GET method fetch shared lists"""
        s = self._setup_share_locally(admin_librarian_fixture)

        self._share_locally(s.list, s.primary_library, admin_librarian_fixture)

        admin_librarian_fixture.admin.add_role(AdminRole.LIBRARIAN, s.shared_with)
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="GET", library=s.shared_with
        ):
            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert isinstance(response, dict)

            assert len(response["custom_lists"]) == 1
            collections = [
                dict(id=c.id, name=c.name, protocol=c.protocol)
                for c in s.list.collections
            ]
            assert response["custom_lists"][0] == dict(
                id=s.list.id,
                name=s.list.name,
                collections=collections,
                entry_count=s.list.size,
                auto_update=False,
                auto_update_query=None,
                auto_update_facets=None,
                auto_update_status=CustomList.INIT,
                is_owner=False,
                is_shared=True,
            )

    def test_share_locally_delete(self, admin_librarian_fixture: AdminLibrarianFixture):
        """Test the deleting of a lists shared status"""
        s = self._setup_share_locally(admin_librarian_fixture)

        self._share_locally(s.list, s.primary_library, admin_librarian_fixture)

        # First, we are shared with a library which uses the list
        # so we cannot delete the share status
        lane_with_shared = admin_librarian_fixture.ctrl.db.lane(library=s.shared_with)
        lane_with_shared.customlists = [s.list]

        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="DELETE", library=s.primary_library
        ):
            response = admin_librarian_fixture.manager.admin_custom_lists_controller.share_locally(
                s.list.id
            )
            assert type(response) == ProblemDetail

        # Second, we remove the lane that uses the shared list_
        # making it available to unshare
        admin_librarian_fixture.ctrl.db.session.delete(lane_with_shared)
        admin_librarian_fixture.ctrl.db.session.commit()

        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="DELETE", library=s.primary_library
        ):
            response = admin_librarian_fixture.manager.admin_custom_lists_controller.share_locally(
                s.list.id
            )
            assert isinstance(response, flask.Response)
            assert response.status_code == 204

        assert s.list.shared_locally is False

        # Third, it is in use by the owner library (not the shared library)
        # so the list can still be unshared
        self._share_locally(s.list, s.primary_library, admin_librarian_fixture)

        lane_with_primary = admin_librarian_fixture.ctrl.db.lane(
            library=s.primary_library,
        )
        lane_with_primary.customlists = [s.list]
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="DELETE", library=s.primary_library
        ):
            response = admin_librarian_fixture.manager.admin_custom_lists_controller.share_locally(
                s.list.id
            )
            assert isinstance(response, flask.Response)
            assert response.status_code == 204

        assert s.list.shared_locally is False

    def test_auto_update_edit(self, admin_librarian_fixture: AdminLibrarianFixture):
        w1 = admin_librarian_fixture.ctrl.db.work()
        custom_list: CustomList
        custom_list, _ = admin_librarian_fixture.ctrl.db.customlist(
            data_source_name=DataSource.LIBRARY_STAFF, num_entries=0
        )
        custom_list.library = admin_librarian_fixture.ctrl.db.default_library()
        custom_list.add_entry(w1)
        custom_list.auto_update_enabled = True
        custom_list.auto_update_query = json.dumps(
            {"query": {"key": "title", "value": "old"}}
        )
        custom_list.auto_update_status = CustomList.UPDATED
        admin_librarian_fixture.ctrl.db.session.commit()

        assert isinstance(custom_list.name, str)
        assert custom_list.library is not None
        changed_query = {"query": {"key": "title", "value": "changed"}}
        response = admin_librarian_fixture.manager.admin_custom_lists_controller._create_or_update_list(
            custom_list.library,
            custom_list.name,
            [],
            [],
            [],
            id=custom_list.id,
            auto_update=True,
            auto_update_query=changed_query,
        )

        assert response.status_code == 200
        assert custom_list.auto_update_query == json.dumps(changed_query)
        assert custom_list.auto_update_status == CustomList.REPOPULATE
        assert [e.work_id for e in custom_list.entries] == [w1.id]

    def test_auto_update_create_unable_to_serialize_query(
        self,
        admin_librarian_fixture: AdminLibrarianFixture,
        db: DatabaseTransactionFixture,
    ):
        library = db.default_library()
        response = admin_librarian_fixture.manager.admin_custom_lists_controller._create_or_update_list(
            library,
            "test list",
            [],
            [],
            [],
            id=None,
            auto_update=True,
            # A bare ``object()`` is deliberately not JSON serializable — the
            # point of this test is the runtime ``json.dumps`` guard, so the
            # value intentionally violates the JSONQueryDict value type.
            auto_update_query={"foo": object()},  # type: ignore[dict-item]
        )

        assert isinstance(response, ProblemDetail)
        assert response.status_code == 400
        assert response.detail == "auto_update_query is not JSON serializable"

    @pytest.mark.parametrize(
        "query,expected_reason",
        [
            pytest.param(
                {"query": {"key": "published", "value": "2025>01>01"}},
                "Could not parse 'published' value '2025>01>01'. Only use 'YYYY-MM-DD'",
                id="unparseable-published-value",
            ),
            pytest.param(
                {"query": {"key": "not_a_field", "value": "x"}},
                "Unrecognized key: not_a_field",
                id="unknown-key",
            ),
            pytest.param(
                {"query": {"key": "title", "value": "x", "op": "sideways"}},
                "Unrecognized operator: sideways",
                id="unknown-operator",
            ),
            pytest.param(
                {"not_query": {"key": "title", "value": "x"}},
                "'query' key must be present as the root",
                id="missing-query-root",
            ),
            # JSON-serializable but wrongly-typed inputs. These once reached the
            # parser's string operations and raised AttributeError/TypeError,
            # which escaped the QueryParseException catch as a 500. They must now
            # come back as a clean 400 like any other invalid query.
            pytest.param(
                {"query": {"key": "language", "value": 5}},
                "Value for 'language' must be a string",
                id="non-string-value",
            ),
            pytest.param(
                {"query": {"key": ["title"], "value": "x"}},
                "Query 'key' must be a string",
                id="non-string-key",
            ),
            pytest.param(
                {"query": "just a string"},
                "Each query part must be an object",
                id="query-part-not-object",
            ),
        ],
    )
    def test_auto_update_query_must_be_a_valid_search_query(
        self,
        query: dict[str, Any],
        expected_reason: str,
        admin_librarian_fixture: AdminLibrarianFixture,
        db: DatabaseTransactionFixture,
    ):
        """A JSON-serializable query that the search layer cannot parse is rejected.

        Storing one would produce a list that silently stops updating: the
        entry-update task can only log and skip a list whose query fails to parse.
        """
        library = db.default_library()
        response = admin_librarian_fixture.manager.admin_custom_lists_controller._create_or_update_list(
            library,
            "test list",
            [],
            [],
            [],
            id=None,
            auto_update=True,
            auto_update_query=query,
        )

        assert isinstance(response, ProblemDetail)
        assert response.status_code == 400
        assert response.detail is not None
        assert "auto_update_query is not a valid search query" in response.detail
        assert expected_reason in response.detail

        # Nothing was persisted.
        assert CustomList.find(db.session, "test list", library=library) is None

    def test_auto_update_query_validated_on_edit(
        self,
        admin_librarian_fixture: AdminLibrarianFixture,
    ):
        """An existing list's query is validated too, not just a new list's.

        Edits were the gap: creating a list incidentally exercised its query through
        populate_query_pages, but an edit never parsed the query at all.
        """
        custom_list: CustomList
        custom_list, _ = admin_librarian_fixture.ctrl.db.customlist(
            data_source_name=DataSource.LIBRARY_STAFF, num_entries=0
        )
        custom_list.library = admin_librarian_fixture.ctrl.db.default_library()
        custom_list.auto_update_enabled = True
        good_query = json.dumps({"query": {"key": "title", "value": "good"}})
        custom_list.auto_update_query = good_query
        custom_list.auto_update_status = CustomList.UPDATED
        admin_librarian_fixture.ctrl.db.session.commit()

        assert isinstance(custom_list.name, str)
        assert custom_list.library is not None
        response = admin_librarian_fixture.manager.admin_custom_lists_controller._create_or_update_list(
            custom_list.library,
            custom_list.name,
            [],
            [],
            [],
            id=custom_list.id,
            auto_update=True,
            auto_update_query={"query": {"key": "published", "value": "2025>01>01"}},
        )

        assert isinstance(response, ProblemDetail)
        assert response.status_code == 400
        # The previously-good query is left untouched.
        assert custom_list.auto_update_query == good_query
        assert custom_list.auto_update_status == CustomList.UPDATED

    def test_auto_update_create_unable_to_serialize_facets(
        self,
        admin_librarian_fixture: AdminLibrarianFixture,
        db: DatabaseTransactionFixture,
    ):
        library = db.default_library()
        response = admin_librarian_fixture.manager.admin_custom_lists_controller._create_or_update_list(
            library,
            "test list",
            [],
            [],
            [],
            id=None,
            auto_update=True,
            auto_update_query={"query": {"key": "title", "value": "foo"}},
            auto_update_facets={"foo": object()},  # type: ignore[dict-item]
        )

        assert isinstance(response, ProblemDetail)
        assert response.status_code == 400
        assert response.detail == "auto_update_facets is not JSON serializable"

    def test_auto_update_deleted_entries(
        self,
        admin_librarian_fixture: AdminLibrarianFixture,
        db: DatabaseTransactionFixture,
    ):
        library = db.default_library()
        response = admin_librarian_fixture.manager.admin_custom_lists_controller._create_or_update_list(
            library,
            "test list",
            [],
            [],
            [{}, {}],
            id=None,
            auto_update=True,
            auto_update_query={"query": {"key": "title", "value": "foo"}},
        )
        assert response == AUTO_UPDATE_CUSTOM_LIST_CANNOT_HAVE_ENTRIES
