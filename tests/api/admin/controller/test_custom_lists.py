import json
from typing import Optional
from unittest import mock

import feedparser
import flask
import pytest
from attr import define
from werkzeug.datastructures import ImmutableMultiDict

from api.admin.controller.custom_lists import CustomListsController
from api.admin.exceptions import AdminNotAuthorized
from api.admin.problem_details import (
    AUTO_UPDATE_CUSTOM_LIST_CANNOT_HAVE_ENTRIES,
    CANNOT_CHANGE_LIBRARY_FOR_CUSTOM_LIST,
    COLLECTION_NOT_ASSOCIATED_WITH_LIBRARY,
    CUSTOM_LIST_NAME_ALREADY_IN_USE,
    MISSING_COLLECTION,
    MISSING_CUSTOM_LIST,
)
from api.problem_details import CANNOT_DELETE_SHARED_LIST
from core.lane import Lane, Pagination
from core.model import (
    Admin,
    AdminRole,
    Collection,
    CustomList,
    CustomListEntry,
    DataSource,
    Edition,
    Library,
    create,
    get_one,
)
from core.query.customlist import CustomListQueries
from core.util.problem_detail import ProblemDetail
from tests.core.util.test_flask_util import add_request_context
from tests.fixtures.api_admin import AdminLibrarianFixture
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
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
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
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            library=admin_librarian_fixture.ctrl.db.default_library(),
            auto_update_enabled=False,
        )

        # This will set the is_shared attribute
        shared_library = admin_librarian_fixture.ctrl.db.library()
        assert (
            CustomListQueries.share_locally_with_library(
                admin_librarian_fixture.ctrl.db.session, no_entries, shared_library
            )
            == True
        )

        with admin_librarian_fixture.request_context_with_library_and_admin("/"):
            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert isinstance(response, dict)

            lists = response.get("custom_lists")
            assert isinstance(lists, list)
            assert 2 == len(lists)

            [l1, l2] = sorted(lists, key=lambda l: l.get("id"))

            assert one_entry.id == l1.get("id")
            assert one_entry.name == l1.get("name")
            assert 1 == l1.get("entry_count")
            assert 1 == len(l1.get("collections"))
            [c] = l1.get("collections")
            assert collection.name == c.get("name")
            assert collection.id == c.get("id")
            assert collection.protocol == c.get("protocol")
            assert True == l1.get("auto_update")
            assert auto_update_query == l1.get("auto_update_query")
            assert CustomList.INIT == l1.get("auto_update_status")
            assert False == l1.get("is_shared")
            assert True == l1.get("is_owner")

            assert no_entries.id == l2.get("id")
            assert no_entries.name == l2.get("name")
            assert 0 == l2.get("entry_count")
            assert 0 == len(l2.get("collections"))
            assert False == l2.get("auto_update")
            assert None == l2.get("auto_update_query")
            assert CustomList.INIT == l2.get("auto_update_status")
            assert True == l2.get("is_shared")
            assert True == l2.get("is_owner")

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
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )
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
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )
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
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )
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
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )
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
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )
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
        ):
            flask.request.library = library  # type: ignore[attr-defined]
            form = ImmutableMultiDict(
                [
                    ("name", "name"),
                    ("collections", json.dumps([])),
                ]
            )
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )
            pytest.raises(
                AdminNotAuthorized,
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists,
            )

    def test_custom_lists_post_collection_with_wrong_library(
        self, admin_librarian_fixture: AdminLibrarianFixture
    ):
        # This collection is not associated with any libraries.
        collection = admin_librarian_fixture.ctrl.db.collection()
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            form = ImmutableMultiDict(
                [
                    ("name", "name"),
                    ("collections", json.dumps([collection.id])),
                ]
            )
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )
            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert COLLECTION_NOT_ASSOCIATED_WITH_LIBRARY == response

    def test_custom_lists_create(self, admin_librarian_fixture: AdminLibrarianFixture):
        work = admin_librarian_fixture.ctrl.db.work(with_open_access_download=True)
        collection = admin_librarian_fixture.ctrl.db.collection()
        collection.libraries = [admin_librarian_fixture.ctrl.db.default_library()]

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
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )

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
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )

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
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )

            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert isinstance(response, ProblemDetail)
            assert response == AUTO_UPDATE_CUSTOM_LIST_CANNOT_HAVE_ENTRIES
            assert 400 == response.status_code

        # Valid auto update query request
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ), mock.patch(
            "api.admin.controller.custom_lists.CustomListQueries"
        ) as mock_query:
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
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )

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

        work1 = admin_librarian_fixture.ctrl.db.work(with_license_pool=True)
        work2 = admin_librarian_fixture.ctrl.db.work(with_license_pool=True)
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

            [entry1, entry2] = feed.entries
            assert work1.title == entry1.get("title")
            assert work2.title == entry2.get("title")

            assert work1.presentation_edition.author == entry1.get("author")
            assert work2.presentation_edition.author == entry2.get("author")

    def test_custom_list_get_with_pagination(
        self, admin_librarian_fixture: AdminLibrarianFixture
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

        with admin_librarian_fixture.request_context_with_library_and_admin("/"):
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
        lane.size = 350

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
        c1.libraries = [admin_librarian_fixture.ctrl.db.default_library()]
        c2 = admin_librarian_fixture.ctrl.db.collection()
        c2.libraries = [admin_librarian_fixture.ctrl.db.default_library()]
        list.collections = [c1]
        new_collections = [c2]

        # The lane size is set to a static value above. After this call it should
        # be reset to a value that reflects the number of documents in the search_engine,
        # regardless of filter, since that's what the mock search engine's count_works does.
        assert lane.size == 350

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
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )

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

        assert lane.size == 2

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
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )

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
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )

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
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )

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

        # Whenever the mocked search engine is asked how many
        # works are in a Lane, it will say there are two.
        admin_librarian_fixture.ctrl.controller.search_engine.docs = dict(
            id1="doc1", id2="doc2"
        )

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
        lane2.size = 100

        # This lane is based on _all_ lists from a given data source.
        # It will also not be deleted when the CustomList is deleted,
        # because other lists from that data source might show up in
        # the future.
        lane3 = admin_librarian_fixture.ctrl.db.lane(
            display_name="All library staff lists"
        )
        lane3.list_datasource = list.data_source
        lane3.size = 150

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
        CustomListQueries.share_locally_with_library(
            admin_librarian_fixture.ctrl.db.session, list, library
        )
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
        shared_with: Optional[Library] = None
        primary_library: Optional[Library] = None
        collection1: Optional[Collection] = None
        list: Optional[CustomList] = None

    def _setup_share_locally(self, admin_librarian_fixture: AdminLibrarianFixture):
        shared_with = admin_librarian_fixture.ctrl.db.library("shared_with")
        primary_library = admin_librarian_fixture.ctrl.db.library("primary")
        collection1 = admin_librarian_fixture.ctrl.db.collection("c1")
        primary_library.collections.append(collection1)

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

    def test_share_locally_missing_collection(
        self, admin_librarian_fixture: AdminLibrarianFixture
    ):
        s = self._setup_share_locally(admin_librarian_fixture)
        response = self._share_locally(
            s.list, s.primary_library, admin_librarian_fixture
        )
        assert response["failures"] == 2
        assert response["successes"] == 0

    def test_share_locally_success(
        self, admin_librarian_fixture: AdminLibrarianFixture
    ):
        s = self._setup_share_locally(admin_librarian_fixture)
        s.shared_with.collections.append(s.collection1)
        response = self._share_locally(
            s.list, s.primary_library, admin_librarian_fixture
        )
        assert response["successes"] == 1
        assert response["failures"] == 1  # The default library

        admin_librarian_fixture.ctrl.db.session.refresh(s.list)
        assert len(s.list.shared_locally_with_libraries) == 1

        # Try again should have 0 more libraries as successes
        response = self._share_locally(
            s.list, s.primary_library, admin_librarian_fixture
        )
        assert response["successes"] == 0
        assert response["failures"] == 1  # The default library

    def test_share_locally_with_invalid_entries(
        self, admin_librarian_fixture: AdminLibrarianFixture
    ):
        s = self._setup_share_locally(admin_librarian_fixture)
        s.shared_with.collections.append(s.collection1)

        # Second collection with work in list
        collection2 = admin_librarian_fixture.ctrl.db.collection()
        s.primary_library.collections.append(collection2)
        w = admin_librarian_fixture.ctrl.db.work(collection=collection2)
        s.list.add_entry(w)

        response = self._share_locally(
            s.list, s.primary_library, admin_librarian_fixture
        )
        assert response["failures"] == 2
        assert response["successes"] == 0

    def test_share_locally_get(self, admin_librarian_fixture: AdminLibrarianFixture):
        """Does the GET method fetch shared lists"""
        s = self._setup_share_locally(admin_librarian_fixture)
        s.shared_with.collections.append(s.collection1)

        resp = self._share_locally(s.list, s.primary_library, admin_librarian_fixture)
        assert resp["successes"] == 1

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
        s.shared_with.collections.append(s.collection1)

        resp = self._share_locally(s.list, s.primary_library, admin_librarian_fixture)
        assert resp["successes"] == 1

        # First, we are shared with a library which uses the list
        # so we cannot delete the share status
        lane_with_shared, _ = create(
            admin_librarian_fixture.ctrl.db.session,
            Lane,
            library_id=s.shared_with.id,
            customlists=[s.list],
        )

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

        assert s.list.shared_locally_with_libraries == []

        # Third, it is in use by the owner library (not the shared library)
        # so the list can still be unshared
        resp = self._share_locally(s.list, s.primary_library, admin_librarian_fixture)
        assert resp["successes"] == 1

        lane_with_primary, _ = create(
            admin_librarian_fixture.ctrl.db.session,
            Lane,
            library_id=s.primary_library.id,
            customlists=[s.list],
        )
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="DELETE", library=s.primary_library
        ):
            response = admin_librarian_fixture.manager.admin_custom_lists_controller.share_locally(
                s.list.id
            )
            assert isinstance(response, flask.Response)
            assert response.status_code == 204

        assert s.list.shared_locally_with_libraries == []

    def test_auto_update_edit(self, admin_librarian_fixture: AdminLibrarianFixture):
        w1 = admin_librarian_fixture.ctrl.db.work()
        custom_list: CustomList
        custom_list, _ = admin_librarian_fixture.ctrl.db.customlist(
            data_source_name=DataSource.LIBRARY_STAFF, num_entries=0
        )
        custom_list.add_entry(w1)
        custom_list.auto_update_enabled = True
        custom_list.auto_update_query = '{"query":"...."}'
        custom_list.auto_update_status = CustomList.UPDATED
        admin_librarian_fixture.ctrl.db.session.commit()

        assert isinstance(custom_list.name, str)
        response = admin_librarian_fixture.manager.admin_custom_lists_controller._create_or_update_list(
            custom_list.library,
            custom_list.name,
            [],
            [],
            [],
            id=custom_list.id,
            auto_update=True,
            auto_update_query={"query": "...changed"},
        )

        assert response.status_code == 200
        assert custom_list.auto_update_query == '{"query": "...changed"}'
        assert custom_list.auto_update_status == CustomList.REPOPULATE
        assert [e.work_id for e in custom_list.entries] == [w1.id]
