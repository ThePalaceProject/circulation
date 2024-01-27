import json
from unittest.mock import MagicMock, create_autospec

import flask
import pytest
from _pytest.monkeypatch import MonkeyPatch
from flask import Response
from werkzeug.datastructures import ImmutableMultiDict

from api.admin.controller.collection_settings import CollectionSettingsController
from api.admin.exceptions import AdminNotAuthorized
from api.admin.problem_details import (
    CANNOT_CHANGE_PROTOCOL,
    CANNOT_DELETE_COLLECTION_WITH_CHILDREN,
    FAILED_TO_RUN_SELF_TESTS,
    INCOMPLETE_CONFIGURATION,
    INTEGRATION_NAME_ALREADY_IN_USE,
    MISSING_IDENTIFIER,
    MISSING_PARENT,
    MISSING_SERVICE,
    MISSING_SERVICE_NAME,
    NO_PROTOCOL_FOR_NEW_SERVICE,
    NO_SUCH_LIBRARY,
    PROTOCOL_DOES_NOT_SUPPORT_PARENTS,
    UNKNOWN_PROTOCOL,
)
from api.integration.registry.license_providers import LicenseProvidersRegistry
from api.selftest import HasCollectionSelfTests
from core.model import AdminRole, Collection, ExternalIntegration, get_one
from core.selftest import HasSelfTests
from core.util.problem_detail import ProblemDetail, ProblemError
from tests.api.mockapi.axis import MockAxis360API
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.flask import FlaskAppFixture


@pytest.fixture
def controller(db: DatabaseTransactionFixture) -> CollectionSettingsController:
    mock_manager = MagicMock()
    mock_manager._db = db.session
    return CollectionSettingsController(mock_manager)


class TestCollectionSettings:
    def test_process_collections(
        self,
        controller: CollectionSettingsController,
        flask_app_fixture: FlaskAppFixture,
    ):
        # Make sure when we call process_collections with a get request that
        # we call process_get and when we call it with a post request that
        # we call process_post.

        mock_process_get = create_autospec(
            controller.process_get, return_value="get_response"
        )
        controller.process_get = mock_process_get

        mock_process_post = create_autospec(
            controller.process_post, return_value="post_response"
        )
        controller.process_post = mock_process_post

        with flask_app_fixture.test_request_context("/"):
            response = controller.process_collections()
            assert response == "get_response"

        assert mock_process_get.call_count == 1
        assert mock_process_post.call_count == 0

        mock_process_get.reset_mock()
        mock_process_post.reset_mock()

        with flask_app_fixture.test_request_context("/", method="POST"):
            response = controller.process_collections()
            assert response == "post_response"

        assert mock_process_get.call_count == 0
        assert mock_process_post.call_count == 1

    def test_collections_get_with_no_collections(
        self, controller: CollectionSettingsController, db: DatabaseTransactionFixture
    ) -> None:
        # Delete any existing collections created by the test setup.
        db.session.delete(db.default_collection())

        response = controller.process_get()
        assert isinstance(response, Response)
        assert response.status_code == 200
        data = response.json
        assert isinstance(data, dict)
        assert data.get("collections") == []

        names = {p.get("name") for p in data.get("protocols", {})}
        expected_names = {k for k, v in LicenseProvidersRegistry()}
        assert names == expected_names

    def test_collections_get_collections_with_multiple_collections(
        self,
        controller: CollectionSettingsController,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ) -> None:
        [c1] = db.default_library().collections

        c2 = db.collection(
            name="Collection 2",
            protocol=ExternalIntegration.OVERDRIVE,
            external_account_id="1234",
            settings=dict(
                overdrive_client_secret="b",
                overdrive_client_key="user",
                overdrive_website_id="100",
            ),
        )

        c3 = db.collection(
            name="Collection 3",
            protocol=ExternalIntegration.OVERDRIVE,
            external_account_id="5678",
        )
        c3.parent = c2

        l1 = db.library(short_name="L1")
        c3.libraries += [l1, db.default_library()]
        assert isinstance(l1.id, int)
        l1_config = c3.integration_configuration.for_library(l1.id)
        assert l1_config is not None
        DatabaseTransactionFixture.set_settings(l1_config, ebook_loan_duration="14")

        admin = flask_app_fixture.admin_user()
        l1_librarian = flask_app_fixture.admin_user(
            email="admin@l1.org", role=AdminRole.LIBRARIAN, library=l1
        )

        with flask_app_fixture.test_request_context("/", admin=admin):
            response1 = controller.process_get()
        assert isinstance(response1, Response)
        assert response1.status_code == 200
        data = response1.json
        assert isinstance(data, dict)
        # The system admin can see all collections.
        coll2, coll3, coll1 = sorted(
            data.get("collections", []), key=lambda c: c.get("name", "")
        )
        assert c1.integration_configuration.id == coll1.get("id")
        assert c2.integration_configuration.id == coll2.get("id")
        assert c3.integration_configuration.id == coll3.get("id")

        assert c1.name == coll1.get("name")
        assert c2.name == coll2.get("name")
        assert c3.name == coll3.get("name")

        assert c1.protocol == coll1.get("protocol")
        assert c2.protocol == coll2.get("protocol")
        assert c3.protocol == coll3.get("protocol")

        settings1 = coll1.get("settings", {})
        settings2 = coll2.get("settings", {})
        settings3 = coll3.get("settings", {})

        assert settings1.get("external_account_id") == "http://opds.example.com/feed"
        assert settings2.get("external_account_id") == "1234"
        assert settings3.get("external_account_id") == "5678"

        assert c2.integration_configuration.settings_dict[
            "overdrive_client_secret"
        ] == settings2.get("overdrive_client_secret")

        assert c2.integration_configuration.id == coll3.get("parent_id")

        coll3_libraries = coll3.get("libraries")
        assert 2 == len(coll3_libraries)
        coll3_l1, coll3_default = sorted(
            coll3_libraries, key=lambda x: x.get("short_name")
        )
        assert "L1" == coll3_l1.get("short_name")
        assert "14" == coll3_l1.get("ebook_loan_duration")
        assert db.default_library().short_name == coll3_default.get("short_name")

        with flask_app_fixture.test_request_context("/", admin=l1_librarian):
            # A librarian only sees collections associated with their library.
            response2 = controller.process_collections()
        assert isinstance(response2, Response)
        assert response2.status_code == 200
        data = response2.json
        assert isinstance(data, dict)
        [coll3] = data.get("collections", [])
        assert c3.integration_configuration.id == coll3.get("id")

        coll3_libraries = coll3.get("libraries")
        assert 1 == len(coll3_libraries)
        assert "L1" == coll3_libraries[0].get("short_name")
        assert "14" == coll3_libraries[0].get("ebook_loan_duration")

    @pytest.mark.parametrize(
        "post_data,expected,detailed",
        [
            pytest.param(
                {"protocol": "Overdrive"},
                MISSING_SERVICE_NAME,
                False,
                id="missing_name",
            ),
            pytest.param(
                {"name": "collection"},
                NO_PROTOCOL_FOR_NEW_SERVICE,
                False,
                id="missing_protocol",
            ),
            pytest.param(
                {"name": "collection", "protocol": "Unknown"},
                UNKNOWN_PROTOCOL,
                False,
                id="unknown_protocol",
            ),
            pytest.param(
                {"id": "123456789", "name": "collection", "protocol": "Bibliotheca"},
                MISSING_SERVICE,
                False,
                id="missing_service",
            ),
            pytest.param(
                {"name": "Collection 1", "protocol": "Bibliotheca"},
                INTEGRATION_NAME_ALREADY_IN_USE,
                False,
                id="name_in_use",
            ),
            pytest.param(
                {"id": "", "name": "Collection 1", "protocol": "Bibliotheca"},
                CANNOT_CHANGE_PROTOCOL,
                False,
                id="change_protocol",
            ),
            pytest.param(
                {
                    "name": "Collection 2",
                    "protocol": "Bibliotheca",
                    "parent_id": "1234",
                },
                PROTOCOL_DOES_NOT_SUPPORT_PARENTS,
                False,
                id="protocol_does_not_support_parents",
            ),
            pytest.param(
                {"name": "Collection 2", "protocol": "Overdrive", "parent_id": "1234"},
                MISSING_PARENT,
                False,
                id="missing_parent",
            ),
            pytest.param(
                {
                    "name": "collection",
                    "protocol": "OPDS Import",
                    "external_account_id": "http://url.test",
                    "data_source": "test",
                    "libraries": json.dumps([{"short_name": "nosuchlibrary"}]),
                },
                NO_SUCH_LIBRARY,
                True,
                id="no_such_library",
            ),
            pytest.param(
                {"name": "collection", "protocol": "OPDS Import"},
                INCOMPLETE_CONFIGURATION,
                True,
                id="incomplete_opds",
            ),
            pytest.param(
                {
                    "name": "collection",
                    "protocol": "Overdrive",
                    "external_account_id": "1234",
                    "overdrive_client_key": "user",
                    "overdrive_client_secret": "password",
                },
                INCOMPLETE_CONFIGURATION,
                True,
                id="incomplete_overdrive",
            ),
            pytest.param(
                {
                    "name": "collection",
                    "protocol": "Bibliotheca",
                    "external_account_id": "1234",
                    "password": "password",
                },
                INCOMPLETE_CONFIGURATION,
                True,
                id="incomplete_bibliotheca",
            ),
            pytest.param(
                {
                    "name": "collection",
                    "protocol": "Axis 360",
                    "username": "user",
                    "password": "password",
                },
                INCOMPLETE_CONFIGURATION,
                True,
                id="incomplete_axis",
            ),
        ],
    )
    def test_collections_post_errors(
        self,
        controller: CollectionSettingsController,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        post_data: dict[str, str],
        expected: ProblemDetail,
        detailed: bool,
    ):
        collection = db.collection(
            name="Collection 1", protocol=ExternalIntegration.OVERDRIVE
        )

        if "id" in post_data and post_data["id"] == "":
            post_data["id"] = str(collection.integration_configuration.id)

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(post_data)
            response = controller.process_collections()

        if detailed:
            assert isinstance(response, ProblemDetail)
            assert response.status_code == expected.status_code
            assert response.uri == expected.uri
        else:
            assert response == expected

    def test_collections_post_errors_no_permissions(
        self,
        controller: CollectionSettingsController,
        flask_app_fixture: FlaskAppFixture,
    ):
        with flask_app_fixture.test_request_context("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Collection 1"),
                    ("protocol", "Overdrive"),
                ]
            )
            pytest.raises(
                AdminNotAuthorized,
                controller.process_collections,
            )

    def test_collections_post_create(
        self,
        controller: CollectionSettingsController,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        l1 = db.library(
            name="Library 1",
            short_name="L1",
        )
        l2 = db.library(
            name="Library 2",
            short_name="L2",
        )
        l3 = db.library(
            name="Library 3",
            short_name="L3",
        )

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "New Collection"),
                    ("protocol", "Overdrive"),
                    (
                        "libraries",
                        json.dumps(
                            [
                                {"short_name": "L1", "ils_name": "l1_ils"},
                                {"short_name": "L2", "ils_name": "l2_ils"},
                            ]
                        ),
                    ),
                    ("external_account_id", "acctid"),
                    ("overdrive_client_key", "username"),
                    ("overdrive_client_secret", "password"),
                    ("overdrive_website_id", "1234"),
                ]
            )
            response = controller.process_collections()
            assert isinstance(response, Response)
            assert response.status_code == 201

        # The collection was created and configured properly.
        collection = Collection.by_name(db.session, name="New Collection")
        assert isinstance(collection, Collection)
        assert collection.integration_configuration.id == int(response.get_data())
        assert "New Collection" == collection.name
        assert (
            "acctid"
            == collection.integration_configuration.settings_dict["external_account_id"]
        )
        assert (
            "username"
            == collection.integration_configuration.settings_dict[
                "overdrive_client_key"
            ]
        )
        assert (
            "password"
            == collection.integration_configuration.settings_dict[
                "overdrive_client_secret"
            ]
        )

        # Two libraries now have access to the collection.
        assert [collection] == l1.collections
        assert [collection] == l2.collections
        assert [] == l3.collections

        # Additional settings were set on the collection.
        assert (
            "1234"
            == collection.integration_configuration.settings_dict[
                "overdrive_website_id"
            ]
        )
        assert isinstance(l1.id, int)
        l1_settings = collection.integration_configuration.for_library(l1.id)
        assert l1_settings is not None
        assert "l1_ils" == l1_settings.settings_dict["ils_name"]
        assert isinstance(l2.id, int)
        l2_settings = collection.integration_configuration.for_library(l2.id)
        assert l2_settings is not None
        assert "l2_ils" == l2_settings.settings_dict["ils_name"]

        # This collection will be a child of the first collection.
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Child Collection"),
                    ("protocol", "Overdrive"),
                    ("parent_id", str(collection.integration_configuration.id)),
                    (
                        "libraries",
                        json.dumps([{"short_name": "L3", "ils_name": "l3_ils"}]),
                    ),
                    ("external_account_id", "child-acctid"),
                ]
            )
            response = controller.process_collections()
            assert isinstance(response, Response)
            assert response.status_code == 201

        # The collection was created and configured properly.
        child = Collection.by_name(db.session, name="Child Collection")
        assert isinstance(child, Collection)
        assert child.integration_configuration.id == int(response.get_data())
        assert "Child Collection" == child.name
        assert (
            "child-acctid"
            == child.integration_configuration.settings_dict["external_account_id"]
        )

        # The settings that are inherited from the parent weren't set.
        assert "username" not in child.integration_configuration.settings_dict
        assert "password" not in child.integration_configuration.settings_dict
        assert "website_id" not in child.integration_configuration.settings_dict

        # One library has access to the collection.
        assert [child] == l3.collections
        assert isinstance(l3.id, int)
        l3_settings = child.integration_configuration.for_library(l3.id)
        assert l3_settings is not None
        assert "l3_ils" == l3_settings.settings_dict["ils_name"]

    def test_collections_post_edit(
        self,
        controller: CollectionSettingsController,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        # The collection exists.
        collection = db.collection(
            name="Collection 1", protocol=ExternalIntegration.OVERDRIVE
        )

        l1 = db.library(
            name="Library 1",
            short_name="L1",
        )

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(collection.integration_configuration.id)),
                    ("name", "Collection 1"),
                    ("protocol", ExternalIntegration.OVERDRIVE),
                    ("external_account_id", "1234"),
                    ("overdrive_client_key", "user2"),
                    ("overdrive_client_secret", "password"),
                    ("overdrive_website_id", "1234"),
                    ("max_retry_count", "10"),
                    (
                        "libraries",
                        json.dumps([{"short_name": "L1", "ils_name": "the_ils"}]),
                    ),
                ]
            )
            response = controller.process_collections()
            assert response.status_code == 200
            assert isinstance(response, Response)

        assert collection.integration_configuration.id == int(response.get_data())

        # The collection has been changed.
        assert "user2" == collection.integration_configuration.settings_dict.get(
            "overdrive_client_key"
        )

        # Type coercion stays intact
        assert 10 == collection.integration_configuration.settings_dict.get(
            "max_retry_count"
        )

        # A library now has access to the collection.
        assert collection.libraries == [l1]

        # Additional settings were set on the collection.
        assert "1234" == collection.integration_configuration.settings_dict.get(
            "overdrive_website_id"
        )
        assert isinstance(l1.id, int)
        l1_settings = collection.integration_configuration.for_library(l1.id)
        assert l1_settings is not None
        assert "the_ils" == l1_settings.settings_dict.get("ils_name")

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(collection.integration_configuration.id)),
                    ("name", "Collection 1"),
                    ("protocol", ExternalIntegration.OVERDRIVE),
                    ("external_account_id", "1234"),
                    ("overdrive_client_key", "user2"),
                    ("overdrive_client_secret", "password"),
                    ("overdrive_website_id", "1234"),
                    ("libraries", json.dumps([])),
                ]
            )
            response = controller.process_collections()
            assert response.status_code == 200
            assert isinstance(response, Response)

        assert collection.integration_configuration.id == int(response.get_data())

        # The collection is the same.
        assert "user2" == collection.integration_configuration.settings_dict.get(
            "overdrive_client_key"
        )
        assert ExternalIntegration.OVERDRIVE == collection.protocol

        # But the library has been removed.
        assert collection.libraries == []

        # All ConfigurationSettings for that library and collection
        # have been deleted.
        assert collection.integration_configuration.library_configurations == []

        parent = db.collection(name="Parent", protocol=ExternalIntegration.OVERDRIVE)

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(collection.integration_configuration.id)),
                    ("name", "Collection 1"),
                    ("protocol", ExternalIntegration.OVERDRIVE),
                    ("parent_id", str(parent.integration_configuration.id)),
                    ("external_account_id", "1234"),
                    ("libraries", json.dumps([])),
                ]
            )
            response = controller.process_collections()
            assert response.status_code == 200
            assert isinstance(response, Response)

        assert collection.integration_configuration.id == int(response.get_data())

        # The collection now has a parent.
        assert parent == collection.parent

        library = db.default_library()
        collection2 = db.collection(
            name="Collection 2", protocol=ExternalIntegration.ODL
        )
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(collection2.integration_configuration.id)),
                    ("name", "Collection 2"),
                    ("protocol", ExternalIntegration.ODL),
                    ("external_account_id", "http://test.com/feed"),
                    ("username", "user"),
                    ("password", "password"),
                    ("data_source", "datasource"),
                    ("passphrase_hint", "passphrase_hint"),
                    ("passphrase_hint_url", "http://passphrase_hint_url.com"),
                    (
                        "libraries",
                        json.dumps(
                            [
                                {
                                    "short_name": library.short_name,
                                    "ebook_loan_duration": "200",
                                }
                            ]
                        ),
                    ),
                ]
            )
            response = controller.process_collections()
            assert response.status_code == 200
            assert isinstance(response, Response)

        assert len(collection2.integration_configuration.library_configurations) == 1
        # The library configuration value was correctly coerced to int
        assert (
            collection2.integration_configuration.library_configurations[
                0
            ].settings_dict.get("ebook_loan_duration")
            == 200
        )

    def test_collections_post_edit_library_specific_configuration(
        self,
        controller: CollectionSettingsController,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        # The collection exists.
        collection = db.collection(
            name="Collection 1", protocol=ExternalIntegration.AXIS_360
        )

        l1 = db.library(
            name="Library 1",
            short_name="L1",
        )

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(collection.integration_configuration.id)),
                    ("name", "Collection 1"),
                    ("protocol", ExternalIntegration.AXIS_360),
                    ("external_account_id", "1234"),
                    ("username", "user2"),
                    ("password", "password"),
                    ("url", "http://axis.test/"),
                    (
                        "libraries",
                        json.dumps([{"short_name": "L1", "ebook_loan_duration": "14"}]),
                    ),
                ]
            )
            response = controller.process_collections()
            assert response.status_code == 200

        # Additional settings were set on the collection+library.
        assert isinstance(l1.id, int)
        l1_settings = collection.integration_configuration.for_library(l1.id)
        assert l1_settings is not None
        assert "14" == l1_settings.settings_dict.get("ebook_loan_duration")

        # Remove the connection between collection and library.
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(collection.integration_configuration.id)),
                    ("name", "Collection 1"),
                    ("protocol", ExternalIntegration.AXIS_360),
                    ("external_account_id", "1234"),
                    ("username", "user2"),
                    ("password", "password"),
                    ("url", "http://axis.test/"),
                    ("libraries", json.dumps([])),
                ]
            )
            response = controller.process_collections()
            assert response.status_code == 200
            assert isinstance(response, Response)

        assert collection.integration_configuration.id == int(response.get_data())

        # The settings associated with the collection+library were removed
        # when the connection between collection and library was deleted.
        assert isinstance(l1.id, int)
        assert collection.integration_configuration.for_library(l1.id) is None
        assert [] == collection.libraries

    def test_collection_delete(
        self,
        controller: CollectionSettingsController,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        collection = db.collection()
        assert collection.marked_for_deletion is False

        with flask_app_fixture.test_request_context("/", method="DELETE"):
            pytest.raises(
                AdminNotAuthorized,
                controller.process_delete,
                collection.integration_configuration.id,
            )

        with flask_app_fixture.test_request_context_system_admin("/", method="DELETE"):
            assert collection.integration_configuration.id is not None
            response = controller.process_delete(
                collection.integration_configuration.id
            )
            assert response.status_code == 200
            assert isinstance(response, Response)

        # The collection should still be available because it is not immediately deleted.
        # The collection will be deleted in the background by a script, but it is
        # now marked for deletion
        fetched_collection = get_one(db.session, Collection, id=collection.id)
        assert fetched_collection == collection
        assert fetched_collection.marked_for_deletion is True

    def test_collection_delete_cant_delete_parent(
        self,
        controller: CollectionSettingsController,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        parent = db.collection(protocol=ExternalIntegration.OVERDRIVE)
        child = db.collection(protocol=ExternalIntegration.OVERDRIVE)
        child.parent = parent

        with flask_app_fixture.test_request_context_system_admin("/", method="DELETE"):
            assert parent.integration_configuration.id is not None
            response = controller.process_delete(parent.integration_configuration.id)
            assert response == CANNOT_DELETE_COLLECTION_WITH_CHILDREN

    def test_collection_self_tests_with_no_identifier(
        self, controller: CollectionSettingsController
    ):
        response = controller.process_collection_self_tests(None)
        assert isinstance(response, ProblemDetail)
        assert response.title == MISSING_IDENTIFIER.title
        assert response.detail == MISSING_IDENTIFIER.detail
        assert response.status_code == 400

    def test_collection_self_tests_with_no_collection_found(
        self, controller: CollectionSettingsController
    ):
        with pytest.raises(ProblemError) as excinfo:
            controller.self_tests_process_get(-1)
        assert excinfo.value.problem_detail == MISSING_SERVICE

    def test_collection_self_tests_with_unknown_protocol(
        self, db: DatabaseTransactionFixture, controller: CollectionSettingsController
    ):
        collection = db.collection(protocol="test")
        assert collection.integration_configuration.id is not None
        with pytest.raises(ProblemError) as excinfo:
            controller.self_tests_process_get(collection.integration_configuration.id)
        assert excinfo.value.problem_detail == UNKNOWN_PROTOCOL

    def test_collection_self_tests_with_unsupported_protocol(
        self, db: DatabaseTransactionFixture, flask_app_fixture: FlaskAppFixture
    ):
        registry = LicenseProvidersRegistry()
        registry.register(object, canonical="mock_api")  # type: ignore[arg-type]
        collection = db.collection(protocol="mock_api")
        manager = MagicMock()
        manager._db = db.session
        controller = CollectionSettingsController(manager, registry)
        assert collection.integration_configuration.id is not None

        with flask_app_fixture.test_request_context_system_admin("/"):
            result = controller.self_tests_process_get(
                collection.integration_configuration.id
            )

        assert result.status_code == 200
        assert isinstance(result.json, dict)
        assert result.json["self_test_results"]["self_test_results"] == {
            "disabled": True,
            "exception": "Self tests are not supported for this integration.",
        }

    def test_collection_self_tests_test_get(
        self,
        db: DatabaseTransactionFixture,
        controller: CollectionSettingsController,
        flask_app_fixture: FlaskAppFixture,
        monkeypatch: MonkeyPatch,
    ):
        collection = MockAxis360API.mock_collection(
            db.session,
            db.default_library(),
        )

        self_test_results = dict(
            duration=0.9,
            start="2018-08-08T16:04:05Z",
            end="2018-08-08T16:05:05Z",
            results=[],
        )
        mock = MagicMock(return_value=self_test_results)
        monkeypatch.setattr(HasSelfTests, "load_self_test_results", mock)

        # Make sure that HasSelfTest.prior_test_results() was called and that
        # it is in the response's collection object.
        assert collection.integration_configuration.id is not None
        with flask_app_fixture.test_request_context_system_admin("/"):
            response = controller.self_tests_process_get(
                collection.integration_configuration.id
            )

        data = response.json
        assert isinstance(data, dict)
        test_results = data.get("self_test_results")
        assert isinstance(test_results, dict)

        assert test_results.get("id") == collection.integration_configuration.id
        assert test_results.get("name") == collection.name
        assert test_results.get("protocol") == collection.protocol
        assert test_results.get("self_test_results") == self_test_results
        assert mock.call_count == 1

    def test_collection_self_tests_failed_post(
        self,
        db: DatabaseTransactionFixture,
        controller: CollectionSettingsController,
        monkeypatch: MonkeyPatch,
    ):
        collection = MockAxis360API.mock_collection(
            db.session,
            db.default_library(),
        )

        # This makes HasSelfTests.run_self_tests return no values
        self_test_results = (None, None)
        mock = MagicMock(return_value=self_test_results)
        monkeypatch.setattr(HasSelfTests, "run_self_tests", mock)

        # Failed to run self tests
        assert collection.integration_configuration.id is not None

        with pytest.raises(ProblemError) as excinfo:
            controller.self_tests_process_post(collection.integration_configuration.id)

        assert excinfo.value.problem_detail == FAILED_TO_RUN_SELF_TESTS

    def test_collection_self_tests_run_self_tests_unsupported_collection(
        self,
        db: DatabaseTransactionFixture,
    ):
        registry = LicenseProvidersRegistry()
        registry.register(object, canonical="mock_api")  # type: ignore[arg-type]
        collection = db.collection(protocol="mock_api")
        manager = MagicMock()
        manager._db = db.session
        controller = CollectionSettingsController(manager, registry)
        response = controller.run_self_tests(collection.integration_configuration)
        assert response is None

    def test_collection_self_tests_post(
        self,
        db: DatabaseTransactionFixture,
    ):
        mock = MagicMock()

        class MockApi(HasCollectionSelfTests):
            def __new__(cls, *args, **kwargs):
                nonlocal mock
                return mock(*args, **kwargs)

            @property
            def collection(self) -> None:
                return None

        registry = LicenseProvidersRegistry()
        registry.register(MockApi, canonical="Foo")  # type: ignore[arg-type]

        collection = db.collection(protocol="Foo")
        manager = MagicMock()
        manager._db = db.session
        controller = CollectionSettingsController(manager, registry)

        assert collection.integration_configuration.id is not None
        response = controller.self_tests_process_post(
            collection.integration_configuration.id
        )

        assert response.get_data(as_text=True) == "Successfully ran new self tests"
        assert response.status_code == 200

        mock.assert_called_once_with(db.session, collection)
        mock()._run_self_tests.assert_called_once_with(db.session)
        assert mock().store_self_test_results.call_count == 1
