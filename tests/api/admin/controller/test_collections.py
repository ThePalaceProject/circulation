import json

import flask
import pytest
from flask import Response
from werkzeug.datastructures import ImmutableMultiDict

from api.admin.exceptions import AdminNotAuthorized
from api.admin.problem_details import (
    CANNOT_CHANGE_PROTOCOL,
    CANNOT_DELETE_COLLECTION_WITH_CHILDREN,
    INCOMPLETE_CONFIGURATION,
    INTEGRATION_NAME_ALREADY_IN_USE,
    MISSING_PARENT,
    MISSING_SERVICE,
    MISSING_SERVICE_NAME,
    NO_PROTOCOL_FOR_NEW_SERVICE,
    NO_SUCH_LIBRARY,
    PROTOCOL_DOES_NOT_SUPPORT_PARENTS,
    UNKNOWN_PROTOCOL,
)
from api.integration.registry.license_providers import LicenseProvidersRegistry
from core.model import (
    Admin,
    AdminRole,
    Collection,
    ExternalIntegration,
    create,
    get_one,
)
from core.util.problem_detail import ProblemDetail
from tests.fixtures.api_admin import AdminControllerFixture
from tests.fixtures.database import DatabaseTransactionFixture


class TestCollectionSettings:
    def test_collections_get_with_no_collections(
        self, admin_ctrl_fixture: AdminControllerFixture
    ) -> None:
        db = admin_ctrl_fixture.ctrl.db
        # Delete any existing collections created by the test setup.
        db.session.delete(db.default_collection())

        with admin_ctrl_fixture.request_context_with_admin("/"):
            response = (
                admin_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert isinstance(response, Response)
            assert response.status_code == 200
            data = response.json
            assert isinstance(data, dict)
            assert data.get("collections") == []

            names = {p.get("name") for p in data.get("protocols", {})}
            expected_names = {k for k, v in LicenseProvidersRegistry()}
            assert names == expected_names

    def test_collections_get_collections_with_multiple_collections(
        self, admin_ctrl_fixture: AdminControllerFixture
    ) -> None:
        session = admin_ctrl_fixture.ctrl.db.session
        db = admin_ctrl_fixture.ctrl.db

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
        # Commit the config changes
        session.commit()

        l1_librarian, ignore = create(session, Admin, email="admin@l1.org")
        l1_librarian.add_role(AdminRole.LIBRARIAN, l1)

        admin_ctrl_fixture.admin.add_role(AdminRole.SYSTEM_ADMIN)

        with admin_ctrl_fixture.request_context_with_admin("/"):
            controller = admin_ctrl_fixture.manager.admin_collection_settings_controller
            response = controller.process_collections()
            assert isinstance(response, Response)
            assert response.status_code == 200
            data = response.json
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

            assert (
                settings1.get("external_account_id") == "http://opds.example.com/feed"
            )
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

        with admin_ctrl_fixture.request_context_with_admin("/", admin=l1_librarian):
            # A librarian only sees collections associated with their library.
            response = controller.process_collections()
            assert isinstance(response, Response)
            assert response.status_code == 200
            data = response.json
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
        admin_ctrl_fixture: AdminControllerFixture,
        post_data: dict[str, str],
        expected: ProblemDetail,
        detailed: bool,
    ):
        admin_ctrl_fixture.admin.add_role(AdminRole.SYSTEM_ADMIN)

        collection = admin_ctrl_fixture.ctrl.db.collection(
            name="Collection 1", protocol=ExternalIntegration.OVERDRIVE
        )

        if "id" in post_data and post_data["id"] == "":
            post_data["id"] = str(collection.integration_configuration.id)

        with admin_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(post_data)
            response = (
                admin_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )

        if detailed:
            assert isinstance(response, ProblemDetail)
            assert response.status_code == expected.status_code
            assert response.uri == expected.uri
        else:
            assert response == expected

    def test_collections_post_errors_no_permissions(
        self, admin_ctrl_fixture: AdminControllerFixture
    ):
        with admin_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Collection 1"),
                    ("protocol", "Overdrive"),
                ]
            )
            pytest.raises(
                AdminNotAuthorized,
                admin_ctrl_fixture.manager.admin_collection_settings_controller.process_collections,
            )

    def test_collections_post_create(self, admin_ctrl_fixture: AdminControllerFixture):
        admin_ctrl_fixture.admin.add_role(AdminRole.SYSTEM_ADMIN)
        db = admin_ctrl_fixture.ctrl.db
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

        with admin_ctrl_fixture.request_context_with_admin("/", method="POST"):
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
            response = (
                admin_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
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
        with admin_ctrl_fixture.request_context_with_admin("/", method="POST"):
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
            response = (
                admin_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
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

    def test_collections_post_edit(self, admin_ctrl_fixture: AdminControllerFixture):
        # The collection exists.
        admin_ctrl_fixture.admin.add_role(AdminRole.SYSTEM_ADMIN)
        db = admin_ctrl_fixture.ctrl.db
        collection = db.collection(
            name="Collection 1", protocol=ExternalIntegration.OVERDRIVE
        )

        l1 = db.library(
            name="Library 1",
            short_name="L1",
        )

        with admin_ctrl_fixture.request_context_with_admin("/", method="POST"):
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
            response = (
                admin_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
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
        assert [collection] == l1.collections

        # Additional settings were set on the collection.
        assert "1234" == collection.integration_configuration.settings_dict.get(
            "overdrive_website_id"
        )
        assert isinstance(l1.id, int)
        l1_settings = collection.integration_configuration.for_library(l1.id)
        assert l1_settings is not None
        assert "the_ils" == l1_settings.settings_dict.get("ils_name")

        with admin_ctrl_fixture.request_context_with_admin("/", method="POST"):
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
            response = (
                admin_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response.status_code == 200
            assert isinstance(response, Response)

        assert collection.integration_configuration.id == int(response.get_data())

        # The collection is the same.
        assert "user2" == collection.integration_configuration.settings_dict.get(
            "overdrive_client_key"
        )
        assert ExternalIntegration.OVERDRIVE == collection.protocol

        # But the library has been removed.
        assert [] == l1.collections

        # All ConfigurationSettings for that library and collection
        # have been deleted.
        assert collection.integration_configuration.library_configurations == []

        parent = db.collection(name="Parent", protocol=ExternalIntegration.OVERDRIVE)

        with admin_ctrl_fixture.request_context_with_admin("/", method="POST"):
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
            response = (
                admin_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response.status_code == 200
            assert isinstance(response, Response)

        assert collection.integration_configuration.id == int(response.get_data())

        # The collection now has a parent.
        assert parent == collection.parent

        library = db.default_library()
        collection2 = db.collection(
            name="Collection 2", protocol=ExternalIntegration.ODL
        )
        with admin_ctrl_fixture.request_context_with_admin("/", method="POST"):
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
            response = (
                admin_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response.status_code == 200
            assert isinstance(response, Response)

        admin_ctrl_fixture.ctrl.db.session.refresh(collection2)
        assert len(collection2.integration_configuration.library_configurations) == 1
        # The library configuration value was correctly coerced to int
        assert (
            collection2.integration_configuration.library_configurations[
                0
            ].settings_dict.get("ebook_loan_duration")
            == 200
        )

    def test_collections_post_edit_library_specific_configuration(
        self, admin_ctrl_fixture: AdminControllerFixture
    ):
        # The collection exists.
        db = admin_ctrl_fixture.ctrl.db
        admin_ctrl_fixture.admin.add_role(AdminRole.SYSTEM_ADMIN)
        collection = db.collection(
            name="Collection 1", protocol=ExternalIntegration.AXIS_360
        )

        l1 = db.library(
            name="Library 1",
            short_name="L1",
        )

        with admin_ctrl_fixture.request_context_with_admin("/", method="POST"):
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
            response = (
                admin_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response.status_code == 200

        # Additional settings were set on the collection+library.
        assert isinstance(l1.id, int)
        l1_settings = collection.integration_configuration.for_library(l1.id)
        assert l1_settings is not None
        assert "14" == l1_settings.settings_dict.get("ebook_loan_duration")

        # Remove the connection between collection and library.
        with admin_ctrl_fixture.request_context_with_admin("/", method="POST"):
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
            response = (
                admin_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response.status_code == 200
            assert isinstance(response, Response)

        assert collection.integration_configuration.id == int(response.get_data())

        # The settings associated with the collection+library were removed
        # when the connection between collection and library was deleted.
        assert isinstance(l1.id, int)
        assert collection.integration_configuration.for_library(l1.id) is None
        assert [] == collection.libraries

    def test_collection_delete(self, admin_ctrl_fixture: AdminControllerFixture):
        db = admin_ctrl_fixture.ctrl.db
        collection = db.collection()
        assert collection.marked_for_deletion is False

        with admin_ctrl_fixture.request_context_with_admin("/", method="DELETE"):
            pytest.raises(
                AdminNotAuthorized,
                admin_ctrl_fixture.manager.admin_collection_settings_controller.process_delete,
                collection.integration_configuration.id,
            )

            admin_ctrl_fixture.admin.add_role(AdminRole.SYSTEM_ADMIN)
            assert collection.integration_configuration.id is not None
            response = admin_ctrl_fixture.manager.admin_collection_settings_controller.process_delete(
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
        self, admin_ctrl_fixture: AdminControllerFixture
    ):
        admin_ctrl_fixture.admin.add_role(AdminRole.SYSTEM_ADMIN)
        db = admin_ctrl_fixture.ctrl.db
        parent = db.collection(protocol=ExternalIntegration.OVERDRIVE)
        child = db.collection(protocol=ExternalIntegration.OVERDRIVE)
        child.parent = parent

        with admin_ctrl_fixture.request_context_with_admin("/", method="DELETE"):
            assert parent.integration_configuration.id is not None
            response = admin_ctrl_fixture.manager.admin_collection_settings_controller.process_delete(
                parent.integration_configuration.id
            )
            assert response == CANNOT_DELETE_COLLECTION_WITH_CHILDREN
