import json

import flask
import pytest
from werkzeug.datastructures import ImmutableMultiDict

from api.admin.exceptions import AdminNotAuthorized
from api.admin.problem_details import (
    CANNOT_CHANGE_PROTOCOL,
    CANNOT_DELETE_COLLECTION_WITH_CHILDREN,
    COLLECTION_NAME_ALREADY_IN_USE,
    INCOMPLETE_CONFIGURATION,
    MISSING_COLLECTION,
    MISSING_COLLECTION_NAME,
    MISSING_PARENT,
    NO_PROTOCOL_FOR_NEW_SERVICE,
    NO_SUCH_LIBRARY,
    PROTOCOL_DOES_NOT_SUPPORT_PARENTS,
    UNKNOWN_PROTOCOL,
)
from api.integration.registry.license_providers import LicenseProvidersRegistry
from api.selftest import HasCollectionSelfTests
from core.model import (
    Admin,
    AdminRole,
    Collection,
    ExternalIntegration,
    create,
    get_one,
)
from core.selftest import HasSelfTests
from tests.fixtures.api_admin import SettingsControllerFixture
from tests.fixtures.database import DatabaseTransactionFixture


class TestCollectionSettings:
    def test_collections_get_with_no_collections(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        # Delete any existing collections created by the test setup.
        for collection in settings_ctrl_fixture.ctrl.db.session.query(Collection):
            settings_ctrl_fixture.ctrl.db.session.delete(collection)

        with settings_ctrl_fixture.request_context_with_admin("/"):
            response = (
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response.get("collections") == []

            names = {p.get("name") for p in response.get("protocols")}
            expected_names = {k for k, v in LicenseProvidersRegistry()}
            assert names == expected_names

    def test_collections_get_collections_with_multiple_collections(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        old_prior_test_results = HasSelfTests.prior_test_results
        setattr(
            HasCollectionSelfTests,
            "prior_test_results",
            settings_ctrl_fixture.mock_prior_test_results,
        )
        session = settings_ctrl_fixture.ctrl.db.session

        [c1] = settings_ctrl_fixture.ctrl.db.default_library().collections

        c2 = settings_ctrl_fixture.ctrl.db.collection(
            name="Collection 2",
            protocol=ExternalIntegration.OVERDRIVE,
        )

        c2.external_account_id = "1234"
        DatabaseTransactionFixture.set_settings(
            c2.integration_configuration,
            overdrive_client_secret="b",
            overdrive_client_key="user",
            overdrive_website_id="100",
        )

        c3 = settings_ctrl_fixture.ctrl.db.collection(
            name="Collection 3",
            protocol=ExternalIntegration.OVERDRIVE,
        )
        c3.external_account_id = "5678"
        c3.parent = c2

        l1 = settings_ctrl_fixture.ctrl.db.library(short_name="L1")
        c3.libraries += [l1, settings_ctrl_fixture.ctrl.db.default_library()]
        assert isinstance(l1.id, int)
        l1_config = c3.integration_configuration.for_library(l1.id, create=True)
        DatabaseTransactionFixture.set_settings(l1_config, ebook_loan_duration="14")
        # Commit the config changes
        session.commit()

        l1_librarian, ignore = create(
            settings_ctrl_fixture.ctrl.db.session, Admin, email="admin@l1.org"
        )
        l1_librarian.add_role(AdminRole.LIBRARIAN, l1)

        with settings_ctrl_fixture.request_context_with_admin("/"):
            controller = (
                settings_ctrl_fixture.manager.admin_collection_settings_controller
            )
            response = controller.process_collections()
            # The system admin can see all collections.
            coll2, coll3, coll1 = sorted(
                response.get("collections"), key=lambda c: c.get("name")
            )
            assert c1.id == coll1.get("id")
            assert c2.id == coll2.get("id")
            assert c3.id == coll3.get("id")

            assert c1.name == coll1.get("name")
            assert c2.name == coll2.get("name")
            assert c3.name == coll3.get("name")

            assert c1.protocol == coll1.get("protocol")
            assert c2.protocol == coll2.get("protocol")
            assert c3.protocol == coll3.get("protocol")

            assert settings_ctrl_fixture.self_test_results == coll1.get(
                "self_test_results"
            )
            assert settings_ctrl_fixture.self_test_results == coll2.get(
                "self_test_results"
            )
            assert settings_ctrl_fixture.self_test_results == coll3.get(
                "self_test_results"
            )

            settings1 = coll1.get("settings", {})
            settings2 = coll2.get("settings", {})
            settings3 = coll3.get("settings", {})

            assert c1.external_account_id == settings1.get("external_account_id")
            assert c2.external_account_id == settings2.get("external_account_id")
            assert c3.external_account_id == settings3.get("external_account_id")

            assert c2.integration_configuration.settings_dict[
                "overdrive_client_secret"
            ] == settings2.get("overdrive_client_secret")

            assert c2.id == coll3.get("parent_id")

            coll3_libraries = coll3.get("libraries")
            assert 2 == len(coll3_libraries)
            coll3_l1, coll3_default = sorted(
                coll3_libraries, key=lambda x: x.get("short_name")
            )
            assert "L1" == coll3_l1.get("short_name")
            assert "14" == coll3_l1.get("ebook_loan_duration")
            assert (
                settings_ctrl_fixture.ctrl.db.default_library().short_name
                == coll3_default.get("short_name")
            )

        with settings_ctrl_fixture.request_context_with_admin("/", admin=l1_librarian):
            # A librarian only sees collections associated with their library.
            response = controller.process_collections()
            [coll3] = response.get("collections")
            assert c3.id == coll3.get("id")

            coll3_libraries = coll3.get("libraries")
            assert 1 == len(coll3_libraries)
            assert "L1" == coll3_libraries[0].get("short_name")
            assert "14" == coll3_libraries[0].get("ebook_loan_duration")

        setattr(HasCollectionSelfTests, "prior_test_results", old_prior_test_results)

    def test_collections_post_errors(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("protocol", "Overdrive"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response == MISSING_COLLECTION_NAME

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "collection"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response == NO_PROTOCOL_FOR_NEW_SERVICE

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "collection"),
                    ("protocol", "Unknown"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response == UNKNOWN_PROTOCOL

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", "123456789"),
                    ("name", "collection"),
                    ("protocol", "Bibliotheca"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response == MISSING_COLLECTION

        collection = settings_ctrl_fixture.ctrl.db.collection(
            name="Collection 1", protocol=ExternalIntegration.OVERDRIVE
        )

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Collection 1"),
                    ("protocol", "Bibliotheca"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response == COLLECTION_NAME_ALREADY_IN_USE

        settings_ctrl_fixture.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(collection.id)),
                    ("name", "Collection 1"),
                    ("protocol", "Overdrive"),
                ]
            )
            pytest.raises(
                AdminNotAuthorized,
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections,
            )

        settings_ctrl_fixture.admin.add_role(AdminRole.SYSTEM_ADMIN)
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(collection.id)),
                    ("name", "Collection 1"),
                    ("protocol", "Bibliotheca"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response == CANNOT_CHANGE_PROTOCOL

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Collection 2"),
                    ("protocol", "Bibliotheca"),
                    ("parent_id", "1234"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response == PROTOCOL_DOES_NOT_SUPPORT_PARENTS

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Collection 2"),
                    ("protocol", "Overdrive"),
                    ("parent_id", "1234"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response == MISSING_PARENT

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "collection"),
                    ("protocol", "OPDS Import"),
                    ("external_account_id", "http://url.test"),
                    ("data_source", "test"),
                    ("libraries", json.dumps([{"short_name": "nosuchlibrary"}])),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response.uri == NO_SUCH_LIBRARY.uri

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "collection1"),
                    ("protocol", "OPDS Import"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "collection1"),
                    ("protocol", "Overdrive"),
                    ("external_account_id", "1234"),
                    ("overdrive_client_key", "user"),
                    ("overdrive_client_secret", "password"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "collection1"),
                    ("protocol", "Bibliotheca"),
                    ("external_account_id", "1234"),
                    ("password", "password"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "collection1"),
                    ("protocol", "Axis 360"),
                    ("username", "user"),
                    ("password", "password"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

    def test_collections_post_create(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        db = settings_ctrl_fixture.ctrl.db
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

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
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
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response.status_code == 201

        # The collection was created and configured properly.
        collection = get_one(
            settings_ctrl_fixture.ctrl.db.session, Collection, name="New Collection"
        )
        assert isinstance(collection, Collection)
        assert collection.id == int(response.response[0])
        assert "New Collection" == collection.name
        assert "acctid" == collection.external_account_id
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
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Child Collection"),
                    ("protocol", "Overdrive"),
                    ("parent_id", str(collection.id)),
                    (
                        "libraries",
                        json.dumps([{"short_name": "L3", "ils_name": "l3_ils"}]),
                    ),
                    ("external_account_id", "child-acctid"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response.status_code == 201

        # The collection was created and configured properly.
        child = get_one(
            settings_ctrl_fixture.ctrl.db.session, Collection, name="Child Collection"
        )
        assert isinstance(child, Collection)
        assert child.id == int(response.response[0])
        assert "Child Collection" == child.name
        assert "child-acctid" == child.external_account_id

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
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        # The collection exists.
        collection = settings_ctrl_fixture.ctrl.db.collection(
            name="Collection 1", protocol=ExternalIntegration.OVERDRIVE
        )

        l1 = settings_ctrl_fixture.ctrl.db.library(
            name="Library 1",
            short_name="L1",
        )

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(collection.id)),
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
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response.status_code == 200

        assert collection.id == int(response.response[0])

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

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(collection.id)),
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
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response.status_code == 200

        assert collection.id == int(response.response[0])

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

        parent = settings_ctrl_fixture.ctrl.db.collection(
            name="Parent", protocol=ExternalIntegration.OVERDRIVE
        )

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(collection.id)),
                    ("name", "Collection 1"),
                    ("protocol", ExternalIntegration.OVERDRIVE),
                    ("parent_id", str(parent.id)),
                    ("external_account_id", "1234"),
                    ("libraries", json.dumps([])),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response.status_code == 200

        assert collection.id == int(response.response[0])

        # The collection now has a parent.
        assert parent == collection.parent

        library = settings_ctrl_fixture.ctrl.db.default_library()
        collection2 = settings_ctrl_fixture.ctrl.db.collection(
            name="Collection 2", protocol=ExternalIntegration.ODL
        )
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(collection2.id)),
                    ("name", "Collection 2"),
                    ("protocol", ExternalIntegration.ODL),
                    ("external_account_id", "1234"),
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
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response.status_code == 200

        settings_ctrl_fixture.ctrl.db.session.refresh(collection2)
        assert len(collection2.integration_configuration.library_configurations) == 1
        # The library configuration value was correctly coerced to int
        assert (
            collection2.integration_configuration.library_configurations[
                0
            ].settings_dict.get("ebook_loan_duration")
            == 200
        )

    def _base_collections_post_request(self, collection):
        """A template for POST requests to the collections controller."""
        return [
            ("id", str(collection.id)),
            ("name", "Collection 1"),
            ("protocol", ExternalIntegration.AXIS_360),
            ("external_account_id", "1234"),
            ("username", "user2"),
            ("password", "password"),
            ("url", "http://axis.test/"),
        ]

    def test_collections_post_edit_library_specific_configuration(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        # The collection exists.
        collection = settings_ctrl_fixture.ctrl.db.collection(
            name="Collection 1", protocol=ExternalIntegration.AXIS_360
        )

        l1 = settings_ctrl_fixture.ctrl.db.library(
            name="Library 1",
            short_name="L1",
        )

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(collection.id)),
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
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response.status_code == 200

        # Additional settings were set on the collection+library.
        assert isinstance(l1.id, int)
        l1_settings = collection.integration_configuration.for_library(l1.id)
        assert l1_settings is not None
        assert "14" == l1_settings.settings_dict.get("ebook_loan_duration")

        # Remove the connection between collection and library.
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(collection.id)),
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
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response.status_code == 200

        assert collection.id == int(response.response[0])

        # The settings associated with the collection+library were removed
        # when the connection between collection and library was deleted.
        assert isinstance(l1.id, int)
        assert None == collection.integration_configuration.for_library(l1.id)
        assert [] == collection.libraries

    def test_collection_delete(self, settings_ctrl_fixture: SettingsControllerFixture):
        collection = settings_ctrl_fixture.ctrl.db.collection()
        assert False == collection.marked_for_deletion

        with settings_ctrl_fixture.request_context_with_admin("/", method="DELETE"):
            settings_ctrl_fixture.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            pytest.raises(
                AdminNotAuthorized,
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_delete,
                collection.id,
            )

            settings_ctrl_fixture.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = settings_ctrl_fixture.manager.admin_collection_settings_controller.process_delete(
                collection.id
            )
            assert response.status_code == 200

        # The collection should still be available because it is not immediately deleted.
        # The collection will be deleted in the background by a script, but it is
        # now marked for deletion
        fetchedCollection = get_one(
            settings_ctrl_fixture.ctrl.db.session, Collection, id=collection.id
        )
        assert collection == fetchedCollection
        assert True == fetchedCollection.marked_for_deletion

    def test_collection_delete_cant_delete_parent(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        parent = settings_ctrl_fixture.ctrl.db.collection(
            protocol=ExternalIntegration.OVERDRIVE
        )
        child = settings_ctrl_fixture.ctrl.db.collection(
            protocol=ExternalIntegration.OVERDRIVE
        )
        child.parent = parent

        with settings_ctrl_fixture.request_context_with_admin("/", method="DELETE"):
            response = settings_ctrl_fixture.manager.admin_collection_settings_controller.process_delete(
                parent.id
            )
            assert CANNOT_DELETE_COLLECTION_WITH_CHILDREN == response
