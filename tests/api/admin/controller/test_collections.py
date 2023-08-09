import json

import flask
import pytest
from werkzeug.datastructures import ImmutableMultiDict

from api.admin.exceptions import *
from api.selftest import HasCollectionSelfTests
from core.model import (
    Admin,
    AdminRole,
    Collection,
    ExternalIntegration,
    create,
    get_one,
)
from core.model.configuration import ExternalIntegrationLink
from core.s3 import S3UploaderConfiguration
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

            names = [p.get("name") for p in response.get("protocols")]
            assert ExternalIntegration.OVERDRIVE in names
            assert ExternalIntegration.OPDS_IMPORT in names

    def test_collections_get_collection_protocols(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        old_prior_test_results = HasSelfTests.prior_test_results
        setattr(
            HasSelfTests,
            "prior_test_results",
            settings_ctrl_fixture.mock_prior_test_results,
        )

        l1 = settings_ctrl_fixture.ctrl.db.default_library()
        [c1] = l1.collections

        # When there is no storage integration configured,
        # the protocols will not offer a 'mirror_integration_id'
        # setting for covers or books.
        with settings_ctrl_fixture.request_context_with_admin("/"):
            response = (
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            protocols = response.get("protocols")
            for protocol in protocols:
                assert all(
                    [
                        not s.get("key").endswith("mirror_integration_id")
                        for s in protocol["settings"]
                        if s
                    ]
                )

        # When storage integrations are configured, each protocol will
        # offer a 'mirror_integration_id' setting for covers and books.
        storage1 = settings_ctrl_fixture.ctrl.db.external_integration(
            name="integration 1",
            protocol=ExternalIntegration.S3,
            goal=ExternalIntegration.STORAGE_GOAL,
            settings={
                S3UploaderConfiguration.BOOK_COVERS_BUCKET_KEY: "covers",
                S3UploaderConfiguration.OA_CONTENT_BUCKET_KEY: "open-access-books",
                S3UploaderConfiguration.PROTECTED_CONTENT_BUCKET_KEY: "protected-access-books",
            },
        )
        storage2 = settings_ctrl_fixture.ctrl.db.external_integration(
            name="integration 2",
            protocol="Some other protocol",
            goal=ExternalIntegration.STORAGE_GOAL,
            settings={
                S3UploaderConfiguration.BOOK_COVERS_BUCKET_KEY: "covers",
                S3UploaderConfiguration.OA_CONTENT_BUCKET_KEY: "open-access-books",
                S3UploaderConfiguration.PROTECTED_CONTENT_BUCKET_KEY: "protected-access-books",
            },
        )

        with settings_ctrl_fixture.request_context_with_admin("/"):
            controller = (
                settings_ctrl_fixture.manager.admin_collection_settings_controller
            )
            response = controller.process_collections()
            protocols = response.get("protocols")
            for protocol in protocols:
                mirror_settings = [
                    x
                    for x in protocol["settings"]
                    if x.get("key").endswith("mirror_integration_id")
                ]

                covers_mirror = mirror_settings[0]
                open_access_books_mirror = mirror_settings[1]
                protected_access_books_mirror = mirror_settings[2]
                assert "Covers Mirror" == covers_mirror["label"]
                assert "Open Access Books Mirror" == open_access_books_mirror["label"]
                assert (
                    "Protected Access Books Mirror"
                    == protected_access_books_mirror["label"]
                )
                covers_mirror_option = covers_mirror["options"]
                open_books_mirror_option = open_access_books_mirror["options"]
                protected_books_mirror_option = protected_access_books_mirror["options"]

                # The first option is to disable mirroring on this
                # collection altogether.
                no_mirror_covers = covers_mirror_option[0]
                no_mirror_open_books = open_books_mirror_option[0]
                no_mirror_protected_books = protected_books_mirror_option[0]
                assert controller.NO_MIRROR_INTEGRATION == no_mirror_covers["key"]
                assert controller.NO_MIRROR_INTEGRATION == no_mirror_open_books["key"]
                assert (
                    controller.NO_MIRROR_INTEGRATION == no_mirror_protected_books["key"]
                )

                # The other options are to use one of the storage
                # integrations to do the mirroring.
                use_covers_mirror = [
                    (x["key"], x["label"]) for x in covers_mirror_option[1:]
                ]
                use_open_books_mirror = [
                    (x["key"], x["label"]) for x in open_books_mirror_option[1:]
                ]
                use_protected_books_mirror = [
                    (x["key"], x["label"]) for x in protected_books_mirror_option[1:]
                ]

                # Expect to have two separate mirrors
                expect_covers = [
                    (str(integration.id), integration.name)
                    for integration in (storage1, storage2)
                ]
                assert expect_covers == use_covers_mirror
                expect_open_books = [
                    (str(integration.id), integration.name)
                    for integration in (storage1, storage2)
                ]
                assert expect_open_books == use_open_books_mirror
                expect_protected_books = [
                    (str(integration.id), integration.name)
                    for integration in (storage1, storage2)
                ]
                assert expect_protected_books == use_protected_books_mirror

        setattr(HasSelfTests, "prior_test_results", old_prior_test_results)

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
        c2_storage = settings_ctrl_fixture.ctrl.db.external_integration(
            protocol=ExternalIntegration.S3, goal=ExternalIntegration.STORAGE_GOAL
        )
        c2_external_integration_link = (
            settings_ctrl_fixture.ctrl.db.external_integration_link(
                integration=c2.external_integration,
                other_integration=c2_storage,
                purpose=ExternalIntegrationLink.COVERS,
            )
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

            assert controller.NO_MIRROR_INTEGRATION == settings1.get(
                "covers_mirror_integration_id"
            )
            assert controller.NO_MIRROR_INTEGRATION == settings1.get(
                "books_mirror_integration_id"
            )
            # Only added an integration for S3 storage for covers.
            assert str(c2_storage.id) == settings2.get("covers_mirror_integration_id")
            assert controller.NO_MIRROR_INTEGRATION == settings2.get(
                "books_mirror_integration_id"
            )
            assert controller.NO_MIRROR_INTEGRATION == settings3.get(
                "covers_mirror_integration_id"
            )
            assert controller.NO_MIRROR_INTEGRATION == settings3.get(
                "books_mirror_integration_id"
            )

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
        assert (
            "l1_ils"
            == collection.integration_configuration.for_library(l1.id).settings_dict[
                "ils_name"
            ]
        )
        assert isinstance(l2.id, int)
        assert (
            "l2_ils"
            == collection.integration_configuration.for_library(l2.id).settings_dict[
                "ils_name"
            ]
        )

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
        assert (
            "l3_ils"
            == child.integration_configuration.for_library(l3.id).settings_dict[
                "ils_name"
            ]
        )

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

        # A library now has access to the collection.
        assert [collection] == l1.collections

        # Additional settings were set on the collection.
        assert "1234" == collection.integration_configuration.settings_dict.get(
            "overdrive_website_id"
        )
        assert isinstance(l1.id, int)
        assert "the_ils" == collection.integration_configuration.for_library(
            l1.id
        ).settings_dict.get("ils_name")

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

    def test_collections_post_edit_mirror_integration(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        # The collection exists.
        collection = settings_ctrl_fixture.ctrl.db.collection(
            name="Collection 1", protocol=ExternalIntegration.AXIS_360
        )

        # There is a storage integration not associated with the collection.
        storage = settings_ctrl_fixture.ctrl.db.external_integration(
            protocol=ExternalIntegration.S3, goal=ExternalIntegration.STORAGE_GOAL
        )

        # It's possible to associate the storage integration with the
        # collection for either a books or covers mirror.
        base_request = self._base_collections_post_request(collection)
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                base_request + [("books_mirror_integration_id", storage.id)]
            )
            response = (
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response.status_code == 200

            # There is an external integration link to associate the collection's
            # external integration with the storage integration for a books mirror.
            external_integration_link = get_one(
                settings_ctrl_fixture.ctrl.db.session,
                ExternalIntegrationLink,
                external_integration_id=collection.external_integration.id,
            )
            assert isinstance(external_integration_link, ExternalIntegrationLink)
            assert storage.id == external_integration_link.other_integration_id

        # It's possible to unset the mirror integration.
        controller = settings_ctrl_fixture.manager.admin_collection_settings_controller
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                base_request
                + [
                    (
                        "books_mirror_integration_id",
                        str(controller.NO_MIRROR_INTEGRATION),
                    )
                ]
            )
            response = controller.process_collections()
            assert response.status_code == 200
            external_integration_link = get_one(
                settings_ctrl_fixture.ctrl.db.session,
                ExternalIntegrationLink,
                external_integration_id=collection.external_integration.id,
            )
            assert None == external_integration_link

        # Providing a nonexistent integration ID gives an error.
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                base_request + [("books_mirror_integration_id", -200)]
            )
            response = (
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response == MISSING_SERVICE

    def test_cannot_set_non_storage_integration_as_mirror_integration(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        # The collection exists.
        collection = settings_ctrl_fixture.ctrl.db.collection(
            name="Collection 1", protocol=ExternalIntegration.AXIS_360
        )

        # There is a storage integration not associated with the collection,
        # which makes it possible to associate storage integrations
        # with collections through the collections controller.
        storage = settings_ctrl_fixture.ctrl.db.external_integration(
            protocol=ExternalIntegration.S3, goal=ExternalIntegration.STORAGE_GOAL
        )

        # Trying to set a non-storage integration (such as the
        # integration associated with the collection's licenses) as
        # the collection's mirror integration gives an error.
        base_request = self._base_collections_post_request(collection)
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                base_request
                + [("books_mirror_integration_id", collection.external_integration.id)]
            )
            response = (
                settings_ctrl_fixture.manager.admin_collection_settings_controller.process_collections()
            )
            assert response == INTEGRATION_GOAL_CONFLICT

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
        assert "14" == collection.integration_configuration.for_library(
            l1.id
        ).settings_dict.get("ebook_loan_duration")

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
