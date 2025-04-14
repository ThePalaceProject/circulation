from __future__ import annotations

import base64
import datetime
import json
from io import BytesIO
from typing import Any
from unittest.mock import MagicMock, create_autospec

import flask
import pytest
from Crypto.PublicKey.RSA import RsaKey, import_key
from PIL import Image
from werkzeug import Response
from werkzeug.datastructures import FileStorage, ImmutableMultiDict

from palace.manager.api.admin.controller.library_settings import (
    LibrarySettingsController,
)
from palace.manager.api.admin.exceptions import AdminNotAuthorized
from palace.manager.api.admin.problem_details import (
    INCOMPLETE_CONFIGURATION,
    INVALID_CONFIGURATION_OPTION,
    LIBRARY_SHORT_NAME_ALREADY_IN_USE,
    UNKNOWN_LANGUAGE,
)
from palace.manager.api.config import Configuration
from palace.manager.api.problem_details import LIBRARY_NOT_FOUND
from palace.manager.core.facets import FacetConstants
from palace.manager.sqlalchemy.model.admin import AdminRole
from palace.manager.sqlalchemy.model.announcements import (
    SETTING_NAME as ANNOUNCEMENTS_SETTING_NAME,
    Announcement,
    AnnouncementData,
)
from palace.manager.sqlalchemy.model.library import Library, LibraryLogo
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException
from tests.fixtures.announcements import AnnouncementFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.flask import FlaskAppFixture
from tests.fixtures.library import LibraryFixture


@pytest.fixture
def controller(db: DatabaseTransactionFixture) -> LibrarySettingsController:
    mock_manager = MagicMock()
    mock_manager._db = db.session
    return LibrarySettingsController(mock_manager)


class TestLibrarySettings:
    @pytest.fixture()
    def logo_properties(self) -> dict[str, Any]:
        image_data_raw = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x01\x03\x00\x00\x00%\xdbV\xca\x00\x00\x00\x06PLTE\xffM\x00\x01\x01\x01\x8e\x1e\xe5\x1b\x00\x00\x00\x01tRNS\xcc\xd24V\xfd\x00\x00\x00\nIDATx\x9cc`\x00\x00\x00\x02\x00\x01H\xaf\xa4q\x00\x00\x00\x00IEND\xaeB`\x82"
        image_data_b64_bytes = base64.b64encode(image_data_raw)
        image_data_b64_unicode = image_data_b64_bytes.decode("utf-8")
        data_url = "data:image/png;base64," + image_data_b64_unicode
        image = Image.open(BytesIO(image_data_raw))
        return {
            "raw_bytes": image_data_raw,
            "base64_bytes": image_data_b64_bytes,
            "base64_unicode": image_data_b64_unicode,
            "data_url": data_url,
            "image": image,
        }

    def library_form(
        self, library: Library, fields: dict[str, str | list[str]] | None = None
    ) -> ImmutableMultiDict[str, str]:
        fields = fields or {}
        defaults: dict[str, str | list[str]] = {
            "uuid": str(library.uuid),
            "name": "The New York Public Library",
            "short_name": str(library.short_name),
            "website": "https://library.library/",
            "help_email": "help@example.com",
            "default_notification_email": "email@example.com",
        }
        defaults.update(fields)

        form_data = []
        for k, v in defaults.items():
            if isinstance(v, list):
                for value in v:
                    form_data.append((k, value))
            else:
                form_data.append((k, v))

        form = ImmutableMultiDict(form_data)
        return form

    def test_libraries_get_with_no_libraries(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller: LibrarySettingsController,
        db: DatabaseTransactionFixture,
    ):
        # Delete any existing library created by the controller test setup.
        library = get_one(db.session, Library)
        if library:
            db.session.delete(library)

        with flask_app_fixture.test_request_context_system_admin("/"):
            response = controller.process_get()
            assert isinstance(response.json, dict)
            assert response.json.get("libraries") == []

    def test_libraries_get_with_announcements(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller: LibrarySettingsController,
        db: DatabaseTransactionFixture,
        announcement_fixture: AnnouncementFixture,
    ):
        # Delete any existing library created by the controller test setup.
        library = get_one(db.session, Library)
        if library:
            db.session.delete(library)

        # Set some announcements for this library.
        test_library = db.library("Library 1", "L1")
        a1 = announcement_fixture.active_announcement(db.session, test_library)
        a2 = announcement_fixture.expired_announcement(db.session, test_library)
        a3 = announcement_fixture.forthcoming_announcement(db.session, test_library)

        # When we request information about this library...
        with flask_app_fixture.test_request_context_system_admin("/"):
            response = controller.process_get()
            assert isinstance(response.json, dict)
            library_settings = response.json.get("libraries", [])[0].get("settings")

            # We find out about the library's announcements.
            announcements = library_settings.get(ANNOUNCEMENTS_SETTING_NAME)
            assert [
                str(a2.id),
                str(a1.id),
                str(a3.id),
            ] == [x.get("id") for x in json.loads(announcements)]

            # The objects found in `library_settings` aren't exactly
            # the same as what is stored in the database: string dates
            # can be parsed into datetime.date objects.
            for i in json.loads(announcements):
                assert isinstance(
                    datetime.datetime.strptime(i.get("start"), "%Y-%m-%d"),
                    datetime.date,
                )
                assert isinstance(
                    datetime.datetime.strptime(i.get("finish"), "%Y-%m-%d"),
                    datetime.date,
                )

    def test_libraries_get_with_logo(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller: LibrarySettingsController,
        db: DatabaseTransactionFixture,
        logo_properties: dict[str, Any],
    ):
        library = db.default_library()

        # Give the library a logo
        library.logo = LibraryLogo(content=logo_properties["base64_bytes"])

        # When we request information about this library...
        with flask_app_fixture.test_request_context_system_admin("/"):
            response = controller.process_get()
        assert isinstance(response.json, dict)
        libraries = response.json.get("libraries", [])
        assert len(libraries) == 1
        library_settings = libraries[0].get("settings")

        assert "logo" in library_settings
        assert library_settings["logo"] == logo_properties["data_url"]

    def test_libraries_get_with_multiple_libraries(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller: LibrarySettingsController,
        db: DatabaseTransactionFixture,
        library_fixture: LibraryFixture,
    ):
        # Delete any existing library created by the controller test setup.
        library = get_one(db.session, Library)
        if library:
            db.session.delete(library)

        l1 = library_fixture.library("Library 1", "L1")
        l2 = library_fixture.library("Library 2", "L2")
        l3 = library_fixture.library("Library 3", "L3")

        # L2 has some additional library-wide settings.
        settings = library_fixture.settings(l2)
        settings.featured_lane_size = 5
        settings.facets_default_order = FacetConstants.ORDER_TITLE
        settings.facets_enabled_order = [
            FacetConstants.ORDER_TITLE,
            FacetConstants.ORDER_AUTHOR,
        ]
        settings.large_collection_languages = ["French"]
        l2.update_settings(settings)

        # The admin only has access to L1 and L2.
        admin = flask_app_fixture.admin_user()
        admin.remove_role(AdminRole.SYSTEM_ADMIN)
        admin.add_role(AdminRole.LIBRARIAN, l1)
        admin.add_role(AdminRole.LIBRARY_MANAGER, l2)

        with flask_app_fixture.test_request_context("/", admin=admin):
            response = controller.process_get()
            assert isinstance(response.json, dict)
            libraries = response.json.get("libraries", [])
            assert 2 == len(libraries)

            assert l1.uuid == libraries[0].get("uuid")
            assert l2.uuid == libraries[1].get("uuid")

            assert l1.name == libraries[0].get("name")
            assert l2.name == libraries[1].get("name")

            assert l1.short_name == libraries[0].get("short_name")
            assert l2.short_name == libraries[1].get("short_name")

            assert {
                "website": "http://library.com",
                "help_web": "http://library.com/support",
            } == libraries[0].get("settings")
            assert 6 == len(libraries[1].get("settings").keys())
            settings_dict = libraries[1].get("settings")
            assert 5 == settings_dict.get("featured_lane_size")
            assert FacetConstants.ORDER_TITLE == settings_dict.get(
                "facets_default_order"
            )
            assert [
                FacetConstants.ORDER_TITLE,
                FacetConstants.ORDER_AUTHOR,
            ] == settings_dict.get("facets_enabled_order")
            assert ["fre"] == settings_dict.get("large_collection_languages")

    def test_libraries_post_errors(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller: LibrarySettingsController,
        db: DatabaseTransactionFixture,
    ):
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict([])
            with pytest.raises(ProblemDetailException) as excinfo:
                controller.process_post()
            assert excinfo.value.problem_detail.uri == INCOMPLETE_CONFIGURATION.uri
            assert (
                "Required field 'Name' is missing."
                == excinfo.value.problem_detail.detail
            )

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Brooklyn Public Library"),
                ]
            )
            with pytest.raises(ProblemDetailException) as excinfo:
                controller.process_post()
            assert excinfo.value.problem_detail.uri == INCOMPLETE_CONFIGURATION.uri
            assert (
                "Required field 'Short name' is missing."
                == excinfo.value.problem_detail.detail
            )

        library = db.library()
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = self.library_form(library, {"uuid": "1234"})
            with pytest.raises(ProblemDetailException) as excinfo:
                controller.process_post()
            assert excinfo.value.problem_detail.uri == LIBRARY_NOT_FOUND.uri

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Brooklyn Public Library"),
                    ("short_name", str(library.short_name)),
                ]
            )
            with pytest.raises(ProblemDetailException) as excinfo:
                controller.process_post()

            assert excinfo.value.problem_detail == LIBRARY_SHORT_NAME_ALREADY_IN_USE

        bpl = db.library(short_name="bpl")
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("uuid", str(bpl.uuid)),
                    ("name", "Brooklyn Public Library"),
                    ("short_name", str(library.short_name)),
                ]
            )
            with pytest.raises(ProblemDetailException) as excinfo:
                controller.process_post()
            assert excinfo.value.problem_detail == LIBRARY_SHORT_NAME_ALREADY_IN_USE

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("uuid", str(library.uuid)),
                    ("name", "The New York Public Library"),
                    ("short_name", str(library.short_name)),
                ]
            )
            with pytest.raises(ProblemDetailException) as excinfo:
                controller.process_post()
            assert excinfo.value.problem_detail.uri == INCOMPLETE_CONFIGURATION.uri

        # Either patron support email or website MUST be present
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Email or Website Library"),
                    ("short_name", "Email or Website"),
                    ("website", "http://example.org"),
                    ("default_notification_email_address", "email@example.org"),
                ]
            )
            with pytest.raises(ProblemDetailException) as excinfo:
                controller.process_post()
            assert excinfo.value.problem_detail.uri == INCOMPLETE_CONFIGURATION.uri
            assert excinfo.value.problem_detail.detail is not None
            assert (
                "'Patron support email address' or 'Patron support website'"
                in excinfo.value.problem_detail.detail
            )

        # Test a web primary and secondary color that doesn't contrast
        # well on white. Here primary will, secondary should not.
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = self.library_form(
                library,
                {
                    "web_primary_color": "#000000",
                    "web_secondary_color": "#e0e0e0",
                },
            )
            with pytest.raises(ProblemDetailException) as excinfo:
                controller.process_post()
            assert excinfo.value.problem_detail.uri == INVALID_CONFIGURATION_OPTION.uri
            assert excinfo.value.problem_detail.detail is not None
            assert (
                "contrast-ratio.com/#%23e0e0e0-on-%23ffffff"
                in excinfo.value.problem_detail.detail
            )
            assert (
                "contrast-ratio.com/#%23e0e0e0-on-%23ffffff"
                in excinfo.value.problem_detail.detail
            )

        # Test a list of web header links and a list of labels that
        # aren't the same length.
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = self.library_form(
                library,
                {
                    "web_header_links": [
                        "http://library.com/1",
                        "http://library.com/2",
                    ],
                    "web_header_labels": "One",
                },
            )
            with pytest.raises(ProblemDetailException) as excinfo:
                controller.process_post()
            assert excinfo.value.problem_detail.uri == INVALID_CONFIGURATION_OPTION.uri

        # Test bad language code
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = self.library_form(
                library, {"tiny_collection_languages": "zzz"}
            )
            with pytest.raises(ProblemDetailException) as excinfo:
                controller.process_post()
            assert excinfo.value.problem_detail.uri == UNKNOWN_LANGUAGE.uri
            assert excinfo.value.problem_detail.detail is not None
            assert (
                '"zzz" is not a valid language code'
                in excinfo.value.problem_detail.detail
            )

        # Test uploading a logo that is in the wrong format.
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = self.library_form(library)
            flask.request.files = ImmutableMultiDict(
                {
                    "logo": FileStorage(
                        stream=BytesIO(b"not a png"),
                        content_type="application/pdf",
                        filename="logo.png",
                    )
                }
            )
            with pytest.raises(ProblemDetailException) as excinfo:
                controller.process_post()
            assert excinfo.value.problem_detail.uri == INVALID_CONFIGURATION_OPTION.uri
            assert excinfo.value.problem_detail.detail is not None
            assert (
                "Image upload must be in GIF, PNG, or JPG format."
                in excinfo.value.problem_detail.detail
            )

        # Test uploading a logo that we can't open to resize.
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = self.library_form(library)
            flask.request.files = ImmutableMultiDict(
                {
                    "logo": FileStorage(
                        stream=BytesIO(b"not a png"),
                        content_type="image/png",
                        filename="logo.png",
                    )
                }
            )
            with pytest.raises(ProblemDetailException) as excinfo:
                controller.process_post()
            assert excinfo.value.problem_detail.uri == INVALID_CONFIGURATION_OPTION.uri
            assert excinfo.value.problem_detail.detail is not None
            assert (
                "Unable to open uploaded image" in excinfo.value.problem_detail.detail
            )

    def test__process_image(self, logo_properties: dict[str, Any]):
        image, expected_encoded_image = (
            logo_properties[key] for key in ("image", "base64_bytes")
        )
        processed_image = LibrarySettingsController._process_image(image)
        assert processed_image == expected_encoded_image

        # CMYK should be converted to RGBA
        image = logo_properties["image"].convert("CMYK")
        assert image.mode == "CMYK"

        processed_image = LibrarySettingsController._process_image(image)
        rgba_image = image.convert("RGBA")
        bio = BytesIO()
        rgba_image.save(bio, "PNG")
        assert processed_image == base64.b64encode(bio.getvalue())

    def test_libraries_post_create(
        self,
        logo_properties: dict[str, Any],
        flask_app_fixture: FlaskAppFixture,
        controller: LibrarySettingsController,
        db: DatabaseTransactionFixture,
        announcement_fixture: AnnouncementFixture,
    ):
        # Pull needed properties from logo fixture
        image_data, expected_logo_data_url, image = (
            logo_properties[key] for key in ("raw_bytes", "data_url", "image")
        )
        # LibrarySettingsController scales down images that are too large,
        # so we fail here if our test fixture image is large enough to cause
        # a mismatch between the expected data URL and the one configured.
        assert max(*image.size) <= Configuration.LOGO_MAX_DIMENSION

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "The New York Public Library"),
                    ("short_name", "nypl"),
                    ("library_description", "Short description of library"),
                    ("website", "https://library.library/"),
                    ("tiny_collection_languages", "ger"),
                    (
                        ANNOUNCEMENTS_SETTING_NAME,
                        json.dumps(
                            [
                                AnnouncementData(
                                    content="This is announcement one.",
                                    start=announcement_fixture.today,
                                    finish=announcement_fixture.tomorrow,
                                ).as_dict(),
                                AnnouncementData(
                                    content="This is announcement two.",
                                    start=announcement_fixture.tomorrow,
                                    finish=announcement_fixture.in_a_week,
                                ).as_dict(),
                            ]
                        ),
                    ),
                    (
                        "default_notification_email_address",
                        "email@example.com",
                    ),
                    ("help_email", "help@example.com"),
                    ("featured_lane_size", "5"),
                    (
                        "facets_default_order",
                        FacetConstants.ORDER_RANDOM,
                    ),
                    (
                        "facets_enabled_order" + "_" + FacetConstants.ORDER_TITLE,
                        "",
                    ),
                    (
                        "facets_enabled_order" + "_" + FacetConstants.ORDER_RANDOM,
                        "",
                    ),
                ]
            )
            flask.request.files = ImmutableMultiDict(
                {
                    "logo": FileStorage(
                        stream=BytesIO(image_data),
                        content_type="image/png",
                        filename="logo.png",
                    )
                }
            )
            response = controller.process_post()
            assert response.status_code == 201

        library = get_one(db.session, Library, short_name="nypl")
        assert isinstance(library, Library)
        assert library.uuid == response.get_data(as_text=True)
        assert library.name == "The New York Public Library"
        assert library.short_name == "nypl"
        assert library.settings.featured_lane_size == 5
        assert library.settings.facets_default_order == FacetConstants.ORDER_RANDOM
        assert library.settings.facets_enabled_order == [
            FacetConstants.ORDER_TITLE,
            FacetConstants.ORDER_RANDOM,
        ]
        assert library.logo is not None
        assert expected_logo_data_url == library.logo.data_url

        # Make sure public and private key were generated and stored.
        assert library.private_key is not None
        assert library.public_key is not None
        assert "BEGIN PUBLIC KEY" in library.public_key
        private_key = import_key(library.private_key)
        assert isinstance(private_key, RsaKey)
        public_key = import_key(library.public_key)
        assert isinstance(public_key, RsaKey)
        expected_public = private_key.public_key().export_key().decode("utf-8")
        assert library.public_key == expected_public

        # Announcements were validated and the result was written to the database, such that we can
        # parse it as a list of Announcement objects.
        announcements = (
            db.session.execute(Announcement.library_announcements(library))
            .scalars()
            .all()
        )
        assert [
            "This is announcement one.",
            "This is announcement two.",
        ] == [x.content for x in announcements]
        assert all(isinstance(x, Announcement) for x in announcements)

        # When the library was created, default lanes were also created
        # according to its language setup. This library has one tiny
        # collection (not a good choice for a real library), so only
        # two lanes were created: "Other Languages" and then "German"
        # underneath it.
        [german, other_languages] = sorted(library.lanes, key=lambda x: x.display_name)
        assert None == other_languages.parent
        assert ["ger"] == other_languages.languages
        assert other_languages == german.parent
        assert ["ger"] == german.languages

    def test_libraries_post_edit(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller: LibrarySettingsController,
        db: DatabaseTransactionFixture,
        library_fixture: LibraryFixture,
    ):
        # A library already exists.
        settings = library_fixture.mock_settings()
        settings.featured_lane_size = 5
        settings.facets_default_order = FacetConstants.ORDER_RANDOM
        settings.facets_enabled_order = [
            FacetConstants.ORDER_TITLE,
            FacetConstants.ORDER_RANDOM,
        ]
        library_to_edit = library_fixture.library(
            "New York Public Library", "nypl", settings
        )
        library_to_edit.logo = LibraryLogo(content=b"A tiny image")
        library_fixture.reset_settings_cache(library_to_edit)

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("uuid", str(library_to_edit.uuid)),
                    ("name", "The New York Public Library"),
                    ("short_name", "nypl"),
                    ("featured_lane_size", "20"),
                    ("minimum_featured_quality", "0.9"),
                    ("website", "https://library.library/"),
                    (
                        "default_notification_email_address",
                        "email@example.com",
                    ),
                    ("help_email", "help@example.com"),
                    (
                        "facets_default_order",
                        FacetConstants.ORDER_AUTHOR,
                    ),
                    (
                        "facets_enabled_order" + "_" + FacetConstants.ORDER_AUTHOR,
                        "",
                    ),
                    (
                        "facets_enabled_order" + "_" + FacetConstants.ORDER_RANDOM,
                        "",
                    ),
                ]
            )
            response = controller.process_post()
            assert response.status_code == 200

        library = get_one(db.session, Library, uuid=library_to_edit.uuid)

        assert library is not None
        assert library.uuid == response.get_data(as_text=True)
        assert library.name == "The New York Public Library"
        assert library.short_name == "nypl"

        # The library-wide settings were updated.
        assert library.settings.website == "https://library.library"
        assert (
            library.settings.default_notification_email_address == "email@example.com"
        )
        assert library.settings.help_email == "help@example.com"
        assert library.settings.featured_lane_size == 20
        assert library.settings.minimum_featured_quality == 0.9
        assert library.settings.facets_default_order == FacetConstants.ORDER_AUTHOR
        assert library.settings.facets_enabled_order == [
            FacetConstants.ORDER_AUTHOR,
            FacetConstants.ORDER_RANDOM,
        ]

        # The library-wide logo was not updated and has been left alone.
        assert library.logo.content == b"A tiny image"

    def test_library_post_empty_values_edit(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller: LibrarySettingsController,
        db: DatabaseTransactionFixture,
        library_fixture: LibraryFixture,
    ):
        settings = library_fixture.mock_settings()
        settings.library_description = "description"
        library_to_edit = library_fixture.library(
            "New York Public Library", "nypl", settings
        )
        library_fixture.reset_settings_cache(library_to_edit)

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("uuid", str(library_to_edit.uuid)),
                    ("name", "The New York Public Library"),
                    ("short_name", "nypl"),
                    ("library_description", ""),  # empty value
                    ("website", "https://library.library/"),
                    ("help_email", "help@example.com"),
                ]
            )
            response = controller.process_post()
            assert response.status_code == 200

        library = get_one(db.session, Library, uuid=library_to_edit.uuid)
        assert library is not None
        assert library.settings.library_description is None

    def test_library_post_empty_values_create(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller: LibrarySettingsController,
        db: DatabaseTransactionFixture,
    ):
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "The New York Public Library"),
                    ("short_name", "nypl"),
                    ("library_description", ""),  # empty value
                    ("website", "https://library.library/"),
                    ("help_email", "help@example.com"),
                ]
            )
            response: Response = controller.process_post()
            assert response.status_code == 201
            uuid = response.get_data(as_text=True)

        library = get_one(db.session, Library, uuid=uuid)
        assert library is not None
        assert library.settings.library_description is None

    def test_library_delete(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller: LibrarySettingsController,
        db: DatabaseTransactionFixture,
    ):
        library = db.library()

        with flask_app_fixture.test_request_context("/", method="DELETE"):
            pytest.raises(
                AdminNotAuthorized,
                controller.process_delete,
                library.uuid,
            )

        with flask_app_fixture.test_request_context_system_admin("/", method="DELETE"):
            response = controller.process_delete(str(library.uuid))
            assert response.status_code == 200

        queried_library = get_one(db.session, Library, uuid=library.uuid)
        assert queried_library is None

    def test_process_libraries(
        self, flask_app_fixture: FlaskAppFixture, controller: LibrarySettingsController
    ):
        mock_process_get = create_autospec(controller.process_get)
        controller.process_get = mock_process_get
        mock_process_post = create_autospec(controller.process_post)
        controller.process_post = mock_process_post

        # Make sure we call process_get for a get request
        with flask_app_fixture.test_request_context("/", method="GET"):
            controller.process_libraries()

        mock_process_get.assert_called_once()
        mock_process_post.assert_not_called()
        mock_process_get.reset_mock()
        mock_process_post.reset_mock()

        # Make sure we call process_post for a post request
        with flask_app_fixture.test_request_context("/", method="POST"):
            controller.process_libraries()

        mock_process_get.assert_not_called()
        mock_process_post.assert_called_once()
        mock_process_get.reset_mock()
        mock_process_post.reset_mock()

        # For any other request, make sure we return a ProblemDetail
        with flask_app_fixture.test_request_context("/", method="PUT"):
            resp = controller.process_libraries()

        mock_process_get.assert_not_called()
        mock_process_post.assert_not_called()
        assert isinstance(resp, ProblemDetail)

        # Make sure that if process_get or process_post raises a ProblemError,
        # we return the problem detail.
        mock_process_get.side_effect = ProblemDetailException(
            problem_detail=INCOMPLETE_CONFIGURATION.detailed("test")
        )
        with flask_app_fixture.test_request_context("/", method="GET"):
            resp = controller.process_libraries()
        assert isinstance(resp, ProblemDetail)
        assert resp.detail == "test"
