from __future__ import annotations

import base64
import json
import uuid
from io import BytesIO

import flask
from flask import Response
from flask_babel import lazy_gettext as _
from PIL import Image, UnidentifiedImageError
from PIL.Image import Resampling
from werkzeug.datastructures import FileStorage

from api.admin.announcement_list_validator import AnnouncementListValidator
from api.admin.controller.base import AdminPermissionsControllerMixin
from api.admin.form_data import ProcessFormData
from api.admin.problem_details import *
from api.circulation_manager import CirculationManager
from api.config import Configuration
from api.lanes import create_default_lanes
from core.configuration.library import LibrarySettings
from core.model import (
    Library,
    Representation,
    create,
    get_one,
    json_serializer,
    site_configuration_has_changed,
)
from core.model.announcements import SETTING_NAME as ANNOUNCEMENT_SETTING_NAME
from core.model.announcements import Announcement
from core.model.library import LibraryLogo
from core.util.problem_detail import ProblemDetail, ProblemError


class LibrarySettingsController(AdminPermissionsControllerMixin):
    def __init__(self, manager: CirculationManager):
        self._db = manager._db

    def process_libraries(self) -> Response | ProblemDetail:
        try:
            if flask.request.method == "GET":
                return self.process_get()
            elif flask.request.method == "POST":
                return self.process_post()
            else:
                return INCOMPLETE_CONFIGURATION
        except ProblemError as e:
            self._db.rollback()
            return e.problem_detail

    def process_get(self) -> Response:
        libraries_response = []
        libraries = self._db.query(Library).order_by(Library.name).all()

        for library in libraries:
            # Only include libraries this admin has librarian access to.
            if not flask.request.admin or not flask.request.admin.is_librarian(library):  # type: ignore[attr-defined]
                continue

            settings = library.settings_dict

            # TODO: It would be nice to make this more sane in the future, but right now the admin interface
            #  is expecting the "announcements" field to be a JSON string, within the JSON document we send,
            #  so it ends up being double-encoded. This is a quick fix to make it work without modifying the
            #  admin interface.
            db_announcements = (
                self._db.execute(Announcement.library_announcements(library))
                .scalars()
                .all()
            )
            announcements = [x.to_data().as_dict() for x in db_announcements]
            if announcements:
                settings["announcements"] = json.dumps(announcements)

            if library.logo:
                settings["logo"] = library.logo.data_url

            libraries_response += [
                dict(
                    uuid=library.uuid,
                    name=library.name,
                    short_name=library.short_name,
                    settings=settings,
                )
            ]
        return Response(
            json_serializer(
                {
                    "libraries": libraries_response,
                    "settings": LibrarySettings.configuration_form(self._db),
                }
            ),
            status=200,
            mimetype="application/json",
        )

    def process_post(self) -> Response:
        is_new = False
        form_data = flask.request.form

        library_uuid = form_data.get("uuid")
        if library_uuid:
            library = self.get_library_from_uuid(library_uuid)
        else:
            library = None

        name = form_data.get("name", "").strip()
        if name == "":
            raise ProblemError(
                problem_detail=INCOMPLETE_CONFIGURATION.detailed(
                    "Required field 'Name' is missing."
                )
            )

        short_name = form_data.get("short_name", "").strip()
        if short_name == "":
            raise ProblemError(
                problem_detail=INCOMPLETE_CONFIGURATION.detailed(
                    "Required field 'Short name' is missing."
                )
            )

        self.check_short_name_unique(library, short_name)
        validated_settings = ProcessFormData.get_settings(LibrarySettings, form_data)

        if not library:
            # Everyone can modify an existing library, but only a system admin can create a new one.
            self.require_system_admin()
            library, is_new = self.create_library(short_name)

        library.name = name
        library.short_name = short_name
        library.settings_dict = validated_settings.dict()

        # Validate and scale logo
        self.scale_and_store_logo(library, flask.request.files.get("logo"))

        if ANNOUNCEMENT_SETTING_NAME in flask.request.form:
            validated_announcements = (
                AnnouncementListValidator().validate_announcements(
                    flask.request.form[ANNOUNCEMENT_SETTING_NAME]
                )
            )
            existing_announcements = (
                self._db.execute(Announcement.library_announcements(library))
                .scalars()
                .all()
            )
            Announcement.sync(
                self._db, existing_announcements, validated_announcements, library
            )

        # Trigger a site configuration change
        site_configuration_has_changed(self._db)

        if is_new:
            # Now that the configuration settings are in place, create
            # a default set of lanes.
            create_default_lanes(self._db, library)
            return Response(str(library.uuid), 201)
        else:
            return Response(str(library.uuid), 200)

    def create_library(self, short_name: str) -> tuple[Library, bool]:
        self.require_system_admin()
        public_key, private_key = Library.generate_keypair()
        library, is_new = create(
            self._db,
            Library,
            short_name=short_name,
            uuid=str(uuid.uuid4()),
            public_key=public_key,
            private_key=private_key,
        )
        return library, is_new

    def process_delete(self, library_uuid: str) -> Response:
        self.require_system_admin()
        library = self.get_library_from_uuid(library_uuid)
        self._db.delete(library)
        return Response(str(_("Deleted")), 200)

    def get_library_from_uuid(self, library_uuid: str) -> Library:
        # Library UUID is required when editing an existing library
        # from the admin interface, and isn't present for new libraries.
        library = get_one(
            self._db,
            Library,
            uuid=library_uuid,
        )
        if library:
            return library
        else:
            raise ProblemError(
                problem_detail=LIBRARY_NOT_FOUND.detailed(
                    _("The specified library uuid does not exist.")
                )
            )

    def check_short_name_unique(
        self, library: Library | None, short_name: str | None
    ) -> None:
        if not library or (short_name and short_name != library.short_name):
            # If you're adding a new short_name, either by editing an
            # existing library or creating a new library, it must be unique.
            library_with_short_name = get_one(self._db, Library, short_name=short_name)
            if library_with_short_name:
                raise ProblemError(problem_detail=LIBRARY_SHORT_NAME_ALREADY_IN_USE)

    @staticmethod
    def _process_image(image: Image.Image, _format: str = "PNG") -> bytes:
        """Convert PIL image to RGBA if necessary and return it
        as base64 encoded bytes.
        """
        buffer = BytesIO()
        # If the image is not RGB, RGBA or P convert it
        # https://pillow.readthedocs.io/en/stable/handbook/concepts.html#modes
        if image.mode not in ("RGB", "RGBA", "P"):
            image = image.convert("RGBA")
        image.save(buffer, format=_format)
        return base64.b64encode(buffer.getvalue())

    @classmethod
    def scale_and_store_logo(
        cls,
        library: Library,
        image_file: FileStorage | None,
        max_dimension: int = Configuration.LOGO_MAX_DIMENSION,
    ) -> None:
        if not image_file:
            return None

        allowed_types = [
            Representation.JPEG_MEDIA_TYPE,
            Representation.PNG_MEDIA_TYPE,
            Representation.GIF_MEDIA_TYPE,
        ]
        image_type = image_file.headers.get("Content-Type")
        if image_type not in allowed_types:
            raise ProblemError(
                INVALID_CONFIGURATION_OPTION.detailed(
                    f"Image upload must be in GIF, PNG, or JPG format. (Upload was {image_type}.)"
                )
            )

        try:
            image = Image.open(image_file.stream)
        except UnidentifiedImageError:
            raise ProblemError(
                INVALID_CONFIGURATION_OPTION.detailed(
                    f"Unable to open uploaded image, please try again or upload a different image."
                )
            )
        width, height = image.size
        if width > max_dimension or height > max_dimension:
            image.thumbnail((max_dimension, max_dimension), Resampling.LANCZOS)

        image_data = cls._process_image(image)
        if library.logo:
            library.logo.content = image_data
        else:
            library.logo = LibraryLogo(content=image_data)
