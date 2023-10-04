import json
from typing import Any, Dict, List, Optional

import flask
from flask import Response
from flask_babel import lazy_gettext as _

from api.admin.controller.settings import SettingsController
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
    PROTOCOL_DOES_NOT_SUPPORT_SETTINGS,
    UNKNOWN_PROTOCOL,
)
from api.integration.registry.license_providers import LicenseProvidersRegistry
from core.model import (
    Collection,
    ConfigurationSetting,
    Library,
    get_one,
    get_one_or_create,
)
from core.model.admin import Admin
from core.model.integration import IntegrationConfiguration
from core.util.problem_detail import ProblemDetail, ProblemError


class CollectionSettingsController(SettingsController):
    def __init__(self, manager):
        super().__init__(manager)
        self.type = _("collection")
        self.registry = LicenseProvidersRegistry()

    def _get_collection_protocols(self):
        protocols = super()._get_collection_protocols(self.registry.integrations)

        # dedupe and only keep the latest SETTINGS
        # this will allow child objects to overwrite
        # parent settings with the same key
        # This relies on the fact that child settings
        # are added after parent settings as such
        # `SETTINGS + <configuration>.to_settings()`
        for protocol in protocols:
            if "settings" not in protocol:
                continue
            _found_settings = dict()
            for ix, setting in enumerate(protocol["settings"]):
                _key = setting["key"]
                _found_settings[_key] = ix
            _settings = []
            # Go through the dict items and only use the latest found settings
            # for any given key
            for _, v in _found_settings.items():
                _settings.append(protocol["settings"][v])
            protocol["settings"] = _settings

        return protocols

    def process_collections(self):
        if flask.request.method == "GET":
            return self.process_get()
        else:
            return self.process_post()

    # GET
    def process_get(self):
        collections_db = self._db.query(Collection).order_by(Collection.name).all()
        ConfigurationSetting.cache_warm(self._db)
        Collection.cache_warm(self._db, lambda: collections_db)
        protocols = self._get_collection_protocols()
        user = flask.request.admin
        collections = []
        collection_object: Collection
        for collection_object in collections_db:
            if not user or not user.can_see_collection(collection_object):
                continue

            collection_dict = self.collection_to_dict(collection_object)
            if collection_object.integration_configuration:
                libraries = self.load_libraries(collection_object, user)
                collection_dict["libraries"] = libraries
                collection_dict[
                    "settings"
                ] = collection_object.integration_configuration.settings_dict
                self.load_settings(collection_object, collection_dict["settings"])
            collection_dict["self_test_results"] = self._get_prior_test_results(
                collection_object
            )
            collection_dict[
                "marked_for_deletion"
            ] = collection_object.marked_for_deletion

            collections.append(collection_dict)

        return dict(
            collections=collections,
            protocols=protocols,
        )

    def collection_to_dict(self, collection_object):
        return dict(
            id=collection_object.id,
            name=collection_object.name,
            protocol=collection_object.protocol,
            parent_id=collection_object.parent_id,
        )

    def load_libraries(self, collection_object: Collection, user: Admin) -> List[Dict]:
        """Get a list of the libraries that 1) are associated with this collection
        and 2) the user is affiliated with"""

        libraries = []
        integration: IntegrationConfiguration = (
            collection_object.integration_configuration
        )
        if not integration:
            return []
        for library in collection_object.libraries:
            if not user or not user.is_librarian(library):
                continue
            library_info = dict(short_name=library.short_name)
            # Find and update the library settings if they exist
            for config in integration.library_configurations:
                if library.id == config.library_id:
                    library_info.update(config.settings_dict)
                    break
            libraries.append(library_info)

        return libraries

    def load_settings(self, collection_object, collection_settings):
        """Compile the information about the collection that corresponds to the settings
        externally imposed by the collection's protocol."""

        settings = collection_settings
        settings["external_account_id"] = collection_object.external_account_id

    def find_protocol_class(self, collection_object):
        """Figure out which class this collection's protocol belongs to, from the list
        of possible protocols defined in the registry"""

        return self.registry.get(collection_object.protocol)

    # POST
    def process_post(self):
        self.require_system_admin()
        protocols = self._get_collection_protocols()
        is_new = False
        collection = None

        name = flask.request.form.get("name")
        protocol_name = flask.request.form.get("protocol")
        parent_id = flask.request.form.get("parent_id")
        fields = {"name": name, "protocol": protocol_name}
        id = flask.request.form.get("id")
        if id:
            collection = get_one(self._db, Collection, id=id)
            fields["collection"] = collection

        error = self.validate_form_fields(is_new, protocols, **fields)
        if error:
            return error

        settings_class = self._get_settings_class(
            self.registry, protocol_name, is_child=(parent_id is not None)
        )
        if not settings_class:
            return UNKNOWN_PROTOCOL

        if protocol_name and not collection:
            collection, is_new = get_one_or_create(self._db, Collection, name=name)
            if not is_new:
                self._db.rollback()
                return COLLECTION_NAME_ALREADY_IN_USE
            collection.create_integration_configuration(protocol_name)
            # Mirrors still use the external integration
            # TODO: Remove the use of external integrations when Mirrors are migrated
            # to use the integration configurations
            collection.create_external_integration(protocol_name)

        collection.name = name
        [protocol_dict] = [p for p in protocols if p.get("name") == protocol_name]

        valid = self.validate_parent(protocol_dict, collection)
        if isinstance(valid, ProblemDetail):
            self._db.rollback()
            return valid

        settings = protocol_dict["settings"]
        settings_error = self.process_settings(settings, collection)
        if settings_error:
            self._db.rollback()
            return settings_error

        libraries_error = self.process_libraries(protocol_dict, collection)
        if libraries_error:
            return libraries_error

        if is_new:
            return Response(str(collection.id), 201)
        else:
            return Response(str(collection.id), 200)

    def validate_form_fields(self, is_new, protocols, **fields):
        """Check that 1) the required fields aren't blank, 2) the protocol is on the
        list of recognized protocols, 3) the collection (if there is one) is valid, and
        4) the URL is valid"""
        if not fields.get("name"):
            return MISSING_COLLECTION_NAME
        if "collection" in fields:
            if fields.get("collection"):
                invalid_collection = self.validate_collection(**fields)
                if invalid_collection:
                    return invalid_collection
            else:
                return MISSING_COLLECTION
        if fields.get("protocol"):
            if fields.get("protocol") not in [p.get("name") for p in protocols]:
                return UNKNOWN_PROTOCOL
        else:
            return NO_PROTOCOL_FOR_NEW_SERVICE

    def validate_collection(self, **fields):
        """The protocol of an existing collection cannot be changed, and
        collections must have unique names."""
        if fields.get("protocol") != fields.get("collection").protocol:
            return CANNOT_CHANGE_PROTOCOL
        if fields.get("name") != fields.get("collection").name:
            collection_with_name = get_one(
                self._db, Collection, name=fields.get("name")
            )
            if collection_with_name:
                return COLLECTION_NAME_ALREADY_IN_USE

    def validate_parent(self, protocol, collection):
        """Verify that the parent collection is set properly, then determine
        the type of the settings that need to be validated: are they 1) settings for a
        regular collection (e.g. client key and client secret for an Overdrive collection),
        or 2) settings for a child collection (e.g. library ID for an Overdrive Advantage collection)?
        """

        parent_id = flask.request.form.get("parent_id")
        if parent_id and not protocol.get("child_settings"):
            return PROTOCOL_DOES_NOT_SUPPORT_PARENTS
        if parent_id:
            parent = get_one(self._db, Collection, id=parent_id)
            if not parent:
                return MISSING_PARENT
            collection.parent = parent
        else:
            collection.parent = None

        return True

    def validate_external_account_id_setting(self, value, setting):
        """Check that the user has submitted any required values for associating
        this collection with an external account."""
        if not value and not setting.get("optional"):
            # Roll back any changes to the collection that have already been made.
            return INCOMPLETE_CONFIGURATION.detailed(
                _(
                    "The collection configuration is missing a required setting: %(setting)s",
                    setting=setting.get("label"),
                )
            )

    def process_settings(
        self, settings: List[Dict[str, Any]], collection: Collection
    ) -> Optional[ProblemDetail]:
        """Process the settings for the given collection.

        Go through the settings that the user has just submitted for this collection,
        and check that each setting is valid and that no required settings are missing.  If
        the setting passes all of the validations, go ahead and set it for this collection.
        """
        settings_class = self._get_settings_class(
            self.registry,
            collection.protocol,
            is_child=(flask.request.form.get("parent_id") is not None),
        )
        if isinstance(settings_class, ProblemDetail):
            return settings_class
        if settings_class is None:
            return PROTOCOL_DOES_NOT_SUPPORT_SETTINGS
        collection_settings = {}
        for setting in settings:
            key = setting["key"]
            value = self._extract_form_setting_value(setting, flask.request.form)
            if key == "external_account_id":
                error = self.validate_external_account_id_setting(value, setting)
                if error:
                    return error
                collection.external_account_id = value
            elif value is not None:
                # Only if the key was present in the request should we add it
                collection_settings[key] = value
            else:
                # Keep existing setting value, when present, if a value is not specified.
                # This can help prevent accidental loss of settings due to some programming errors.
                if key in collection.integration_configuration.settings_dict:
                    collection_settings[
                        key
                    ] = collection.integration_configuration.settings_dict[key]

        # validate then apply
        try:
            validated_settings = settings_class(**collection_settings)
        except ProblemError as ex:
            return ex.problem_detail
        collection.integration_configuration.settings_dict = validated_settings.dict()
        return None

    def process_libraries(self, protocol, collection):
        """Go through the libraries that the user is trying to associate with this collection;
        check that each library actually exists, and that the library-related configuration settings
        that the user has submitted are complete and valid.  If the library passes all of the validations,
        go ahead and associate it with this collection."""

        libraries = []
        protocol_class = self.registry.get(protocol["name"])
        if flask.request.form.get("libraries"):
            libraries = json.loads(flask.request.form.get("libraries"))

        for library_info in libraries:
            library = get_one(
                self._db, Library, short_name=library_info.get("short_name")
            )
            if not library:
                return NO_SUCH_LIBRARY.detailed(
                    _(
                        "You attempted to add the collection to %(library_short_name)s, but the library does not exist.",
                        library_short_name=library_info.get("short_name"),
                    )
                )
            if collection not in library.collections:
                library.collections.append(collection)
            result = self._set_configuration_library(
                collection.integration_configuration, library_info, protocol_class
            )
            if isinstance(result, ProblemDetail):
                return result

        short_names = [l.get("short_name") for l in libraries]
        for library in collection.libraries:
            if library.short_name not in short_names:
                collection.disassociate_library(library)

    # DELETE
    def process_delete(self, collection_id):
        self.require_system_admin()
        collection = get_one(self._db, Collection, id=collection_id)
        if not collection:
            return MISSING_COLLECTION
        if len(collection.children) > 0:
            return CANNOT_DELETE_COLLECTION_WITH_CHILDREN

        # Flag the collection to be deleted by script in the background.
        collection.marked_for_deletion = True
        return Response(str(_("Deleted")), 200)
