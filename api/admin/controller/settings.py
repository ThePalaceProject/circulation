from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

import flask
from flask import Response
from flask_babel import lazy_gettext as _

from api.admin.controller.base import AdminPermissionsControllerMixin
from api.admin.problem_details import (
    CANNOT_CHANGE_PROTOCOL,
    DUPLICATE_INTEGRATION,
    INCOMPLETE_CONFIGURATION,
    INTEGRATION_NAME_ALREADY_IN_USE,
    INTEGRATION_URL_ALREADY_IN_USE,
    INVALID_CONFIGURATION_OPTION,
    MISSING_SERVICE,
    NO_PROTOCOL_FOR_NEW_SERVICE,
    NO_SUCH_LIBRARY,
    PROTOCOL_DOES_NOT_SUPPORT_PARENTS,
    UNKNOWN_PROTOCOL,
)
from api.admin.validator import Validator
from api.controller.circulation_manager import CirculationManagerController
from core.integration.base import (
    HasChildIntegrationConfiguration,
    HasIntegrationConfiguration,
    HasLibraryIntegrationConfiguration,
)
from core.integration.registry import IntegrationRegistry
from core.integration.settings import BaseSettings
from core.model import (
    ConfigurationSetting,
    ExternalIntegration,
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
    Library,
    create,
    get_one,
    get_one_or_create,
)
from core.util.problem_detail import ProblemDetail

if TYPE_CHECKING:
    from werkzeug.datastructures import ImmutableMultiDict


class SettingsController(CirculationManagerController, AdminPermissionsControllerMixin):
    METADATA_SERVICE_URI_TYPE = "application/opds+json;profile=https://librarysimplified.org/rel/profile/metadata-service"

    NO_MIRROR_INTEGRATION = "NO_MIRROR"

    def _get_settings_class(
        self, registry: IntegrationRegistry, protocol_name: str, is_child=False
    ) -> type[BaseSettings] | ProblemDetail | None:
        api_class = registry.get(protocol_name)
        if not api_class:
            return None

        if is_child and issubclass(api_class, HasChildIntegrationConfiguration):
            return api_class.child_settings_class()
        elif is_child:
            return PROTOCOL_DOES_NOT_SUPPORT_PARENTS

        return api_class.settings_class()

    def _get_integration_protocols(
        self, provider_apis, protocol_name_attr="__module__"
    ):
        protocols = []
        _db = self._db
        for api in provider_apis:
            is_integration = issubclass(api, HasIntegrationConfiguration)
            protocol = dict()
            name = (
                getattr(api, protocol_name_attr) if not is_integration else api.label()
            )
            protocol["name"] = name

            label = getattr(api, "NAME", name)
            protocol["label"] = label

            description = (
                getattr(api, "DESCRIPTION", None)
                if not is_integration
                else api.description()
            )
            if description != None:
                protocol["description"] = description

            instructions = getattr(api, "INSTRUCTIONS", None)
            if instructions != None:
                protocol["instructions"] = instructions

            sitewide = getattr(api, "SITEWIDE", None)
            if sitewide != None:
                protocol["sitewide"] = sitewide

            settings = getattr(api, "SETTINGS", [])
            protocol["settings"] = list(settings)
            if _db and issubclass(api, HasIntegrationConfiguration):
                protocol["settings"] = api.settings_class().configuration_form(_db)

            if issubclass(api, HasChildIntegrationConfiguration):
                protocol[
                    "child_settings"
                ] = api.child_settings_class().configuration_form(_db)

            library_settings = getattr(api, "LIBRARY_SETTINGS", None)
            if library_settings != None:
                protocol["library_settings"] = list(library_settings)

            if _db and issubclass(api, HasLibraryIntegrationConfiguration):
                protocol[
                    "library_settings"
                ] = api.library_settings_class().configuration_form(_db)

            cardinality = getattr(api, "CARDINALITY", None)
            if cardinality != None:
                protocol["cardinality"] = cardinality

            supports_registration = getattr(api, "SUPPORTS_REGISTRATION", None)
            if supports_registration != None:
                protocol["supports_registration"] = supports_registration
            supports_staging = getattr(api, "SUPPORTS_STAGING", None)
            if supports_staging != None:
                protocol["supports_staging"] = supports_staging

            protocols.append(protocol)
        return protocols

    def _get_integration_library_info(self, integration, library, protocol):
        library_info = dict(short_name=library.short_name)
        for setting in protocol.get("library_settings", []):
            key = setting.get("key")
            if setting.get("type") == "list":
                value = ConfigurationSetting.for_library_and_externalintegration(
                    self._db, key, library, integration
                ).json_value
            else:
                value = ConfigurationSetting.for_library_and_externalintegration(
                    self._db, key, library, integration
                ).value
            if value:
                library_info[key] = value
        return library_info

    def _get_integration_info(self, goal, protocols):
        services = []
        settings_query = (
            self._db.query(ConfigurationSetting)
            .join(ExternalIntegration)
            .filter(ExternalIntegration.goal == goal)
        )
        ConfigurationSetting.cache_warm(self._db, settings_query.all)
        for service in (
            self._db.query(ExternalIntegration)
            .filter(ExternalIntegration.goal == goal)
            .order_by(ExternalIntegration.name)
        ):
            candidates = [p for p in protocols if p.get("name") == service.protocol]
            if not candidates:
                continue
            protocol = candidates[0]
            libraries = []
            if not protocol.get("sitewide") or protocol.get("library_settings"):
                for library in service.libraries:
                    libraries.append(
                        self._get_integration_library_info(service, library, protocol)
                    )

            settings = dict()
            for setting in protocol.get("settings", []):
                key = setting.get("key")
                if setting.get("type") in ("list", "menu"):
                    value = ConfigurationSetting.for_externalintegration(
                        key, service
                    ).json_value
                else:
                    value = ConfigurationSetting.for_externalintegration(
                        key, service
                    ).value
                settings[key] = value

            service_info = dict(
                id=service.id,
                name=service.name,
                protocol=service.protocol,
                settings=settings,
                libraries=libraries,
            )

            if "test_search_term" in [x.get("key") for x in protocol.get("settings")]:
                service_info["self_test_results"] = self._get_prior_test_results(
                    service
                )

            services.append(service_info)
        return services

    @staticmethod
    def _get_menu_values(setting_key, form):
        """circulation-admin returns "menu" values in a different format not compatible with werkzeug.MultiDict semantics:
            {setting_key}_{menu} = {value_in_the_dropdown_box}
            {setting_key}_{setting_value1} = {setting_label1}
            {setting_key}_{setting_value2} = {setting_label2}
            ...
            {setting_key}_{setting_valueN} = {setting_labelN}

        It means we can't use werkzeug.MultiDict.getlist method and have to extract them manually.

        :param setting_key: Setting's key
        :type setting_key: str

        :param form: Multi-dictionary containing input values submitted by the user
            and sent back to CM by circulation-admin
        :type form: werkzeug.MultiDict

        :return: List of "menu" values
        :rtype: List[str]
        """
        values = []

        for form_item_key in list(form.keys()):
            if setting_key in form_item_key:
                value = form_item_key.replace(setting_key, "").lstrip("_")

                if value != "menu":
                    values.append(value)

        return values

    def _extract_form_setting_value(
        self, setting: dict[str, Any], form_data: ImmutableMultiDict
    ) -> Any | None:
        """Extract the value of a setting from form data."""

        key = setting.get("key")
        setting_type = setting.get("type")

        value: Any | None
        if setting_type == "list" and not setting.get("options"):
            value = [item for item in form_data.getlist(key) if item]
        elif setting_type == "menu":
            value = self._get_menu_values(key, form_data)
        else:
            value = form_data.get(key)
        return value

    def _set_integration_setting(self, integration, setting):
        value = self._extract_form_setting_value(setting, flask.request.form)
        if value and setting.get("type") == "list":
            value = json.dumps(value)

        if value and setting.get("options"):
            # This setting can only take on values that are in its
            # list of options.
            allowed_values = [option.get("key") for option in setting.get("options")]
            submitted_values = value

            if not isinstance(submitted_values, list):
                submitted_values = [submitted_values]

            for submitted_value in submitted_values:
                if submitted_value not in allowed_values:
                    return INVALID_CONFIGURATION_OPTION.detailed(
                        _(
                            "The configuration value for %(setting)s is invalid.",
                            setting=setting.get("label"),
                        )
                    )

        value_missing = value is None
        value_required = setting.get("required")

        if value_missing and value_required:
            value_default = setting.get("default")
            if not value_default:
                return INCOMPLETE_CONFIGURATION.detailed(
                    _(
                        "The configuration is missing a required setting: %(setting)s",
                        setting=setting.get("label"),
                    )
                )

        if isinstance(value, list):
            value = json.dumps(value)

        integration.setting(setting.get("key")).value = value

    def _set_configuration_library(
        self,
        configuration: IntegrationConfiguration,
        library_info: dict,
        protocol_class: type[HasLibraryIntegrationConfiguration],
    ) -> IntegrationLibraryConfiguration:
        """Set the library configuration for the integration configuration.
        The data will be validated first."""
        # We copy the data so we can remove unwanted keys like "short_name"
        info_copy = library_info.copy()
        library = get_one(self._db, Library, short_name=info_copy.pop("short_name"))
        if not library:
            raise RuntimeError("Could not find the configuration library")

        # Validate first
        validated_data = protocol_class.library_settings_class()(**info_copy)

        # Attach the configuration
        library_configuration = IntegrationLibraryConfiguration(
            library=library, settings_dict=validated_data.dict()
        )
        configuration.library_configurations.append(library_configuration)
        return library_configuration

    def _set_integration_library(self, integration, library_info, protocol):
        library = get_one(self._db, Library, short_name=library_info.get("short_name"))
        if not library:
            return NO_SUCH_LIBRARY.detailed(
                _(
                    "You attempted to add the integration to %(library_short_name)s, but it does not exist.",
                    library_short_name=library_info.get("short_name"),
                )
            )

        integration.libraries += [library]
        for setting in protocol.get("library_settings", []):
            key = setting.get("key")
            value = library_info.get(key)
            if value and setting.get("type") == "list" and not setting.get("options"):
                value = json.dumps(value)
            if setting.get("options") and value not in [
                option.get("key") for option in setting.get("options")
            ]:
                return INVALID_CONFIGURATION_OPTION.detailed(
                    _(
                        "The configuration value for %(setting)s is invalid.",
                        setting=setting.get("label"),
                    )
                )
            if not value and setting.get("required"):
                return INCOMPLETE_CONFIGURATION.detailed(
                    _(
                        "The configuration is missing a required setting: %(setting)s for library %(library)s",
                        setting=setting.get("label"),
                        library=library.short_name,
                    )
                )
            ConfigurationSetting.for_library_and_externalintegration(
                self._db, key, library, integration
            ).value = value

    def _set_integration_settings_and_libraries(self, integration, protocol):
        settings = protocol.get("settings")
        for setting in settings:
            if not setting.get("key").endswith("mirror_integration_id"):
                result = self._set_integration_setting(integration, setting)
                if isinstance(result, ProblemDetail):
                    return result

        if not protocol.get("sitewide") or protocol.get("library_settings"):
            integration.libraries = []

            libraries = []
            if flask.request.form.get("libraries"):
                libraries = json.loads(flask.request.form.get("libraries"))

            for library_info in libraries:
                result = self._set_integration_library(
                    integration, library_info, protocol
                )
                if isinstance(result, ProblemDetail):
                    return result
        return True

    def _delete_integration(self, integration_id, goal):
        if flask.request.method != "DELETE":
            return
        self.require_system_admin()

        integration = get_one(
            self._db, ExternalIntegration, id=integration_id, goal=goal
        )
        if not integration:
            return MISSING_SERVICE
        self._db.delete(integration)
        return Response(str(_("Deleted")), 200)

    def _get_collection_protocols(self, provider_apis):
        protocols = self._get_integration_protocols(
            provider_apis, protocol_name_attr="NAME"
        )

        return protocols

    def _get_prior_test_results(self, item, protocol_class=None, *extra_args):
        # :param item: An ExternalSearchIndex, an ExternalIntegration for patron authentication, or a Collection
        if not protocol_class and hasattr(self, "protocol_class"):
            protocol_class = self.protocol_class

        if not item:
            return None

        self_test_results = None

        try:
            if self.type == "metadata service" and protocol_class:
                self_test_results = protocol_class.prior_test_results(
                    self._db, *extra_args
                )

        except Exception as e:
            # This is bad, but not so bad that we should short-circuit
            # this whole process -- that might prevent an admin from
            # making the configuration changes necessary to fix
            # this problem.
            message = _("Exception getting self-test results for %s %s: %s")
            error_message = str(e)
            args = (self.type, item.name, error_message)
            logging.warning(message, *args, exc_info=error_message)
            self_test_results = dict(exception=message % args)

        return self_test_results

    def _create_integration(self, protocol_definitions, protocol, goal):
        """Create a new ExternalIntegration for the given protocol and
        goal, assuming that doing so is compatible with the protocol's
        definition.

        :return: A 2-tuple (result, is_new). `result` will be an
            ExternalIntegration if one could be created, and a
            ProblemDetail otherwise.
        """
        if not protocol:
            return NO_PROTOCOL_FOR_NEW_SERVICE, False
        matches = [x for x in protocol_definitions if x.get("name") == protocol]
        if not matches:
            return UNKNOWN_PROTOCOL, False
        definition = matches[0]

        # Most of the time there can be multiple ExternalIntegrations with
        # the same protocol and goal...
        allow_multiple = True
        m = create
        args = (self._db, ExternalIntegration)
        kwargs = dict(protocol=protocol, goal=goal)
        if definition.get("cardinality") == 1:
            # ...but not all the time.
            allow_multiple = False
            existing = get_one(*args, **kwargs)
            if existing is not None:
                # We were asked to create a new ExternalIntegration
                # but there's already one for this protocol, which is not
                # allowed.
                return DUPLICATE_INTEGRATION, False
            m = get_one_or_create

        integration, is_new = m(*args, **kwargs)
        if not is_new and not allow_multiple:
            # This can happen, despite our check above, in a race
            # condition where two clients try simultaneously to create
            # two integrations of the same type.
            return DUPLICATE_INTEGRATION, False
        return integration, is_new

        [protocol] = [p for p in protocols if p.get("name") == protocol]
        result = self._set_integration_settings_and_libraries(auth_service, protocol)
        if isinstance(result, ProblemDetail):
            return result

    def check_name_unique(self, new_service, name):
        """A service cannot be created with, or edited to have, the same name
        as a service that already exists.
        This method is used by analytics_services, cdn_services, discovery_services,
        metadata_services, and sitewide_services.
        """

        existing_service = get_one(self._db, ExternalIntegration, name=name)
        if existing_service and not existing_service.id == new_service.id:
            # Without checking that the IDs are different, you can't save
            # changes to an existing service unless you've also changed its name.
            return INTEGRATION_NAME_ALREADY_IN_USE

    @classmethod
    def url_variants(cls, url, check_protocol_variant=True):
        """Generate minor variants of a URL -- HTTP vs HTTPS, trailing slash
        vs not, etc.

        Technically these are all distinct URLs, but in real life they
        generally mean someone typed the same URL slightly
        differently. Since this isn't an exact science, this doesn't
        need to catch all variant URLs, only the most common ones.
        """
        if not Validator()._is_url(url, []):
            # An invalid URL has no variants.
            return

        # A URL is a 'variant' of itself.
        yield url

        # Adding or removing a slash creates a variant.
        if url.endswith("/"):
            yield url[:-1]
        else:
            yield url + "/"

        # Changing protocols may create one or more variants.
        https = "https://"
        http = "http://"
        if check_protocol_variant:
            protocol_variant = None
            if url.startswith(https):
                protocol_variant = url.replace(https, http, 1)
            elif url.startswith(http):
                protocol_variant = url.replace(http, https, 1)
            if protocol_variant:
                yield from cls.url_variants(protocol_variant, False)

    def check_url_unique(self, new_service, url, protocol, goal):
        """Enforce a rule that a given circulation manager can only have
        one integration that uses a given URL for a certain purpose.

        Whether to enforce this rule for a given type of integration
        is up to you -- it's a good general rule but there are
        conceivable exceptions.

        This method is used by discovery_services.
        """
        if not url:
            return

        # Look for the given URL as well as minor variations.
        #
        # We can't use urlparse to ignore minor differences in URLs
        # because we're doing the comparison in the database.
        urls = list(self.url_variants(url))

        qu = (
            self._db.query(ExternalIntegration)
            .join(ExternalIntegration.settings)
            .filter(
                # Protocol must match.
                ExternalIntegration.protocol
                == protocol
            )
            .filter(
                # Goal must match.
                ExternalIntegration.goal
                == goal
            )
            .filter(ConfigurationSetting.key == ExternalIntegration.URL)
            .filter(
                # URL must be one of the URLs we're concerned about.
                ConfigurationSetting.value.in_(urls)
            )
            .filter(
                # But don't count the service we're trying to edit.
                ExternalIntegration.id
                != new_service.id
            )
        )
        if qu.count() > 0:
            return INTEGRATION_URL_ALREADY_IN_USE

    def look_up_service_by_id(self, id, protocol, goal=None):
        """Find an existing service, and make sure that the user is not trying to edit
        its protocol.
        This method is used by analytics_services, cdn_services, metadata_services,
        and sitewide_services.
        """

        if not goal:
            goal = self.goal

        service = get_one(self._db, ExternalIntegration, id=id, goal=goal)
        if not service:
            return MISSING_SERVICE
        if protocol and (protocol != service.protocol):
            return CANNOT_CHANGE_PROTOCOL
        return service

    def set_protocols(self, service, protocol, protocols=None):
        """Validate the protocol that the user has submitted; depending on whether
        the validations pass, either save it to this metadata service or
        return an error message.
        This method is used by analytics_services, cdn_services, discovery_services,
        metadata_services, and sitewide_services.
        """

        if not protocols:
            protocols = self.protocols

        [protocol] = [p for p in protocols if p.get("name") == protocol]
        result = self._set_integration_settings_and_libraries(service, protocol)
        if isinstance(result, ProblemDetail):
            return result

    def validate_protocol(self, protocols=None):
        protocols = protocols or self.protocols
        if flask.request.form.get("protocol") not in [p.get("name") for p in protocols]:
            return UNKNOWN_PROTOCOL

    def _get_settings(self):
        if hasattr(self, "protocols"):
            [protocol] = [
                p
                for p in self.protocols
                if p.get("name") == flask.request.form.get("protocol")
            ]
            return protocol.get("settings")
        return []

    def validate_formats(self, settings=None, validator=None):
        # If the service has self.protocols set, we can extract the list of settings here;
        # otherwise, the settings have to be passed in as an argument--either a list or
        # a string.
        validator = validator or Validator()
        settings = settings or self._get_settings()
        form = flask.request.form or None
        try:
            files = flask.request.files
        except:
            files = None
        error = validator.validate(settings, dict(form=form, files=files))
        if error:
            return error
