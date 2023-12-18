import json
import logging
import os

from flask_babel import lazy_gettext as _
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import ArgumentError

# It's convenient for other modules import IntegrationException
# from this module, alongside CannotLoadConfiguration.
from core.exceptions import IntegrationException
from core.util import LanguageCodes, ansible_boolean
from core.util.datetime_helpers import to_utc, utc_now


class CannotLoadConfiguration(IntegrationException):
    """The current configuration of an external integration, or of the
    site as a whole, is in an incomplete or inconsistent state.

    This is more specific than a base IntegrationException because it
    assumes the problem is evident just by looking at the current
    configuration, with no need to actually talk to the foreign
    server.
    """


class ConfigurationConstants:
    TRUE = "true"
    FALSE = "false"


class Configuration(ConfigurationConstants):
    log = logging.getLogger("Configuration file loader")

    # Environment variables that contain URLs to the database
    DATABASE_TEST_ENVIRONMENT_VARIABLE = "SIMPLIFIED_TEST_DATABASE"
    DATABASE_PRODUCTION_ENVIRONMENT_VARIABLE = "SIMPLIFIED_PRODUCTION_DATABASE"

    # TODO: We can remove this variable once basic token authentication is fully deployed.
    # Patron token authentication enabled switch.
    BASIC_TOKEN_AUTH_ENABLED_ENVVAR = "SIMPLIFIED_ENABLE_BASIC_TOKEN_AUTH"

    # Environment variables for Firebase Cloud Messaging (FCM) service account key
    FCM_CREDENTIALS_FILE_ENVIRONMENT_VARIABLE = "SIMPLIFIED_FCM_CREDENTIALS_FILE"
    FCM_CREDENTIALS_JSON_ENVIRONMENT_VARIABLE = "SIMPLIFIED_FCM_CREDENTIALS_JSON"

    # Environment variable for Overdrive fulfillment keys
    OD_PREFIX_PRODUCTION_PREFIX = "SIMPLIFIED"
    OD_PREFIX_TESTING_PREFIX = "SIMPLIFIED_TESTING"
    OD_FULFILLMENT_CLIENT_KEY_SUFFIX = "OVERDRIVE_FULFILLMENT_CLIENT_KEY"
    OD_FULFILLMENT_CLIENT_SECRET_SUFFIX = "OVERDRIVE_FULFILLMENT_CLIENT_SECRET"

    # Quicksight
    # Comma separated aws arns
    QUICKSIGHT_AUTHORIZED_ARNS_KEY = "QUICKSIGHT_AUTHORIZED_ARNS"

    # Environment variable for SirsiDynix Auth
    SIRSI_DYNIX_APP_ID = "SIMPLIFIED_SIRSI_DYNIX_APP_ID"

    # Environment variable for temporary reporting email
    REPORTING_EMAIL_ENVIRONMENT_VARIABLE = "SIMPLIFIED_REPORTING_EMAIL"

    # Environment variable for used to distinguish one CM environment from another in reports
    REPORTING_NAME_ENVIRONMENT_VARIABLE = "PALACE_REPORTING_NAME"

    # ConfigurationSetting key for the base url of the app.
    BASE_URL_KEY = "base_url"

    # ConfigurationSetting to enable the MeasurementReaper script
    MEASUREMENT_REAPER = "measurement_reaper_enabled"

    # Configuration key for push notifications status
    PUSH_NOTIFICATIONS_STATUS = "push_notifications_status"

    # Integrations
    URL = "url"
    INTEGRATIONS = "integrations"

    DEFAULT_MINIMUM_FEATURED_QUALITY = 0.65

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"

    # The default value to put into the 'app' field of JSON-format logs,
    # unless LOG_APP_NAME overrides it.
    DEFAULT_APP_NAME = "simplified"

    # Settings for the integration with protocol=INTERNAL_LOGGING
    LOG_LEVEL = "log_level"
    LOG_APP_NAME = "log_app"
    DATABASE_LOG_LEVEL = "database_log_level"
    LOG_LEVEL_UI = [
        {"key": DEBUG, "label": _("Debug")},
        {"key": INFO, "label": _("Info")},
        {"key": WARN, "label": _("Warn")},
        {"key": ERROR, "label": _("Error")},
    ]

    EXCLUDED_AUDIO_DATA_SOURCES = "excluded_audio_data_sources"

    SITEWIDE_SETTINGS = [
        {
            "key": BASE_URL_KEY,
            "label": _("Base url of the application"),
            "required": True,
            "format": "url",
        },
        {
            "key": LOG_LEVEL,
            "label": _("Log Level"),
            "type": "select",
            "options": LOG_LEVEL_UI,
            "default": INFO,
        },
        {
            "key": LOG_APP_NAME,
            "label": _("Application name"),
            "description": _(
                "Log messages originating from this application will be tagged with this name. If you run multiple instances, giving each one a different application name will help you determine which instance is having problems."
            ),
            "default": DEFAULT_APP_NAME,
            "required": True,
        },
        {
            "key": DATABASE_LOG_LEVEL,
            "label": _("Database Log Level"),
            "type": "select",
            "options": LOG_LEVEL_UI,
            "description": _(
                "Database logs are extremely verbose, so unless you're diagnosing a database-related problem, it's a good idea to set a higher log level for database messages."
            ),
            "default": WARN,
        },
        {
            "key": EXCLUDED_AUDIO_DATA_SOURCES,
            "label": _("Excluded audiobook sources"),
            "description": _(
                "Audiobooks from these data sources will be hidden from the collection, even if they would otherwise show up as available."
            ),
            "default": None,
            "required": True,
        },
        {
            "key": MEASUREMENT_REAPER,
            "label": _("Cleanup old measurement data"),
            "type": "select",
            "description": _(
                "If this settings is 'true' old book measurement data will be cleaned out of the database. Some sites may want to keep this data for later analysis."
            ),
            "options": {"true": "true", "false": "false"},
            "default": "true",
        },
        {
            "key": PUSH_NOTIFICATIONS_STATUS,
            "label": _("Push notifications status"),
            "type": "select",
            "description": _(
                "If this settings is 'true' push notification jobs will run as scheduled, and attempt to notify patrons via mobile push notifications."
            ),
            "options": [
                {"key": ConfigurationConstants.TRUE, "label": _("True")},
                {"key": ConfigurationConstants.FALSE, "label": _("False")},
            ],
            "default": ConfigurationConstants.TRUE,
        },
    ]

    @classmethod
    def database_url(cls):
        """Find the database URL configured for this site.

        For compatibility with old configurations, we will look in the
        site configuration first.

        If it's not there, we will look in the appropriate environment
        variable.
        """

        # To avoid expensive mistakes, test and production databases
        # are always configured with separate keys. The TESTING variable
        # controls which database is used, and it's set by the
        # package_setup() function called in every component's
        # tests/__init__.py.
        test = os.environ.get("TESTING", False)
        if test:
            environment_variable = cls.DATABASE_TEST_ENVIRONMENT_VARIABLE
        else:
            environment_variable = cls.DATABASE_PRODUCTION_ENVIRONMENT_VARIABLE

        url = os.environ.get(environment_variable)
        if not url:
            raise CannotLoadConfiguration(
                "Database URL was not defined in environment variable (%s)."
                % environment_variable
            )

        url_obj = None
        try:
            url_obj = make_url(url)
        except ArgumentError as e:
            # Improve the error message by giving a guide as to what's
            # likely to work.
            raise ArgumentError(
                "Bad format for database URL (%s). Expected something like postgresql://[username]:[password]@[hostname]:[port]/[database name]"
                % url
            )

        # Calling __to_string__ will hide the password.
        logging.info("Connecting to database: %s" % url_obj.__to_string__())
        return url

    # TODO: We can remove this method once basic token authentication is fully deployed.
    @classmethod
    def basic_token_auth_is_enabled(cls) -> bool:
        """Is basic token authentication enabled?

        Return False, if the variable is unset or is an empty string.
        Raises CannotLoadConfiguration, if the setting is invalid.
        :raise CannotLoadConfiguration: If the setting contains an unsupported value.
        """
        try:
            return ansible_boolean(
                os.environ.get(cls.BASIC_TOKEN_AUTH_ENABLED_ENVVAR),
                label=cls.BASIC_TOKEN_AUTH_ENABLED_ENVVAR,
                default=False,
            )
        except (TypeError, ValueError) as e:
            raise CannotLoadConfiguration(
                f"Invalid value for {cls.BASIC_TOKEN_AUTH_ENABLED_ENVVAR} environment variable."
            ) from e

    @classmethod
    def fcm_credentials(cls) -> dict[str, str]:
        """Returns a dictionary containing Firebase Cloud Messaging credentials.

        Credentials are provided as a JSON string, either (1) directly in an environment
        variable or (2) in a file that is specified in another environment variable.
        """
        config_json = os.environ.get(cls.FCM_CREDENTIALS_JSON_ENVIRONMENT_VARIABLE, "")
        config_file = os.environ.get(cls.FCM_CREDENTIALS_FILE_ENVIRONMENT_VARIABLE, "")
        if not config_json and not config_file:
            raise CannotLoadConfiguration(
                "FCM Credentials configuration environment variable not defined. "
                f"Use either '{cls.FCM_CREDENTIALS_JSON_ENVIRONMENT_VARIABLE}' "
                f"or '{cls.FCM_CREDENTIALS_FILE_ENVIRONMENT_VARIABLE}'."
            )
        if config_json and config_file:
            raise CannotLoadConfiguration(
                f"Both JSON ('{cls.FCM_CREDENTIALS_JSON_ENVIRONMENT_VARIABLE}') "
                f"and file-based ('{cls.FCM_CREDENTIALS_FILE_ENVIRONMENT_VARIABLE}') "
                "FCM Credential environment variables are defined, but only one is allowed."
            )
        if config_json:
            try:
                return json.loads(config_json, strict=False)
            except:
                raise CannotLoadConfiguration(
                    "Cannot parse value of FCM credential environment variable "
                    f"'{cls.FCM_CREDENTIALS_JSON_ENVIRONMENT_VARIABLE}' as JSON."
                )

        # If we make it this far, we are dealing with a configuration file.
        if not os.path.exists(config_file):
            raise FileNotFoundError(
                f"The FCM credentials file ('{config_file}') does not exist."
            )
        with open(config_file) as f:
            try:
                return json.load(f)
            except:
                raise CannotLoadConfiguration(
                    f"Cannot parse contents of FCM credentials file ('{config_file}') as JSON."
                )

    @classmethod
    def overdrive_fulfillment_keys(cls, testing=False) -> dict[str, str]:
        prefix = (
            cls.OD_PREFIX_TESTING_PREFIX if testing else cls.OD_PREFIX_PRODUCTION_PREFIX
        )
        key = os.environ.get(f"{prefix}_{cls.OD_FULFILLMENT_CLIENT_KEY_SUFFIX}")
        secret = os.environ.get(f"{prefix}_{cls.OD_FULFILLMENT_CLIENT_SECRET_SUFFIX}")
        if key is None or secret is None:
            raise CannotLoadConfiguration("Missing fulfillment credentials.")
        if not key:
            raise CannotLoadConfiguration("Invalid fulfillment credentials.")
        return {"key": key, "secret": secret}

    @classmethod
    def quicksight_authorized_arns(cls) -> dict[str, list[str]]:
        """Split the comma separated arns"""
        arns_str = os.environ.get(cls.QUICKSIGHT_AUTHORIZED_ARNS_KEY, "")
        return json.loads(arns_str)

    @classmethod
    def localization_languages(cls):
        return [LanguageCodes.three_to_two["eng"]]

    # The last time the database configuration is known to have changed.
    SITE_CONFIGURATION_LAST_UPDATE = None

    # The last time we *checked* whether the database configuration had
    # changed.
    LAST_CHECKED_FOR_SITE_CONFIGURATION_UPDATE = None

    # A sitewide configuration setting controlling *how often* to check
    # whether the database configuration has changed.
    #
    # NOTE: This setting is currently not used; the most reliable
    # value seems to be zero. Assuming that's true, this whole
    # subsystem can be removed.
    SITE_CONFIGURATION_TIMEOUT = "site_configuration_timeout"

    # The name of the service associated with a Timestamp that tracks
    # the last time the site's configuration changed in the database.
    SITE_CONFIGURATION_CHANGED = "Site Configuration Changed"

    @classmethod
    def last_checked_for_site_configuration_update(cls):
        """When was the last time we actually checked when the database
        was updated?
        """
        return cls.LAST_CHECKED_FOR_SITE_CONFIGURATION_UPDATE

    @classmethod
    def site_configuration_last_update(cls, _db, known_value=None, timeout=0):
        """Check when the site configuration was last updated.

        Updates Configuration.instance[Configuration.SITE_CONFIGURATION_LAST_UPDATE].
        It's the application's responsibility to periodically check
        this value and reload the configuration if appropriate.

        :param known_value: We know when the site configuration was
            last updated--it's this timestamp. Use it instead of checking
            with the database.

        :param timeout: We will only call out to the database once in
            this number of seconds. If we are asked again before this
            number of seconds elapses, we will assume site
            configuration has not changed. By default, we call out to
            the database every time.

        :return: a datetime object.

        """

        now = utc_now()

        # NOTE: Currently we never check the database (because timeout is
        # never set to None). This code will hopefully be removed soon.
        if _db and timeout is None:
            from core.model import ConfigurationSetting

            timeout = ConfigurationSetting.sitewide(
                _db, cls.SITE_CONFIGURATION_TIMEOUT
            ).int_value

        if timeout is None:
            # NOTE: this only happens if timeout is explicitly set to
            # None _and_ no database value is present. Right now that
            # never happens because timeout is never explicitly set to
            # None.
            timeout = 60

        last_check = cls.LAST_CHECKED_FOR_SITE_CONFIGURATION_UPDATE

        if (
            not known_value
            and last_check
            and (now - last_check).total_seconds() < timeout
        ):
            # We went to the database less than [timeout] seconds ago.
            # Assume there has been no change.
            return cls._site_configuration_last_update()

        # Ask the database when was the last time the site
        # configuration changed. Specifically, this is the last time
        # site_configuration_was_changed() (defined in model.py) was
        # called.
        if not known_value:
            from core.model import Timestamp

            known_value = Timestamp.value(
                _db, cls.SITE_CONFIGURATION_CHANGED, service_type=None, collection=None
            )
        if not known_value:
            # The site configuration has never changed.
            last_update = None
        else:
            last_update = known_value

        # Update the Configuration object's record of the last update time.
        cls.SITE_CONFIGURATION_LAST_UPDATE = last_update

        # Whether that record changed or not, the time at which we
        # _checked_ is going to be set to the current time.
        cls.LAST_CHECKED_FOR_SITE_CONFIGURATION_UPDATE = now
        return last_update

    @classmethod
    def _site_configuration_last_update(cls):
        """Get the raw SITE_CONFIGURATION_LAST_UPDATE value,
        without any attempt to find a fresher value from the database.
        """
        last_update = cls.SITE_CONFIGURATION_LAST_UPDATE
        if last_update:
            last_update = to_utc(last_update)
        return last_update


class ConfigurationTrait:
    """An abstract class that denotes a configuration mixin/trait. Configuration
    traits should subclass this class in order to make implementations easy to find
    in IDEs."""
