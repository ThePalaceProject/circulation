import logging
import os
from typing import Dict

from flask_babel import lazy_gettext as _
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import ArgumentError

# It's convenient for other modules import IntegrationException
# from this module, alongside CannotLoadConfiguration.
from core.exceptions import IntegrationException

from .entrypoint import EntryPoint
from .facets import FacetConstants
from .util import LanguageCodes
from .util.datetime_helpers import to_utc, utc_now


class CannotLoadConfiguration(IntegrationException):
    """The current configuration of an external integration, or of the
    site as a whole, is in an incomplete or inconsistent state.

    This is more specific than a base IntegrationException because it
    assumes the problem is evident just by looking at the current
    configuration, with no need to actually talk to the foreign
    server.
    """


class ConfigurationConstants:

    # Each facet group has two associated per-library keys: one
    # configuring which facets are enabled for that facet group, and
    # one configuring which facet is the default.
    ENABLED_FACETS_KEY_PREFIX = "facets_enabled_"
    DEFAULT_FACET_KEY_PREFIX = "facets_default_"

    # The "level" property determines which admins will be able to modify the setting.  Level 1 settings can be modified by anyone.
    # Level 2 settings can be modified only by library managers and system admins (i.e. not by librarians).  Level 3 settings can be changed only by system admins.
    # If no level is specified, the setting will be treated as Level 1 by default.
    ALL_ACCESS = 1
    SYS_ADMIN_OR_MANAGER = 2
    SYS_ADMIN_ONLY = 3


class Configuration(ConfigurationConstants):

    log = logging.getLogger("Configuration file loader")

    # Environment variables that contain URLs to the database
    DATABASE_TEST_ENVIRONMENT_VARIABLE = "SIMPLIFIED_TEST_DATABASE"
    DATABASE_PRODUCTION_ENVIRONMENT_VARIABLE = "SIMPLIFIED_PRODUCTION_DATABASE"

    # Environment variable for Overdrive fulfillment keys
    OD_PREFIX_PRODUCTION_PREFIX = "SIMPLIFIED"
    OD_PREFIX_TESTING_PREFIX = "SIMPLIFIED_TESTING"
    OD_FULFILLMENT_CLIENT_KEY_SUFFIX = "OVERDRIVE_FULFILLMENT_CLIENT_KEY"
    OD_FULFILLMENT_CLIENT_SECRET_SUFFIX = "OVERDRIVE_FULFILLMENT_CLIENT_SECRET"

    # Environment variable for SirsiDynix Auth
    SIRSI_DYNIX_APP_ID = "SIMPLIFIED_SIRSI_DYNIX_APP_ID"

    # ConfigurationSetting key for the base url of the app.
    BASE_URL_KEY = "base_url"

    # ConfigurationSetting to enable the MeasurementReaper script
    MEASUREMENT_REAPER = "measurement_reaper_enabled"

    # Lane policies
    DEFAULT_OPDS_FORMAT = "verbose_opds_entry"

    # Integrations
    URL = "url"
    INTEGRATIONS = "integrations"

    # The name of the per-library configuration policy that controls whether
    # books may be put on hold.
    ALLOW_HOLDS = "allow_holds"

    # Each library may set a minimum quality for the books that show
    # up in the 'featured' lanes that show up on the front page.
    MINIMUM_FEATURED_QUALITY = "minimum_featured_quality"
    DEFAULT_MINIMUM_FEATURED_QUALITY = 0.65

    # Each library may configure the maximum number of books in the
    # 'featured' lanes.
    FEATURED_LANE_SIZE = "featured_lane_size"

    WEBSITE_URL = "website"
    NAME = "name"
    SHORT_NAME = "short_name"

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
    ]

    LIBRARY_SETTINGS = (
        [
            {
                "key": NAME,
                "label": _("Name"),
                "description": _("The human-readable name of this library."),
                "category": "Basic Information",
                "level": ConfigurationConstants.SYS_ADMIN_ONLY,
                "required": True,
            },
            {
                "key": SHORT_NAME,
                "label": _("Short name"),
                "description": _(
                    "A short name of this library, to use when identifying it in scripts or URLs, e.g. 'NYPL'."
                ),
                "category": "Basic Information",
                "level": ConfigurationConstants.SYS_ADMIN_ONLY,
                "required": True,
            },
            {
                "key": WEBSITE_URL,
                "label": _("URL of the library's website"),
                "description": _(
                    "The library's main website, e.g. \"https://www.nypl.org/\" (not this Circulation Manager's URL)."
                ),
                "required": True,
                "format": "url",
                "level": ConfigurationConstants.SYS_ADMIN_ONLY,
                "category": "Basic Information",
            },
            {
                "key": ALLOW_HOLDS,
                "label": _("Allow books to be put on hold"),
                "type": "select",
                "options": [
                    {"key": "true", "label": _("Allow holds")},
                    {"key": "false", "label": _("Disable holds")},
                ],
                "default": "true",
                "category": "Loans, Holds, & Fines",
                "level": ConfigurationConstants.SYS_ADMIN_ONLY,
            },
            {
                "key": EntryPoint.ENABLED_SETTING,
                "label": _("Enabled entry points"),
                "description": _(
                    "Patrons will see the selected entry points at the top level and in search results. <p>Currently supported audiobook vendors: Bibliotheca, Axis 360"
                ),
                "type": "list",
                "options": [
                    {
                        "key": entrypoint.INTERNAL_NAME,
                        "label": EntryPoint.DISPLAY_TITLES.get(entrypoint),
                    }
                    for entrypoint in EntryPoint.ENTRY_POINTS
                ],
                "default": [x.INTERNAL_NAME for x in EntryPoint.DEFAULT_ENABLED],
                "category": "Lanes & Filters",
                # Renders a component with options that get narrowed down as the user makes selections.
                "format": "narrow",
                # Renders an input field that cannot be edited.
                "readOnly": True,
                "level": ConfigurationConstants.SYS_ADMIN_ONLY,
            },
            {
                "key": FEATURED_LANE_SIZE,
                "label": _("Maximum number of books in the 'featured' lanes"),
                "type": "number",
                "default": 15,
                "category": "Lanes & Filters",
                "level": ConfigurationConstants.ALL_ACCESS,
            },
            {
                "key": MINIMUM_FEATURED_QUALITY,
                "label": _(
                    "Minimum quality for books that show up in 'featured' lanes"
                ),
                "description": _("Between 0 and 1."),
                "type": "number",
                "max": 1,
                "default": DEFAULT_MINIMUM_FEATURED_QUALITY,
                "category": "Lanes & Filters",
                "level": ConfigurationConstants.ALL_ACCESS,
            },
        ]
        + [
            {
                "key": ConfigurationConstants.ENABLED_FACETS_KEY_PREFIX + group,
                "label": description,
                "type": "list",
                "options": [
                    {
                        "key": facet,
                        "label": FacetConstants.FACET_DISPLAY_TITLES.get(facet),
                    }
                    for facet in FacetConstants.FACETS_BY_GROUP.get(group, [])
                ],
                "default": FacetConstants.FACETS_BY_GROUP.get(group),
                "category": "Lanes & Filters",
                # Tells the front end that each of these settings is related to the corresponding default setting.
                "paired": ConfigurationConstants.DEFAULT_FACET_KEY_PREFIX + group,
                "level": ConfigurationConstants.SYS_ADMIN_OR_MANAGER,
            }
            for group, description in FacetConstants.GROUP_DESCRIPTIONS.items()
        ]
        + [
            {
                "key": ConfigurationConstants.DEFAULT_FACET_KEY_PREFIX + group,
                "label": _("Default %(group)s", group=display_name),
                "type": "select",
                "options": [
                    {
                        "key": facet,
                        "label": FacetConstants.FACET_DISPLAY_TITLES.get(facet),
                    }
                    for facet in FacetConstants.FACETS_BY_GROUP.get(group, [])
                ],
                "default": FacetConstants.DEFAULT_FACET.get(group),
                "category": "Lanes & Filters",
                "skip": True,
            }
            for group, display_name in FacetConstants.GROUP_DISPLAY_TITLES.items()
        ]
    )

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

    @classmethod
    def overdrive_fulfillment_keys(cls, testing=False) -> Dict[str, str]:
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
            from .model import ConfigurationSetting

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
            from .model import Timestamp

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
