from __future__ import annotations

import logging
import os
from enum import Enum

from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import ArgumentError

# It's convenient for other modules import IntegrationException
# from this module, alongside CannotLoadConfiguration.
from core.exceptions import IntegrationException
from core.util import LanguageCodes
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


class ConfigurationAttributeValue(Enum):
    """Enumeration of common configuration attribute values"""

    YESVALUE = "yes"
    NOVALUE = "no"


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

    # Environment variable for temporary reporting email
    REPORTING_EMAIL_ENVIRONMENT_VARIABLE = "SIMPLIFIED_REPORTING_EMAIL"

    # Environment variable for used to distinguish one CM environment from another in reports
    REPORTING_NAME_ENVIRONMENT_VARIABLE = "PALACE_REPORTING_NAME"

    # Integrations
    URL = "url"
    INTEGRATIONS = "integrations"

    DEFAULT_MINIMUM_FEATURED_QUALITY = 0.65

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"

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
