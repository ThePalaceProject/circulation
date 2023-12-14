from collections.abc import Iterable

from Crypto.Cipher import PKCS1_OAEP
from Crypto.Cipher.PKCS1_OAEP import PKCS1OAEP_Cipher
from Crypto.PublicKey import RSA
from flask_babel import lazy_gettext as _
from money import Money

from core.config import CannotLoadConfiguration  # noqa: autoflake
from core.config import IntegrationException  # noqa: autoflake
from core.config import Configuration as CoreConfiguration
from core.configuration.library import LibrarySettings
from core.model import Library
from core.model.announcements import SETTING_NAME as ANNOUNCEMENT_SETTING_NAME
from core.util import MoneyUtility


class Configuration(CoreConfiguration):
    # The list of patron web urls allowed to access this CM
    PATRON_WEB_HOSTNAMES = "patron_web_hostnames"

    # The name of the sitewide secret used to sign cookies for admin login.
    SECRET_KEY = "secret_key"

    # The name of the setting that controls how long static files are cached.
    STATIC_FILE_CACHE_TIME = "static_file_cache_time"

    # The name of the setting controlling how long authentication
    # documents are cached.
    AUTHENTICATION_DOCUMENT_CACHE_TIME = "authentication_document_cache_time"

    # A custom link to a Terms of Service document to be understood by
    # users of the administrative interface.
    #
    # This is _not_ the end-user terms of service for SimplyE or any
    # other mobile client. The default value links to the terms of
    # service for a library's inclusion in the SimplyE library
    # registry.
    CUSTOM_TOS_HREF = "tos_href"
    DEFAULT_TOS_HREF = "https://thepalaceproject.org/terms-of-service/"

    # Custom text for the link defined in CUSTOM_TOS_LINK.
    CUSTOM_TOS_TEXT = "tos_text"
    DEFAULT_TOS_TEXT = (
        "Terms of Service for presenting content through the Palace client applications"
    )

    # Name of the site-wide ConfigurationSetting containing the secret
    # used to sign bearer tokens.
    BEARER_TOKEN_SIGNING_SECRET = "bearer_token_signing_secret"

    # Maximum height and width for the saved logo image
    LOGO_MAX_DIMENSION = 135

    # A library with this many titles in a given language will be given
    # a large, detailed lane configuration for that language.
    LARGE_COLLECTION_CUTOFF = 10000
    # A library with this many titles in a given language will be
    # given separate fiction and nonfiction lanes for that language.
    SMALL_COLLECTION_CUTOFF = 500
    # A library with fewer titles than that will be given a single
    # lane containing all books in that language.

    # Features of an OPDS client which a library may want to enable or
    # disable.
    RESERVATIONS_FEATURE = "https://librarysimplified.org/rel/policy/reservations"

    SITEWIDE_SETTINGS = CoreConfiguration.SITEWIDE_SETTINGS + [
        {
            "key": BEARER_TOKEN_SIGNING_SECRET,
            "label": _("Internal signing secret for OAuth and SAML bearer tokens"),
            "required": True,
        },
        {
            "key": SECRET_KEY,
            "label": _("Internal secret key for admin interface cookies"),
            "required": True,
        },
        {
            "key": PATRON_WEB_HOSTNAMES,
            "label": _("Hostnames for web application access"),
            "required": True,
            "description": _(
                "Only web applications from these hosts can access this circulation manager. This can be a single hostname (http://catalog.library.org) or a pipe-separated list of hostnames (http://catalog.library.org|https://beta.library.org). You must include the scheme part of the URI (http:// or https://). You can also set this to '*' to allow access from any host, but you must not do this in a production environment -- only during development."
            ),
        },
        {
            "key": STATIC_FILE_CACHE_TIME,
            "label": _(
                "Cache time for static images and JS and CSS files (in seconds)"
            ),
            "required": True,
            "type": "number",
        },
        {
            "key": AUTHENTICATION_DOCUMENT_CACHE_TIME,
            "label": _("Cache time for authentication documents (in seconds)"),
            "required": True,
            "type": "number",
            "default": 0,
        },
        {
            "key": CUSTOM_TOS_HREF,
            "label": _("Custom Terms of Service link"),
            "required": False,
            "default": DEFAULT_TOS_HREF,
            "description": _(
                "If your inclusion in the SimplyE mobile app is governed by terms other than the default, put the URL to those terms in this link so that librarians will have access to them. This URL will be used for all libraries on this circulation manager."
            ),
        },
        {
            "key": CUSTOM_TOS_TEXT,
            "label": _("Custom Terms of Service link text"),
            "required": False,
            "default": DEFAULT_TOS_TEXT,
            "description": _(
                "Custom text for the Terms of Service link in the footer of these administrative interface pages. This is primarily useful if you're not connecting this circulation manager to the SimplyE mobile app. This text will be used for all libraries on this circulation manager."
            ),
        },
    ]

    ANNOUNCEMENT_SETTINGS = [
        {
            "key": ANNOUNCEMENT_SETTING_NAME,
            "label": _("Scheduled announcements"),
            "description": _(
                "Announcements will be displayed to authenticated patrons."
            ),
            "category": "Announcements",
            "type": "announcements",
        },
    ]

    @classmethod
    def estimate_language_collections_when_unset(cls, library: Library) -> None:
        settings = library.settings
        if (
            settings.large_collection_languages is None
            and settings.small_collection_languages is None
            and settings.tiny_collection_languages is None
        ):
            cls.estimate_language_collections_for_library(library)

    @classmethod
    def large_collection_languages(cls, library: Library) -> list[str]:
        cls.estimate_language_collections_when_unset(library)
        if library.settings.large_collection_languages is None:
            return []
        return library.settings.large_collection_languages

    @classmethod
    def small_collection_languages(cls, library: Library) -> list[str]:
        cls.estimate_language_collections_when_unset(library)
        if library.settings.small_collection_languages is None:
            return []
        return library.settings.small_collection_languages

    @classmethod
    def tiny_collection_languages(cls, library: Library) -> list[str]:
        cls.estimate_language_collections_when_unset(library)
        if library.settings.tiny_collection_languages is None:
            return []
        return library.settings.tiny_collection_languages

    @classmethod
    def max_outstanding_fines(cls, library: Library) -> Money | None:
        if library.settings.max_outstanding_fines is None:
            return None
        return MoneyUtility.parse(library.settings.max_outstanding_fines)

    @classmethod
    def estimate_language_collections_for_library(cls, library: Library) -> None:
        """Guess at appropriate values for the given library for
        LARGE_COLLECTION_LANGUAGES, SMALL_COLLECTION_LANGUAGES, and
        TINY_COLLECTION_LANGUAGES. Set configuration values
        appropriately, overriding any previous values.
        """
        holdings = library.estimated_holdings_by_language()
        large, small, tiny = cls.classify_holdings(holdings)
        settings = LibrarySettings.construct(
            large_collection_languages=large,
            small_collection_languages=small,
            tiny_collection_languages=tiny,
        )
        library.update_settings(settings)

    @classmethod
    def classify_holdings(cls, works_by_language):
        """Divide languages into 'large', 'small', and 'tiny' colletions based
        on the number of works available for each.

        :param works_by_language: A Counter mapping languages to the
            number of active works available for that language.  The
            output of `Library.estimated_holdings_by_language` is a good
            thing to pass in.

        :return: a 3-tuple of lists (large, small, tiny).
        """
        large = []
        small = []
        tiny = []
        result = [large, small, tiny]

        if not works_by_language:
            # In the absence of any information, assume we have an
            # English collection and nothing else.
            large.append("eng")
            return result

        # The single most common language always gets a large
        # collection.
        #
        # Otherwise, it depends on how many works are in the
        # collection.
        for language, num_works in works_by_language.most_common():
            if not large:
                bucket = large
            elif num_works >= cls.LARGE_COLLECTION_CUTOFF:
                bucket = large
            elif num_works >= cls.SMALL_COLLECTION_CUTOFF:
                bucket = small
            else:
                bucket = tiny
            bucket.append(language)

        return result

    @classmethod
    def _as_mailto(cls, value):
        """Turn an email address into a mailto: URI."""
        if not value:
            return value
        if value.startswith("mailto:"):
            return value
        return "mailto:%s" % value

    @classmethod
    def help_uris(cls, library: Library) -> Iterable[tuple[str | None, str]]:
        """Find all the URIs that might help patrons get help from
        this library.

        :yield: A sequence of 2-tuples (media type, URL)
        """
        if library.settings.help_email:
            yield None, cls._as_mailto(library.settings.help_email)
        if library.settings.help_web:
            yield "text/html", library.settings.help_web

    @classmethod
    def copyright_designated_agent_uri(cls, library: Library) -> str | None:
        if library.settings.copyright_designated_agent_email_address:
            email = library.settings.copyright_designated_agent_email_address
        elif library.settings.help_email:
            email = library.settings.help_email
        else:
            return None

        return cls._as_mailto(email)

    @classmethod
    def configuration_contact_uri(cls, library: Library) -> str | None:
        if library.settings.configuration_contact_email_address:
            email = library.settings.configuration_contact_email_address
        elif library.settings.help_email:
            email = library.settings.help_email
        else:
            return None

        return cls._as_mailto(email)

    @classmethod
    def cipher(cls, key: bytes) -> PKCS1OAEP_Cipher:
        """Create a Cipher for a public or private key.

        This just wraps some hard-to-remember Crypto code.

        :param key: A string containing the key.

        :return: A Cipher object which will support either
            encrypt() (public key) or decrypt() (private key).
        """
        return PKCS1_OAEP.new(RSA.import_key(key))
