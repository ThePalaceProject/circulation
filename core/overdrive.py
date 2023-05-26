import datetime
import json
import logging
from threading import RLock
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote, urlsplit, urlunsplit

import isbnlib
from flask_babel import lazy_gettext as _
from requests.adapters import CaseInsensitiveDict, Response
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.session import Session

from api.circulation_exceptions import CannotFulfill

from .config import CannotLoadConfiguration, Configuration
from .coverage import BibliographicCoverageProvider
from .importers import BaseImporterConfiguration
from .metadata_layer import (
    CirculationData,
    ContributorData,
    FormatData,
    IdentifierData,
    LinkData,
    MeasurementData,
    Metadata,
    SubjectData,
)
from .model import (
    Classification,
    Collection,
    ConfigurationSetting,
    Contributor,
    Credential,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    Measurement,
    MediaTypes,
    Representation,
    Subject,
    get_one_or_create,
)
from .model.configuration import (
    ConfigurationAttributeType,
    ConfigurationFactory,
    ConfigurationGrouping,
    ConfigurationMetadata,
    ConfigurationOption,
    ConfigurationStorage,
    HasExternalIntegration,
)
from .util.datetime_helpers import strptime_utc, utc_now
from .util.http import HTTP, BadResponseException
from .util.string_helpers import base64


class OverdriveConfiguration(ConfigurationGrouping, BaseImporterConfiguration):
    """The basic Overdrive configuration"""

    OVERDRIVE_CLIENT_KEY = "overdrive_client_key"
    OVERDRIVE_CLIENT_SECRET = "overdrive_client_secret"
    OVERDRIVE_SERVER_NICKNAME = "overdrive_server_nickname"
    OVERDRIVE_WEBSITE_ID = "overdrive_website_id"

    # Note that the library ID is not included here because it is not Overdrive-specific
    OVERDRIVE_CONFIGURATION_KEYS = {
        OVERDRIVE_CLIENT_KEY,
        OVERDRIVE_CLIENT_SECRET,
        OVERDRIVE_SERVER_NICKNAME,
        OVERDRIVE_WEBSITE_ID,
    }

    library_id = ConfigurationMetadata(
        key=Collection.EXTERNAL_ACCOUNT_ID_KEY,
        label=_("Library ID"),
        type=ConfigurationAttributeType.TEXT,
        description="The library identifier.",
        required=True,
    )
    overdrive_website_id = ConfigurationMetadata(
        key=OVERDRIVE_WEBSITE_ID,
        label=_("Website ID"),
        type=ConfigurationAttributeType.TEXT,
        description="The web site identifier.",
        required=True,
    )
    overdrive_client_key = ConfigurationMetadata(
        key=OVERDRIVE_CLIENT_KEY,
        label=_("Client Key"),
        type=ConfigurationAttributeType.TEXT,
        description="The Overdrive client key.",
        required=True,
    )
    overdrive_client_secret = ConfigurationMetadata(
        key=OVERDRIVE_CLIENT_SECRET,
        label=_("Client Secret"),
        type=ConfigurationAttributeType.TEXT,
        description="The Overdrive client secret.",
        required=True,
    )

    PRODUCTION_SERVERS = "production"
    TESTING_SERVERS = "testing"

    overdrive_server_nickname = ConfigurationMetadata(
        key=OVERDRIVE_SERVER_NICKNAME,
        label=_("Server family"),
        type=ConfigurationAttributeType.SELECT,
        required=False,
        default=PRODUCTION_SERVERS,
        description="Unless you hear otherwise from Overdrive, your integration should use their production servers.",
        options=[
            ConfigurationOption(label=_("Production"), key=PRODUCTION_SERVERS),
            ConfigurationOption(label=_("Testing"), key=TESTING_SERVERS),
        ],
    )


class OverdriveCoreAPI(HasExternalIntegration):
    # An OverDrive defined constant indicating the "main" or parent account
    # associated with an OverDrive collection.
    OVERDRIVE_MAIN_ACCOUNT_ID = -1

    log = logging.getLogger("Overdrive API")

    # A lock for threaded usage.
    lock = RLock()

    # Production and testing have different host names for some of the
    # API endpoints. This is configurable on the collection level.
    HOSTS = {
        OverdriveConfiguration.PRODUCTION_SERVERS: dict(
            host="https://api.overdrive.com",
            patron_host="https://patron.api.overdrive.com",
        ),
        OverdriveConfiguration.TESTING_SERVERS: dict(
            host="https://integration.api.overdrive.com",
            patron_host="https://integration-patron.api.overdrive.com",
        ),
    }

    # Production and testing setups use the same URLs for Client
    # Authentication and Patron Authentication, but we use the same
    # system as for other hostnames to give a consistent look to the
    # templates.
    for host in list(HOSTS.values()):
        host["oauth_patron_host"] = "https://oauth-patron.overdrive.com"
        host["oauth_host"] = "https://oauth.overdrive.com"

    # Each of these endpoint URLs has a slot to plug in one of the
    # appropriate servers. This will be filled in either by a call to
    # the endpoint() method (if there are other variables in the
    # template), or by the _do_get or _do_post methods (if there are
    # no other variables).
    TOKEN_ENDPOINT = "%(oauth_host)s/token"
    PATRON_TOKEN_ENDPOINT = "%(oauth_patron_host)s/patrontoken"

    LIBRARY_ENDPOINT = "%(host)s/v1/libraries/%(library_id)s"
    ADVANTAGE_LIBRARY_ENDPOINT = (
        "%(host)s/v1/libraries/%(parent_library_id)s/advantageAccounts/%(library_id)s"
    )
    ALL_PRODUCTS_ENDPOINT = (
        "%(host)s/v1/collections/%(collection_token)s/products?sort=%(sort)s"
    )
    METADATA_ENDPOINT = (
        "%(host)s/v1/collections/%(collection_token)s/products/%(item_id)s/metadata"
    )
    EVENTS_ENDPOINT = "%(host)s/v1/collections/%(collection_token)s/products?lastUpdateTime=%(lastupdatetime)s&sort=%(sort)s&limit=%(limit)s"
    AVAILABILITY_ENDPOINT = "%(host)s/v2/collections/%(collection_token)s/products/%(product_id)s/availability"

    PATRON_INFORMATION_ENDPOINT = "%(patron_host)s/v1/patrons/me"
    CHECKOUTS_ENDPOINT = "%(patron_host)s/v1/patrons/me/checkouts"
    CHECKOUT_ENDPOINT = "%(patron_host)s/v1/patrons/me/checkouts/%(overdrive_id)s"
    FORMATS_ENDPOINT = (
        "%(patron_host)s/v1/patrons/me/checkouts/%(overdrive_id)s/formats"
    )
    HOLDS_ENDPOINT = "%(patron_host)s/v1/patrons/me/holds"
    HOLD_ENDPOINT = "%(patron_host)s/v1/patrons/me/holds/%(product_id)s"
    ME_ENDPOINT = "%(patron_host)s/v1/patrons/me"

    MAX_CREDENTIAL_AGE = 50 * 60

    PAGE_SIZE_LIMIT = 300
    EVENT_SOURCE = "Overdrive"

    EVENT_DELAY = datetime.timedelta(minutes=120)

    # The formats we care about.
    FORMATS = "ebook-epub-open,ebook-epub-adobe,ebook-pdf-adobe,ebook-pdf-open,audiobook-overdrive".split(
        ","
    )

    # The formats that can be read by the default Library Simplified reader.
    DEFAULT_READABLE_FORMATS = {
        "ebook-epub-open",
        "ebook-epub-adobe",
        "ebook-pdf-open",
        "audiobook-overdrive",
    }

    # The formats that indicate the book has been fulfilled on an
    # incompatible platform and just can't be fulfilled on Simplified
    # in any format.
    INCOMPATIBLE_PLATFORM_FORMATS = {"ebook-kindle"}

    OVERDRIVE_READ_FORMAT = "ebook-overdrive"

    TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    # When associating an Overdrive account with a library, it's
    # necessary to also specify an "ILS name" obtained from
    # Overdrive. Components that don't authenticate patrons (such as
    # the metadata wrangler) don't need to set this value.
    ILS_NAME_KEY = "ils_name"
    ILS_NAME_DEFAULT = "default"

    _configuration_storage: ConfigurationStorage
    _configuration_factory: ConfigurationFactory
    _configuration: OverdriveConfiguration
    _external_integration: ExternalIntegration
    _db: Session
    _hosts: Dict[str, str]
    _library_id: str
    _collection_id: int

    def __init__(self, _db: Session, collection: Collection):
        if collection.protocol != ExternalIntegration.OVERDRIVE:
            raise ValueError(
                "Collection protocol is %s, but passed into OverdriveAPI!"
                % collection.protocol
            )

        _library_id = collection.external_account_id
        if not _library_id:
            raise ValueError(
                "Collection %s must have an external account ID" % collection.id
            )
        else:
            self._library_id = _library_id

        self._db = _db
        self._external_integration = collection.external_integration
        if collection.id is None:
            raise ValueError(
                "Collection passed into OverdriveAPI must have an ID, but %s does not"
                % collection.name
            )
        self._collection_id = collection.id

        # Initialize configuration information.
        self._configuration_storage = ConfigurationStorage(self)
        self._configuration_factory = ConfigurationFactory()
        self._configuration = OverdriveConfiguration(
            configuration_storage=self._configuration_storage, db=_db
        )

        if collection.parent:
            # This is an Overdrive Advantage account.
            self.parent_library_id = collection.parent.external_account_id

            # We're going to inherit all of the Overdrive credentials
            # from the parent (the main Overdrive account), except for the
            # library ID, which we already set.
            parent_integration = collection.parent.external_integration

            for key in OverdriveConfiguration.OVERDRIVE_CONFIGURATION_KEYS:
                parent_value = parent_integration.setting(key)
                self._configuration.set_setting_value(key, parent_value.value)
        else:
            self.parent_library_id = None

        if not self._configuration.overdrive_client_key:
            raise CannotLoadConfiguration("Overdrive client key is not configured")
        if not self._configuration.overdrive_client_secret:
            raise CannotLoadConfiguration(
                "Overdrive client password/secret is not configured"
            )
        if not self._configuration.overdrive_website_id:
            raise CannotLoadConfiguration("Overdrive website ID is not configured")

        self._server_nickname = (
            self._configuration.overdrive_server_nickname
            or OverdriveConfiguration.PRODUCTION_SERVERS
        )

        self._hosts = self._determine_hosts(server_nickname=self._server_nickname)

        # This is set by an access to .token, or by a call to
        # check_creds() or refresh_creds().
        self._token = None

        # This is set by an access to .collection_token
        self._collection_token = None

    def _determine_hosts(self, *, server_nickname: str) -> Dict[str, str]:
        # Figure out which hostnames we'll be using when constructing
        # endpoint URLs.
        if server_nickname not in self.HOSTS:
            server_nickname = OverdriveConfiguration.PRODUCTION_SERVERS

        return dict(self.HOSTS[server_nickname])

    def external_integration(self, db: Session) -> ExternalIntegration:
        return self._external_integration

    def endpoint(self, url: str, **kwargs) -> str:
        """Create the URL to an Overdrive API endpoint.

        :param url: A template for the URL.
        :param kwargs: Arguments to be interpolated into the template.
           The server hostname will be interpolated automatically; you
           don't have to pass it in.
        """
        if not "%(" in url:
            # Nothing to interpolate.
            return url
        kwargs.update(self._hosts)
        return url % kwargs

    @property
    def token(self):
        if not self._token:
            self.check_creds()
        return self._token

    @property
    def collection_token(self):
        """Get the token representing this particular Overdrive collection.

        As a side effect, this will verify that the Overdrive
        credentials are working.
        """
        if not self._collection_token:
            self.check_creds()
            library = self.get_library()
            error = library.get("errorCode")
            if error:
                message = library.get("message")
                raise CannotLoadConfiguration(
                    "Overdrive credentials are valid but could not fetch library: %s"
                    % message
                )
            self._collection_token = library["collectionToken"]
        return self._collection_token

    @property
    def collection(self) -> Optional[Collection]:
        return Collection.by_id(self._db, id=self._collection_id)

    @property
    def source(self):
        return DataSource.lookup(self._db, DataSource.OVERDRIVE)

    def ils_name(self, library):
        """Determine the ILS name to use for the given Library."""
        return self.ils_name_setting(
            self._db, self.collection, library
        ).value_or_default(self.ILS_NAME_DEFAULT)

    @classmethod
    def ils_name_setting(cls, _db, collection, library):
        """Find the ConfigurationSetting controlling the ILS name
        for the given collection and library.
        """
        return ConfigurationSetting.for_library_and_externalintegration(
            _db, cls.ILS_NAME_KEY, library, collection.external_integration
        )

    @property
    def advantage_library_id(self):
        """The library ID for this library, as we should look for it in
        certain API documents served by Overdrive.

        For ordinary collections (ie non-Advantage) with or without associated
        Advantage (ie child) collections shared among libraries, this will be
        equal to the OVERDRIVE_MAIN_ACCOUNT_ID.

        For Overdrive Advantage accounts, this will be the numeric
        value of the Overdrive library ID.
        """
        if self.parent_library_id is None:
            # This is not an Overdrive Advantage collection.
            #
            # Instead of looking for the library ID itself in these
            # documents, we should look for the constant main account id.
            return self.OVERDRIVE_MAIN_ACCOUNT_ID
        return int(self._library_id)

    def check_creds(self, force_refresh=False):
        """If the Bearer Token has expired, update it."""
        with self.lock:
            refresh_on_lookup = self.refresh_creds
            if force_refresh:
                refresh_on_lookup = lambda x: x

            credential = self.credential_object(refresh_on_lookup)
            if force_refresh:
                self.refresh_creds(credential)
            self._token = credential.credential

    def credential_object(self, refresh):
        """Look up the Credential object that allows us to use
        the Overdrive API.
        """
        return Credential.lookup(
            self._db,
            DataSource.OVERDRIVE,
            None,
            None,
            refresh,
            collection=self.collection,
        )

    def refresh_creds(self, credential):
        """Fetch a new Bearer Token and update the given Credential object."""
        response = self.token_post(
            self.TOKEN_ENDPOINT,
            dict(grant_type="client_credentials"),
            allowed_response_codes=[200],
        )
        data = response.json()
        self._update_credential(credential, data)
        self._token = credential.credential

    def get(
        self, url: str, extra_headers={}, exception_on_401=False
    ) -> Tuple[int, CaseInsensitiveDict, bytes]:
        """Make an HTTP GET request using the active Bearer Token."""
        request_headers = dict(Authorization="Bearer %s" % self.token)
        request_headers.update(extra_headers)

        response: Response = self._do_get(
            url, request_headers, allowed_response_codes=["2xx", "3xx", "401", "404"]
        )
        status_code: int = response.status_code
        headers: CaseInsensitiveDict = response.headers
        content: bytes = response.content

        if status_code == 401:
            if exception_on_401:
                # This is our second try. Give up.
                raise BadResponseException.from_response(
                    url,
                    "Something's wrong with the Overdrive OAuth Bearer Token!",
                    (status_code, headers, content),
                )
            else:
                # Refresh the token and try again.
                self.check_creds(True)
                return self.get(url, extra_headers, True)
        else:
            return status_code, headers, content

    @property
    def token_authorization_header(self) -> str:
        s = b"%s:%s" % (self.client_key(), self.client_secret())
        return "Basic " + base64.standard_b64encode(s).strip()

    @property
    def fulfillment_authorization_header(self) -> str:
        is_test_mode = (
            True
            if self._server_nickname == OverdriveConfiguration.TESTING_SERVERS
            else False
        )
        try:
            client_credentials = Configuration.overdrive_fulfillment_keys(
                testing=is_test_mode
            )
        except CannotLoadConfiguration as e:
            raise CannotFulfill(*e.args)

        s = b"%s:%s" % (
            client_credentials["key"].encode(),
            client_credentials["secret"].encode(),
        )
        return "Basic " + base64.standard_b64encode(s).strip()

    def token_post(
        self,
        url: str,
        payload: Dict[str, str],
        is_fulfillment=False,
        headers={},
        **kwargs,
    ) -> Response:
        """Make an HTTP POST request for purposes of getting an OAuth token."""
        headers = dict(headers)
        headers["Authorization"] = (
            self.token_authorization_header
            if not is_fulfillment
            else self.fulfillment_authorization_header
        )
        return self._do_post(url, payload, headers, **kwargs)

    @staticmethod
    def _update_credential(credential, overdrive_data):
        """Copy Overdrive OAuth data into a Credential object."""
        credential.credential = overdrive_data["access_token"]
        expires_in = overdrive_data["expires_in"] * 0.9
        credential.expires = utc_now() + datetime.timedelta(seconds=expires_in)

    @property
    def _library_endpoint(self) -> str:
        """Which URL should we go to to get information about this collection?

        If this is an ordinary Overdrive account, we get information
        from LIBRARY_ENDPOINT.

        If this is an Overdrive Advantage account, we get information
        from LIBRARY_ADVANTAGE_ENDPOINT.
        """
        args = dict(library_id=self._library_id)
        if self.parent_library_id:
            # This is an Overdrive advantage account.
            args["parent_library_id"] = self.parent_library_id
            endpoint = self.ADVANTAGE_LIBRARY_ENDPOINT
        else:
            endpoint = self.LIBRARY_ENDPOINT
        return self.endpoint(endpoint, **args)

    def get_library(self):
        """Get basic information about the collection, including
        a link to the titles in the collection.
        """
        url = self._library_endpoint
        with self.lock:
            representation, cached = Representation.get(
                self._db,
                url,
                self.get,
                exception_handler=Representation.reraise_exception,
            )
            return json.loads(representation.content)

    def get_advantage_accounts(self):
        """Find all the Overdrive Advantage accounts managed by this library.

        :yield: A sequence of OverdriveAdvantageAccount objects.
        """
        library = self.get_library()
        links = library.get("links", {})
        advantage = links.get("advantageAccounts")
        if not advantage:
            return []
        if advantage:
            # This library has Overdrive Advantage accounts, or at
            # least a link where some may be found.
            advantage_url = advantage.get("href")
            if not advantage_url:
                return
            representation, cached = Representation.get(
                self._db,
                advantage_url,
                self.get,
                exception_handler=Representation.reraise_exception,
            )
            return OverdriveAdvantageAccount.from_representation(representation.content)

    def all_ids(self):
        """Get IDs for every book in the system, with the most recently added
        ones at the front.
        """
        next_link = self._all_products_link
        while next_link:
            page_inventory, next_link = self._get_book_list_page(next_link, "next")

            yield from page_inventory

    @property
    def _all_products_link(self) -> str:
        url = self.endpoint(
            self.ALL_PRODUCTS_ENDPOINT,
            collection_token=self.collection_token,
            sort="dateAdded:desc",
        )
        return self.make_link_safe(url)

    def _get_book_list_page(self, link, rel_to_follow="next", extractor_class=None):
        """Process a page of inventory whose circulation we need to check.

        Returns a 2-tuple: (availability_info, next_link).
        `availability_info` is a list of dictionaries, each containing
           basic availability and bibliographic information about
           one book.
        `next_link` is a link to the next page of results.
        """
        extractor_class = extractor_class or OverdriveRepresentationExtractor
        # We don't cache this because it changes constantly.
        status_code, headers, content = self.get(link, {})
        if isinstance(content, (bytes, str)):
            content = json.loads(content)

        # Find the link to the next page of results, if any.
        next_link = extractor_class.link(content, rel_to_follow)

        # Prepare to get availability information for all the books on
        # this page.
        availability_queue = extractor_class.availability_link_list(content)
        return availability_queue, next_link

    def recently_changed_ids(self, start, cutoff):
        """Get IDs of books whose status has changed between the start time
        and now.
        """
        # `cutoff` is not supported by Overdrive, so we ignore it. All
        # we can do is get events between the start time and now.

        last_update_time = start - self.EVENT_DELAY
        self.log.info("Asking for circulation changes since %s", last_update_time)
        last_update = last_update_time.strftime(self.TIME_FORMAT)

        next_link = self.endpoint(
            self.EVENTS_ENDPOINT,
            lastupdatetime=last_update,
            sort="popularity:desc",
            limit=self.PAGE_SIZE_LIMIT,
            collection_token=self.collection_token,
        )
        next_link = self.make_link_safe(next_link)
        while next_link:
            page_inventory, next_link = self._get_book_list_page(next_link)
            # We won't be sending out any events for these books yet,
            # because we don't know if anything changed, but we will
            # be putting them on the list of inventory items to
            # refresh. At that point we will send out events.
            yield from page_inventory

    def metadata_lookup(self, identifier):
        """Look up metadata for an Overdrive identifier."""
        url = self.endpoint(
            self.METADATA_ENDPOINT,
            collection_token=self.collection_token,
            item_id=identifier.identifier,
        )
        status_code, headers, content = self.get(url, {})
        if isinstance(content, (bytes, str)):
            content = json.loads(content)
        return content

    def metadata_lookup_obj(self, identifier):
        url = self.endpoint(
            self.METADATA_ENDPOINT,
            collection_token=self.collection_token,
            item_id=identifier,
        )
        status_code, headers, content = self.get(url, {})
        if isinstance(content, (bytes, str)):
            content = json.loads(content)
        return OverdriveRepresentationExtractor.book_info_to_metadata(content)

    @classmethod
    def make_link_safe(cls, url: str) -> str:
        """Turn a server-provided link into a link the server will accept!

        The {} part is completely obnoxious and I have complained about it to
        Overdrive.

        The availability part is to make sure we always use v2 of the
        availability API, even if Overdrive sent us a link to v1.
        """
        parts = list(urlsplit(url))
        parts[2] = quote(parts[2])
        endings = ("/availability", "/availability/")
        if parts[2].startswith("/v1/collections/") and any(
            parts[2].endswith(x) for x in endings
        ):
            parts[2] = parts[2].replace("/v1/collections/", "/v2/collections/", 1)
        query_string = parts[3]
        query_string = query_string.replace("+", "%2B")
        query_string = query_string.replace(":", "%3A")
        query_string = query_string.replace("{", "%7B")
        query_string = query_string.replace("}", "%7D")
        parts[3] = query_string
        return urlunsplit(tuple(parts))

    def _do_get(self, url: str, headers, **kwargs) -> Response:
        """This method is overridden in MockOverdriveAPI."""
        url = self.endpoint(url)
        kwargs["max_retry_count"] = int(self._configuration.max_retry_count)
        kwargs["timeout"] = 120
        return HTTP.get_with_timeout(url, headers=headers, **kwargs)

    def _do_post(self, url: str, payload, headers, **kwargs) -> Response:
        """This method is overridden in MockOverdriveAPI."""
        url = self.endpoint(url)
        kwargs["max_retry_count"] = int(self._configuration.max_retry_count)
        kwargs["timeout"] = 120
        return HTTP.post_with_timeout(url, payload, headers=headers, **kwargs)

    def website_id(self) -> bytes:
        return self._configuration.overdrive_website_id.encode("utf-8")

    def client_key(self) -> bytes:
        return self._configuration.overdrive_client_key.encode("utf-8")

    def client_secret(self) -> bytes:
        return self._configuration.overdrive_client_secret.encode("utf-8")

    def library_id(self) -> str:
        return self._library_id

    def hosts(self) -> Dict[str, str]:
        return dict(self._hosts)


class OverdriveRepresentationExtractor:
    """Extract useful information from Overdrive's JSON representations."""

    log = logging.getLogger("Overdrive representation extractor")

    def __init__(self, api):
        """Constructor.

        :param api: An OverdriveAPI object. This will be used when deciding
        which portions of a JSON representation are relevant to the active
        Overdrive collection.
        """
        self.library_id = api.advantage_library_id

    @classmethod
    def availability_link_list(cls, book_list):
        """:return: A list of dictionaries with keys `id`, `title`, `availability_link`."""
        l = []
        if not "products" in book_list:
            return []

        products = book_list["products"]
        for product in products:
            if not "id" in product:
                cls.log.warning("No ID found in %r", product)
                continue
            book_id = product["id"]
            data = dict(
                id=book_id,
                title=product.get("title"),
                author_name=None,
                date_added=product.get("dateAdded"),
            )
            if "primaryCreator" in product:
                creator = product["primaryCreator"]
                if creator.get("role") == "Author":
                    data["author_name"] = creator.get("name")
            links = product.get("links", [])
            if "availability" in links:
                link = links["availability"]["href"]
                data["availability_link"] = OverdriveCoreAPI.make_link_safe(link)
            else:
                logging.getLogger("Overdrive API").warning(
                    "No availability link for %s", book_id
                )
            l.append(data)
        return l

    @classmethod
    def link(self, page, rel):
        if "links" in page and rel in page["links"]:
            raw_link = page["links"][rel]["href"]
            link = OverdriveCoreAPI.make_link_safe(raw_link)
        else:
            link = None
        return link

    format_data_for_overdrive_format = {
        "ebook-pdf-adobe": (Representation.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
        "ebook-pdf-open": (Representation.PDF_MEDIA_TYPE, DeliveryMechanism.NO_DRM),
        "ebook-epub-adobe": (
            Representation.EPUB_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM,
        ),
        "ebook-epub-open": (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM),
        "audiobook-mp3": ("application/x-od-media", DeliveryMechanism.OVERDRIVE_DRM),
        "music-mp3": ("application/x-od-media", DeliveryMechanism.OVERDRIVE_DRM),
        "ebook-overdrive": [
            (
                MediaTypes.OVERDRIVE_EBOOK_MANIFEST_MEDIA_TYPE,
                DeliveryMechanism.LIBBY_DRM,
            ),
            (
                DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
                DeliveryMechanism.STREAMING_DRM,
            ),
        ],
        "audiobook-overdrive": [
            (
                MediaTypes.OVERDRIVE_AUDIOBOOK_MANIFEST_MEDIA_TYPE,
                DeliveryMechanism.LIBBY_DRM,
            ),
            (
                DeliveryMechanism.STREAMING_AUDIO_CONTENT_TYPE,
                DeliveryMechanism.STREAMING_DRM,
            ),
        ],
        "video-streaming": (
            DeliveryMechanism.STREAMING_VIDEO_CONTENT_TYPE,
            DeliveryMechanism.STREAMING_DRM,
        ),
        "ebook-kindle": (
            DeliveryMechanism.KINDLE_CONTENT_TYPE,
            DeliveryMechanism.KINDLE_DRM,
        ),
        "periodicals-nook": (
            DeliveryMechanism.NOOK_CONTENT_TYPE,
            DeliveryMechanism.NOOK_DRM,
        ),
    }

    # A mapping of the overdrive format name to end sample content type
    # Overdrive samples are not DRM protected so the links should be
    # stored as the end sample content type
    sample_format_to_content_type = {
        "ebook-overdrive": "text/html",
        "audiobook-wma": "audio/x-ms-wma",
        "audiobook-mp3": "audio/mpeg",
        "audiobook-overdrive": "text/html",
        "ebook-epub-adobe": "application/epub+zip",
        "magazine-overdrive": "text/html",
    }

    @classmethod
    def internal_formats(cls, overdrive_format):
        """Yield all internal formats for the given Overdrive format.

        Some Overdrive formats become multiple internal formats.

        :yield: A sequence of (content type, DRM system) 2-tuples
        """
        result = cls.format_data_for_overdrive_format.get(overdrive_format)
        if not result:
            return
        if isinstance(result, list):
            yield from result
        else:
            yield result

    ignorable_overdrive_formats: Set[str] = set()

    overdrive_role_to_simplified_role = {
        "actor": Contributor.ACTOR_ROLE,
        "artist": Contributor.ARTIST_ROLE,
        "book producer": Contributor.PRODUCER_ROLE,
        "associated name": Contributor.ASSOCIATED_ROLE,
        "author": Contributor.AUTHOR_ROLE,
        "author of introduction": Contributor.INTRODUCTION_ROLE,
        "author of foreword": Contributor.FOREWORD_ROLE,
        "author of afterword": Contributor.AFTERWORD_ROLE,
        "contributor": Contributor.CONTRIBUTOR_ROLE,
        "colophon": Contributor.COLOPHON_ROLE,
        "adapter": Contributor.ADAPTER_ROLE,
        "etc.": Contributor.UNKNOWN_ROLE,
        "cast member": Contributor.ACTOR_ROLE,
        "collaborator": Contributor.COLLABORATOR_ROLE,
        "compiler": Contributor.COMPILER_ROLE,
        "composer": Contributor.COMPOSER_ROLE,
        "copyright holder": Contributor.COPYRIGHT_HOLDER_ROLE,
        "director": Contributor.DIRECTOR_ROLE,
        "editor": Contributor.EDITOR_ROLE,
        "engineer": Contributor.ENGINEER_ROLE,
        "executive producer": Contributor.EXECUTIVE_PRODUCER_ROLE,
        "illustrator": Contributor.ILLUSTRATOR_ROLE,
        "musician": Contributor.MUSICIAN_ROLE,
        "narrator": Contributor.NARRATOR_ROLE,
        "other": Contributor.UNKNOWN_ROLE,
        "performer": Contributor.PERFORMER_ROLE,
        "producer": Contributor.PRODUCER_ROLE,
        "translator": Contributor.TRANSLATOR_ROLE,
        "photographer": Contributor.PHOTOGRAPHER_ROLE,
        "lyricist": Contributor.LYRICIST_ROLE,
        "transcriber": Contributor.TRANSCRIBER_ROLE,
        "designer": Contributor.DESIGNER_ROLE,
    }

    overdrive_medium_to_simplified_medium = {
        "eBook": Edition.BOOK_MEDIUM,
        "Video": Edition.VIDEO_MEDIUM,
        "Audiobook": Edition.AUDIO_MEDIUM,
        "Music": Edition.MUSIC_MEDIUM,
        "Periodicals": Edition.PERIODICAL_MEDIUM,
    }

    DATE_FORMAT = "%Y-%m-%d"

    @classmethod
    def parse_roles(cls, id, rolestring):
        rolestring = rolestring.lower()
        roles = [x.strip() for x in rolestring.split(",")]
        if " and " in roles[-1]:
            roles = roles[:-1] + [x.strip() for x in roles[-1].split(" and ")]
        processed = []
        for x in roles:
            if x not in cls.overdrive_role_to_simplified_role:
                cls.log.error("Could not process role %s for %s", x, id)
            else:
                processed.append(cls.overdrive_role_to_simplified_role[x])
        return processed

    def book_info_to_circulation(self, book):
        """Note:  The json data passed into this method is from a different file/stream
        from the json data that goes into the book_info_to_metadata() method.
        """
        # In Overdrive, 'reserved' books show up as books on
        # hold. There is no separate notion of reserved books.
        licenses_reserved = 0

        licenses_owned = None
        licenses_available = None
        patrons_in_hold_queue = None

        # TODO: The only reason this works for a NotFound error is the
        # circulation code sticks the known book ID into `book` ahead
        # of time. That's a code smell indicating that this system
        # needs to be refactored.
        if "reserveId" in book and not "id" in book:
            book["id"] = book["reserveId"]
        if not "id" in book:
            return None
        overdrive_id = book["id"]
        primary_identifier = IdentifierData(Identifier.OVERDRIVE_ID, overdrive_id)
        # TODO: We might be able to use this information to avoid the
        # need for explicit configuration of Advantage collections, or
        # at least to keep Advantage collections more up-to-date than
        # they would be otherwise, as a side effect of updating
        # regular Overdrive collections.

        # TODO: this would be the place to handle simultaneous use
        # titles -- these can be detected with
        # availabilityType="AlwaysAvailable" and have their
        # .licenses_owned set to LicensePool.UNLIMITED_ACCESS.
        # see http://developer.overdrive.com/apis/library-availability-new

        # TODO: Cost-per-circ titles
        # (availabilityType="LimitedAvailablility") can be handled
        # similarly, though those can abruptly become unavailable, so
        # UNLIMITED_ACCESS is probably not appropriate.

        error_code = book.get("errorCode")
        # TODO: It's not clear what other error codes there might be.
        # The current behavior will respond to errors other than
        # NotFound by leaving the book alone, but this might not be
        # the right behavior.
        if error_code == "NotFound":
            licenses_owned = 0
            licenses_available = 0
            patrons_in_hold_queue = 0
        elif book.get("isOwnedByCollections") is not False:
            # We own this book.
            licenses_owned = 0
            licenses_available = 0

            for account in self._get_applicable_accounts(book.get("accounts", [])):
                licenses_owned += int(account.get("copiesOwned", 0))
                licenses_available += int(account.get("copiesAvailable", 0))

            if "numberOfHolds" in book:
                if patrons_in_hold_queue is None:
                    patrons_in_hold_queue = 0
                patrons_in_hold_queue += book["numberOfHolds"]

        return CirculationData(
            data_source=DataSource.OVERDRIVE,
            primary_identifier=primary_identifier,
            licenses_owned=licenses_owned,
            licenses_available=licenses_available,
            licenses_reserved=licenses_reserved,
            patrons_in_hold_queue=patrons_in_hold_queue,
        )

    def _get_applicable_accounts(
        self, accounts: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Returns those accounts from the accounts array that apply the
        current overdrive collection context.

        If this is an overdrive parent collection, we want to return accounts
        associated with the main OverDrive "library" and any non-main account
        with sharing enabled.

        If this is a child OverDrive collection, then we return only the
        account associated with that child's OverDrive Advantage "library".
        Additionally, we want to exclude the account if it is "shared" since
        we will be counting it with the parent collection.
        """

        if self.library_id == OverdriveCoreAPI.OVERDRIVE_MAIN_ACCOUNT_ID:
            # this is a parent collection
            filtered_result = filter(
                lambda account: account.get("id")
                == OverdriveCoreAPI.OVERDRIVE_MAIN_ACCOUNT_ID
                or account.get("shared", False),
                accounts,
            )
        else:
            # this is child collection
            filtered_result = filter(
                lambda account: account.get("id") == self.library_id
                and not account.get("shared", False),
                accounts,
            )

        return list(filtered_result)

    @classmethod
    def image_link_to_linkdata(cls, link, rel):
        if not link or not "href" in link:
            return None
        href = link["href"]
        if "00000000-0000-0000-0000" in href:
            # This is a stand-in cover for preorders. It's better not
            # to have a cover at all -- we might be able to get one
            # later, or from another source.
            return None
        href = OverdriveCoreAPI.make_link_safe(href)
        media_type = link.get("type", None)
        return LinkData(rel=rel, href=href, media_type=media_type)

    @classmethod
    def book_info_to_metadata(
        cls, book, include_bibliographic=True, include_formats=True
    ):
        """Turn Overdrive's JSON representation of a book into a Metadata
        object.

        Note:  The json data passed into this method is from a different file/stream
        from the json data that goes into the book_info_to_circulation() method.
        """
        if not "id" in book:
            return None
        overdrive_id = book["id"]
        primary_identifier = IdentifierData(Identifier.OVERDRIVE_ID, overdrive_id)

        # If we trust classification data, we'll give it this weight.
        # Otherwise we'll probably give it a fraction of this weight.
        trusted_weight = Classification.TRUSTED_DISTRIBUTOR_WEIGHT

        if include_bibliographic:
            title = book.get("title", None)
            sort_title = book.get("sortTitle")
            subtitle = book.get("subtitle", None)
            series = book.get("series", None)
            publisher = book.get("publisher", None)
            imprint = book.get("imprint", None)

            if "publishDate" in book:
                published = strptime_utc(book["publishDate"][:10], cls.DATE_FORMAT)
            else:
                published = None

            languages = [l["code"] for l in book.get("languages", [])]
            if "eng" in languages or not languages:
                language = "eng"
            else:
                language = sorted(languages)[0]

            contributors = []
            for creator in book.get("creators", []):
                sort_name = creator["fileAs"]
                display_name = creator["name"]
                role = creator["role"]
                roles = cls.parse_roles(overdrive_id, role) or [
                    Contributor.UNKNOWN_ROLE
                ]
                contributor = ContributorData(
                    sort_name=sort_name,
                    display_name=display_name,
                    roles=roles,
                    biography=creator.get("bioText", None),
                )
                contributors.append(contributor)

            subjects = []
            for sub in book.get("subjects", []):
                subject = SubjectData(
                    type=Subject.OVERDRIVE,
                    identifier=sub["value"],
                    weight=trusted_weight,
                )
                subjects.append(subject)

            for sub in book.get("keywords", []):
                subject = SubjectData(
                    type=Subject.TAG,
                    identifier=sub["value"],
                    # We don't use TRUSTED_DISTRIBUTOR_WEIGHT because
                    # we don't know where the tags come from --
                    # probably Overdrive users -- and they're
                    # frequently wrong.
                    weight=1,
                )
                subjects.append(subject)

            extra = dict()
            if "grade_levels" in book:
                # n.b. Grade levels are measurements of reading level, not
                # age appropriateness. We can use them as a measure of age
                # appropriateness in a pinch, but we weight them less
                # heavily than TRUSTED_DISTRIBUTOR_WEIGHT.
                for i in book["grade_levels"]:
                    subject = SubjectData(
                        type=Subject.GRADE_LEVEL,
                        identifier=i["value"],
                        weight=trusted_weight / 10,
                    )
                    subjects.append(subject)

            overdrive_medium = book.get("mediaType", None)
            if (
                overdrive_medium
                and overdrive_medium not in cls.overdrive_medium_to_simplified_medium
            ):
                cls.log.error(
                    "Could not process medium %s for %s", overdrive_medium, overdrive_id
                )

            medium = cls.overdrive_medium_to_simplified_medium.get(
                overdrive_medium, Edition.BOOK_MEDIUM
            )

            measurements = []
            if "awards" in book:
                extra["awards"] = book.get("awards", [])
                num_awards = len(extra["awards"])
                measurements.append(
                    MeasurementData(Measurement.AWARDS, str(num_awards))
                )

            for name, subject_type in (
                ("ATOS", Subject.ATOS_SCORE),
                ("lexileScore", Subject.LEXILE_SCORE),
                ("interestLevel", Subject.INTEREST_LEVEL),
            ):
                if not name in book:
                    continue
                identifier = str(book[name])
                subjects.append(
                    SubjectData(
                        type=subject_type, identifier=identifier, weight=trusted_weight
                    )
                )

            for grade_level_info in book.get("gradeLevels", []):
                grade_level = grade_level_info.get("value")
                subjects.append(
                    SubjectData(
                        type=Subject.GRADE_LEVEL,
                        identifier=grade_level,
                        weight=trusted_weight,
                    )
                )

            identifiers = []
            links = []
            sample_hrefs = set()
            for format in book.get("formats", []):
                for new_id in format.get("identifiers", []):
                    t = new_id["type"]
                    v = new_id["value"]
                    orig_v = v
                    type_key = None
                    if t == "ASIN":
                        type_key = Identifier.ASIN
                    elif t == "ISBN":
                        type_key = Identifier.ISBN
                        if len(v) == 10:
                            v = isbnlib.to_isbn13(v)
                        if v is None or not isbnlib.is_isbn13(v):
                            # Overdrive sometimes uses invalid values
                            # like "n/a" as placeholders. Ignore such
                            # values to avoid a situation where hundreds of
                            # books appear to have the same ISBN. ISBNs
                            # which fail check digit checks or are invalid
                            # also can occur. Log them for review.
                            cls.log.info("Bad ISBN value provided: %s", orig_v)
                            continue
                    elif t == "DOI":
                        type_key = Identifier.DOI
                    elif t == "UPC":
                        type_key = Identifier.UPC
                    elif t == "PublisherCatalogNumber":
                        continue
                    if type_key and v:
                        identifiers.append(IdentifierData(type_key, v, 1))

                # Samples become links.
                if "samples" in format:
                    for sample_info in format["samples"]:
                        href = sample_info["url"]
                        # Have we already parsed this sample? Overdrive repeats samples per format
                        if href in sample_hrefs:
                            continue

                        # Every sample has its own format type
                        overdrive_format_name = sample_info.get("formatType")
                        if not overdrive_format_name:
                            # Malformed sample
                            continue
                        content_type = cls.sample_format_to_content_type.get(
                            overdrive_format_name
                        )
                        if not content_type:
                            # Unusable by us.
                            cls.log.warning(
                                f"Did not find a sample format mapping for '{overdrive_format_name}': {href}"
                            )
                            continue

                        if Representation.is_media_type(content_type):
                            links.append(
                                LinkData(
                                    rel=Hyperlink.SAMPLE,
                                    href=href,
                                    media_type=content_type,
                                )
                            )
                            sample_hrefs.add(href)

            # A cover and its thumbnail become a single LinkData.
            if "images" in book:
                images = book["images"]
                image_data = cls.image_link_to_linkdata(
                    images.get("cover"), Hyperlink.IMAGE
                )
                for name in ["cover300Wide", "cover150Wide", "thumbnail"]:
                    # Try to get a thumbnail that's as close as possible
                    # to the size we use.
                    image = images.get(name)
                    thumbnail_data = cls.image_link_to_linkdata(
                        image, Hyperlink.THUMBNAIL_IMAGE
                    )
                    if not image_data:
                        image_data = cls.image_link_to_linkdata(image, Hyperlink.IMAGE)
                    if thumbnail_data:
                        break

                if image_data:
                    if thumbnail_data:
                        image_data.thumbnail = thumbnail_data
                    links.append(image_data)

            # Descriptions become links.
            short = book.get("shortDescription")
            full = book.get("fullDescription")
            if full:
                links.append(
                    LinkData(
                        rel=Hyperlink.DESCRIPTION,
                        content=full,
                        media_type="text/html",
                    )
                )

            if short and (not full or not full.startswith(short)):
                links.append(
                    LinkData(
                        rel=Hyperlink.SHORT_DESCRIPTION,
                        content=short,
                        media_type="text/html",
                    )
                )

            # Add measurements: rating and popularity
            if book.get("starRating") is not None and book["starRating"] > 0:
                measurements.append(
                    MeasurementData(
                        quantity_measured=Measurement.RATING, value=book["starRating"]
                    )
                )

            if book.get("popularity"):
                measurements.append(
                    MeasurementData(
                        quantity_measured=Measurement.POPULARITY,
                        value=book["popularity"],
                    )
                )

            metadata = Metadata(
                data_source=DataSource.OVERDRIVE,
                title=title,
                subtitle=subtitle,
                sort_title=sort_title,
                language=language,
                medium=medium,
                series=series,
                publisher=publisher,
                imprint=imprint,
                published=published,
                primary_identifier=primary_identifier,
                identifiers=identifiers,
                subjects=subjects,
                contributors=contributors,
                measurements=measurements,
                links=links,
            )
        else:
            metadata = Metadata(
                data_source=DataSource.OVERDRIVE,
                primary_identifier=primary_identifier,
            )

        if include_formats:
            formats = []
            for format in book.get("formats", []):
                format_id = format["id"]
                internal_formats = list(cls.internal_formats(format_id))
                if internal_formats:
                    for content_type, drm_scheme in internal_formats:
                        formats.append(FormatData(content_type, drm_scheme))
                elif format_id not in cls.ignorable_overdrive_formats:
                    cls.log.error(
                        "Could not process Overdrive format %s for %s",
                        format_id,
                        overdrive_id,
                    )

            # Also make a CirculationData so we can write the formats,
            circulationdata = CirculationData(
                data_source=DataSource.OVERDRIVE,
                primary_identifier=primary_identifier,
                formats=formats,
            )

            metadata.circulation = circulationdata

        return metadata


class OverdriveAdvantageAccount:
    """Holder and parser for data associated with Overdrive Advantage."""

    def __init__(self, parent_library_id: int, library_id: int, name: str, token: str):
        """Constructor.

        :param parent_library_id: The library ID of the parent Overdrive
            account.
        :param library_id: The library ID of the Overdrive Advantage account.
        :param name: The name of the library whose Advantage account this is.
        :param token: The collection token for this Advantage account
        """
        self.parent_library_id = parent_library_id
        self.library_id = library_id
        self.name = name
        self.token = token

    @classmethod
    def from_representation(cls, content):
        """Turn the representation of an advantageAccounts link into a list of
        OverdriveAdvantageAccount objects.

        :param content: The data obtained by following an advantageAccounts
            link.
        :yield: A sequence of OverdriveAdvantageAccount objects.
        """
        data = json.loads(content)
        parent_id = str(data.get("id"))
        accounts = data.get("advantageAccounts", {})
        for account in accounts:
            name = account["name"]
            products_link = account["links"]["products"]["href"]
            library_id = str(account.get("id"))
            name = account.get("name")
            token = account.get("collectionToken")
            yield cls(
                parent_library_id=parent_id,
                library_id=library_id,
                name=name,
                token=token,
            )

    def to_collection(self, _db):
        """Find or create a Collection object for this Overdrive Advantage
        account.

        :return: a 2-tuple of Collections (primary Overdrive
            collection, Overdrive Advantage collection)
        """
        # First find the parent Collection.
        try:
            parent = (
                Collection.by_protocol(_db, ExternalIntegration.OVERDRIVE)
                .filter(Collection.external_account_id == self.parent_library_id)
                .one()
            )
        except NoResultFound as e:
            # Without the parent's credentials we can't access the child.
            raise ValueError(
                "Cannot create a Collection whose parent does not already exist."
            )
        name = parent.name + " / " + self.name
        child, is_new = get_one_or_create(
            _db,
            Collection,
            parent_id=parent.id,
            external_account_id=self.library_id,
            create_method_kwargs=dict(name=name),
        )
        if is_new:
            # Make sure the child has its protocol set appropriately.
            integration = child.create_external_integration(
                ExternalIntegration.OVERDRIVE
            )

        # Set or update the name of the collection to reflect the name of
        # the library, just in case that name has changed.
        child.name = name
        return parent, child


class OverdriveBibliographicCoverageProvider(BibliographicCoverageProvider):
    """Fill in bibliographic metadata for Overdrive records.

    This will occasionally fill in some availability information for a
    single Collection, but we rely on Monitors to keep availability
    information up to date for all Collections.
    """

    SERVICE_NAME = "Overdrive Bibliographic Coverage Provider"
    DATA_SOURCE_NAME = DataSource.OVERDRIVE
    PROTOCOL = ExternalIntegration.OVERDRIVE
    INPUT_IDENTIFIER_TYPES = Identifier.OVERDRIVE_ID

    def __init__(self, collection, api_class=OverdriveCoreAPI, **kwargs):
        """Constructor.

        :param collection: Provide bibliographic coverage to all
            Overdrive books in the given Collection.
        :param api_class: Instantiate this class with the given Collection,
            rather than instantiating OverdriveAPI.
        """
        super().__init__(collection, **kwargs)
        if isinstance(api_class, OverdriveCoreAPI):
            # Use a previously instantiated OverdriveAPI instance
            # rather than creating a new one.
            self.api = api_class
        else:
            # A web application should not use this option because it
            # will put a non-scoped session in the mix.
            _db = Session.object_session(collection)
            self.api = api_class(_db, collection)

    def process_item(self, identifier):
        info = self.api.metadata_lookup(identifier)
        error = None
        if info.get("errorCode") == "NotFound":
            error = "ID not recognized by Overdrive: %s" % identifier.identifier
        elif info.get("errorCode") == "InvalidGuid":
            error = "Invalid Overdrive ID: %s" % identifier.identifier

        if error:
            return self.failure(identifier, error, transient=False)

        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info)

        if not metadata:
            e = "Could not extract metadata from Overdrive data: %r" % info
            return self.failure(identifier, e)

        self.metadata_pre_hook(metadata)
        return self.set_metadata(identifier, metadata)

    def metadata_pre_hook(self, metadata):
        """A hook method that allows subclasses to modify a Metadata
        object derived from Overdrive before it's applied.
        """
        return metadata
