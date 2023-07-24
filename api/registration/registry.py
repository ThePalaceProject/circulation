from __future__ import annotations

import base64
import json
import logging
import sys
from argparse import ArgumentParser
from typing import Any, Callable, Dict, Generator, List, Literal, Optional, Tuple

import feedparser
from Crypto.Cipher.PKCS1_OAEP import PKCS1OAEP_Cipher
from flask import url_for
from flask_babel import lazy_gettext as _
from html_sanitizer import Sanitizer
from requests import Response
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.session import Session

from api.adobe_vendor_id import AuthdataUtility
from api.config import Configuration
from api.controller import CirculationManager
from api.problem_details import *
from core.model import (
    ConfigurationSetting,
    ExternalIntegration,
    Library,
    create,
    get_one,
)
from core.scripts import LibraryInputScript
from core.util.http import HTTP
from core.util.problem_detail import JSON_MEDIA_TYPE as PROBLEM_DETAIL_JSON_MEDIA_TYPE
from core.util.problem_detail import ProblemDetail

from ..util.flask import PalaceFlask
from .constants import RegistrationConstants

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class RemoteRegistry:
    """A circulation manager's view of a remote service that supports
    the OPDS Directory Registration Protocol:

    https://github.com/NYPL-Simplified/Simplified/wiki/OPDS-Directory-Registration-Protocol

    In practical terms, this may be a library registry (which has
    DISCOVERY_GOAL and wants to help patrons find their libraries) or
    it may be a shared ODL collection (which has LICENSE_GOAL).
    """

    DEFAULT_LIBRARY_REGISTRY_URL = "https://registry.thepalaceproject.org"
    DEFAULT_LIBRARY_REGISTRY_NAME = "Palace Library Registry"

    OPDS_1_PREFIX = "application/atom+xml;profile=opds-catalog"
    OPDS_2_TYPE = "application/opds+json"

    def __init__(self, integration: ExternalIntegration) -> None:
        """Constructor."""
        self.integration = integration

    @classmethod
    def for_integration_id(
        cls, _db: Session, integration_id: Optional[int], goal: str
    ) -> Optional[Self]:
        """Find a LibraryRegistry object configured
        by the given ExternalIntegration ID.

        :param goal: The ExternalIntegration's .goal must be this goal.
        """
        integration = get_one(_db, ExternalIntegration, goal=goal, id=integration_id)
        if not integration:
            return None
        return cls(integration)

    @classmethod
    def for_protocol_and_goal(
        cls, _db: Session, protocol: Optional[str], goal: Optional[str]
    ) -> Generator[Self, None, None]:
        """Find all LibraryRegistry objects with the given protocol and goal."""
        for i in _db.query(ExternalIntegration).filter(
            ExternalIntegration.goal == goal,
            ExternalIntegration.protocol == protocol,
        ):
            yield cls(i)

    @classmethod
    def for_protocol_goal_and_url(
        cls, _db: Session, protocol: str, goal: str, url: str
    ) -> Self:
        """Get a LibraryRegistry for the given protocol, goal, and
        URL. Create the corresponding ExternalIntegration if necessary.
        """
        try:
            integration = ExternalIntegration.with_setting_value(
                _db, protocol, goal, ExternalIntegration.URL, url
            ).one()
        except NoResultFound:
            integration = None
        if not integration:
            integration, is_new = create(
                _db, ExternalIntegration, protocol=protocol, goal=goal
            )
            integration.setting(ExternalIntegration.URL).value = url
        return cls(integration)

    @property
    def registrations(self) -> Generator[Registration, None, None]:
        """Find all of this site's successful registrations with
        this RemoteRegistry.

        :yield: A sequence of Registration objects.
        """
        for x in self.integration.libraries:
            yield Registration(self, x)

    def fetch_catalog(
        self,
        catalog_url: Optional[str] = None,
        do_get: Callable[..., Response | ProblemDetail] = HTTP.debuggable_get,
    ) -> Tuple[str, str] | ProblemDetail:
        """Fetch the root catalog for this RemoteRegistry.

        :return: A ProblemDetail if there's a problem communicating
            with the service or parsing the catalog; otherwise a 2-tuple
            (registration URL, Adobe vendor ID).
        """
        catalog_url = catalog_url or self.integration.url
        response = do_get(catalog_url)
        if isinstance(response, ProblemDetail):
            return response
        return self._extract_catalog_information(response)

    @classmethod
    def _extract_catalog_information(
        cls, response: Response
    ) -> Tuple[str, str] | ProblemDetail:
        """From an OPDS catalog, extract information that's essential to
        kickstarting the OPDS Directory Registration Protocol.

        :param response: A requests-style Response object.

        :return A ProblemDetail if there's a problem accessing the
            catalog; otherwise a 2-tuple (registration URL, Adobe vendor
            ID).
        """
        result = cls._extract_links(response)
        if isinstance(result, ProblemDetail):
            return result
        catalog, links = result
        if catalog:
            vendor_id = catalog.get("metadata", {}).get("adobe_vendor_id")
        else:
            vendor_id = None
        register_url = None
        for link in links:
            if link.get("rel") == "register":
                register_url = link.get("href")
                break
        if not register_url:
            return REMOTE_INTEGRATION_FAILED.detailed(
                _(
                    "The service at %(url)s did not provide a register link.",
                    url=response.url,
                )
            )
        return register_url, vendor_id

    def fetch_registration_document(
        self, do_get: Callable[..., Response | ProblemDetail] = HTTP.debuggable_get
    ) -> Tuple[Optional[str], Optional[str]] | ProblemDetail:
        """Fetch a discovery service's registration document and extract
        useful information from it.

        :return: A ProblemDetail if there's a problem accessing the
            service; otherwise, a 2-tuple (terms_of_service_link,
            terms_of_service_html), containing information about the
            Terms of Service that govern a circulation manager's
            registration with the discovery service.
        """
        catalog = self.fetch_catalog(do_get=do_get)
        if isinstance(catalog, ProblemDetail):
            return catalog
        registration_url, vendor_id = catalog

        response = do_get(registration_url)
        if isinstance(response, ProblemDetail):
            return response
        (
            terms_of_service_link,
            terms_of_service_html,
        ) = self._extract_registration_information(response)
        return terms_of_service_link, terms_of_service_html

    @classmethod
    def _extract_registration_information(
        cls, response: Response
    ) -> Tuple[Optional[str], Optional[str]]:
        """From an OPDS registration document, extract information that's
        useful to kickstarting the OPDS Directory Registration Protocol.

        The registration document is completely optional, so an
        invalid or unintelligible document is treated the same as a
        missing document.

        :return: A 2-tuple (terms_of_service_link,
            terms_of_service_html), containing information about the
            Terms of Service that govern a circulation manager's
            registration with the discovery service. If the
            registration document is missing or malformed, both values
            will be None.
        """
        tos_link = None
        tos_html = None
        result = cls._extract_links(response)
        if isinstance(result, ProblemDetail):
            return None, None
        catalog, links = result
        for link in links:
            if link.get("rel") != "terms-of-service":
                continue
            url = link.get("href") or ""
            is_http = any(
                [url.startswith(protocol + "://") for protocol in ("http", "https")]
            )
            if is_http and not tos_link:
                tos_link = url
            elif url.startswith("data:") and not tos_html:
                try:
                    tos_html = cls._decode_data_url(url)
                except Exception as e:
                    tos_html = None
        return tos_link, tos_html

    @classmethod
    def _extract_links(
        cls, response: Response
    ) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, str]]] | ProblemDetail:
        """Parse an OPDS 1 or OPDS feed out of a Requests response object.

        :return: A 2-tuple (parsed_catalog, links),
           with `links` being a list of dictionaries, each containing
           one OPDS link.
        """
        # The response must contain either an OPDS 2 catalog or an OPDS 1 feed.
        type = response.headers.get("Content-Type")
        if type and type.startswith(cls.OPDS_2_TYPE):
            # This is an OPDS 2 catalog.
            catalog = json.loads(response.content)
            links = catalog.get("links", [])
        elif type and type.startswith(cls.OPDS_1_PREFIX):
            # This is an OPDS 1 feed.
            feed = feedparser.parse(response.content)
            links = feed.get("feed", {}).get("links", [])
            catalog = None
        else:
            return REMOTE_INTEGRATION_FAILED.detailed(
                _("The service at %(url)s did not return OPDS.", url=response.url)
            )
        return catalog, links

    @classmethod
    def _decode_data_url(cls, url: str) -> str:
        """Convert a data: URL to a string of sanitized HTML.

        :raise ValueError: If the data: URL is invalid, in an
            unexpected format, or does not have a supported media type.
        :return: A string.
        """
        if not url.startswith("data:"):
            raise ValueError("Not a data: URL: %s" % url)
        parts = url.split(",")
        if len(parts) != 2:
            raise ValueError("Invalid data: URL: %s" % url)
        header, encoded = parts
        if not header.endswith(";base64"):
            raise ValueError("data: URL not base64-encoded: %s" % url)
        media_type = header[len("data:") : -len(";base64")]
        if not any(media_type.startswith(x) for x in ("text/html", "text/plain")):
            raise ValueError("Unsupported media type in data: URL: %s" % media_type)
        html = base64.b64decode(encoded.encode("utf-8")).decode("utf-8")
        return Sanitizer().sanitize(html)  # type: ignore[no-any-return]


class Registration(RegistrationConstants):
    """A library's registration for a particular registry.

    The registration does not correspond to one specific data model
    object -- it's a relationship between a Library and an
    ExternalIntegration, and a set of ConfigurationSettings that
    configure the relationship between the two.
    """

    def __init__(self, registry: RemoteRegistry, library: Library) -> None:
        self.registry = registry
        self.integration = self.registry.integration
        self.library = library
        self._db = Session.object_session(self.integration)

        if not library in self.integration.libraries:
            self.integration.libraries.append(library)

        # Find or create all the ConfigurationSettings that configure
        # this relationship between library and registry.
        # Has the registration succeeded? (Initial value: no.)
        self.status_field = self.setting(
            self.LIBRARY_REGISTRATION_STATUS, self.FAILURE_STATUS
        )

        # Does the library want to be in the testing or production stage?
        # (Initial value: testing.)
        self.stage_field = self.setting(
            self.LIBRARY_REGISTRATION_STAGE, self.TESTING_STAGE
        )

        # If the registry provides a web client for the library, it will
        # be stored in this setting.
        self.web_client_field = self.setting(self.LIBRARY_REGISTRATION_WEB_CLIENT)

    def setting(
        self, key: str, default_value: Optional[str] = None
    ) -> ConfigurationSetting:
        """Find or create a ConfigurationSetting that configures this
        relationship between library and registry.

        :param key: Name of the ConfigurationSetting.
        :return: A 2-tuple (ConfigurationSetting, is_new)
        """
        setting = ConfigurationSetting.for_library_and_externalintegration(
            self._db, key, self.library, self.integration
        )
        if setting.value is None and default_value is not None:
            setting.value = default_value
        return setting  # type: ignore[no-any-return]

    def push(
        self,
        stage: str,
        url_for: Callable[..., str],
        catalog_url: Optional[str] = None,
        do_get: Callable[..., Response] = HTTP.debuggable_get,
        do_post: Callable[..., Response] = HTTP.debuggable_post,
    ) -> Literal[True] | ProblemDetail:
        """Attempt to register a library with a RemoteRegistry.

        NOTE: This method is designed to be used in a
        controller. Other callers may use this method, but they must be
        able to render a ProblemDetail when there's a failure.

        NOTE: The application server must be running when this method
        is called, because part of the OPDS Directory Registration
        Protocol is the remote server retrieving the library's
        Authentication For OPDS document.

        :param stage: Either TESTING_STAGE or PRODUCTION_STAGE
        :param url_for: Flask url_for() or equivalent, used to generate URLs
            for the application server.
        :param do_get: Mockable method to make a GET request.
        :param do_post: Mockable method to make a POST request.

        :return: A ProblemDetail if there was a problem; otherwise True.
        """
        # Assume that the registration will fail.
        #
        # TODO: If a registration has previously succeeded, failure to
        # re-register probably means a maintenance of the status quo,
        # not a change of success to failure. But we don't have any way
        # of being sure.
        self.status_field.value = self.FAILURE_STATUS

        if stage not in self.VALID_REGISTRATION_STAGES:
            return INVALID_INPUT.detailed(
                _("%r is not a valid registration stage") % stage
            )

        if self.library.private_key is None:
            raise RuntimeError(f"{self.library.short_name} has no private key set.")
        cipher = Configuration.cipher(self.library.private_key)

        # Before we can start the registration protocol, we must fetch
        # the remote catalog's URL and extract the link to the
        # registration resource that kicks off the protocol.
        result = self.registry.fetch_catalog(catalog_url, do_get)
        if isinstance(result, ProblemDetail):
            return result
        register_url, vendor_id = result

        # Store the vendor id as a ConfigurationSetting on the integration
        # -- it'll be the same value for all libraries.
        if vendor_id:
            ConfigurationSetting.for_externalintegration(
                AuthdataUtility.VENDOR_ID_KEY, self.integration
            ).value = vendor_id

        # Build the document we'll be sending to the registration URL.
        payload = self._create_registration_payload(url_for, stage)

        if isinstance(payload, ProblemDetail):
            return payload

        headers = self._create_registration_headers()
        if isinstance(headers, ProblemDetail):
            return headers

        # Send the document.
        response = self._send_registration_request(
            register_url, headers, payload, do_post
        )

        if isinstance(response, ProblemDetail):
            return response
        catalog = json.loads(response.content)

        # Process the result.
        return self._process_registration_result(catalog, cipher, stage)

    def _create_registration_payload(
        self, url_for: Callable[..., str], stage: str
    ) -> Dict[str, str]:
        """Collect the key-value pairs to be sent when kicking off the
        registration protocol.

        :param url_for: An implementation of Flask url_for.
        :param state: The registrant's opinion about what stage this
           registration should be in.
        :return: A dictionary suitable for passing into requests.post.
        """
        auth_document_url = url_for(
            "authentication_document",
            library_short_name=self.library.short_name,
            _external=True,
        )
        payload = dict(url=auth_document_url, stage=stage)

        # Find the email address the administrator should use if they notice
        # a problem with the way the library is using an integration.
        contact = Configuration.configuration_contact_uri(self.library)
        if contact:
            payload["contact"] = contact
        return payload

    def _create_registration_headers(self) -> Dict[str, str]:
        shared_secret = self.setting(ExternalIntegration.PASSWORD).value
        headers = {}
        if shared_secret:
            headers["Authorization"] = "Bearer %s" % shared_secret
        return headers

    @classmethod
    def _send_registration_request(
        cls,
        register_url: str,
        headers: Dict[str, str],
        payload: Dict[str, str],
        do_post: Callable[..., Response],
    ) -> Response | ProblemDetail:
        """Send the request that actually kicks off the OPDS Directory
        Registration Protocol.

        :return: Either a ProblemDetail or a requests-like Response object.
        """
        # Allow 400 and 401 so we can provide a more useful error message.
        response = do_post(
            register_url,
            headers=headers,
            payload=payload,
            timeout=60,
            allowed_response_codes=["2xx", "3xx", "400", "401"],
        )
        if response.status_code in [400, 401]:
            if response.headers.get("Content-Type") == PROBLEM_DETAIL_JSON_MEDIA_TYPE:
                problem = json.loads(response.content)
                return INTEGRATION_ERROR.detailed(
                    _(
                        'Remote service returned: "%(problem)s"',
                        problem=problem.get("detail"),
                    )
                )
            else:
                return INTEGRATION_ERROR.detailed(
                    _(
                        'Remote service returned: "%(problem)s"',
                        problem=response.content.decode("utf-8"),
                    )
                )
        return response

    @classmethod
    def _decrypt_shared_secret(
        cls, cipher: PKCS1OAEP_Cipher, cipher_text: str
    ) -> bytes | ProblemDetail:
        """Attempt to decrypt an encrypted shared secret.

        :param cipher: A Cipher object.

        :param shared_secret: A byte string.

        :return: The decrypted shared secret, as a bytestring, or
        a ProblemDetail if it could not be decrypted.
        """
        try:
            shared_secret = cipher.decrypt(base64.b64decode(cipher_text))
        except ValueError as e:
            return SHARED_SECRET_DECRYPTION_ERROR.detailed(
                _("Could not decrypt shared secret %s") % cipher_text
            )
        return shared_secret

    def _process_registration_result(
        self, catalog: Dict[str, Any], cipher: PKCS1OAEP_Cipher, desired_stage: str
    ) -> Literal[True] | ProblemDetail:
        """We just sent out a registration request and got an OPDS catalog
        in return. Process that catalog.

        :param catalog: A dictionary derived from an OPDS 2 catalog.
        :param cipher: A Cipher object.
        :param desired_stage: Our opinion, as communicated to the
            server, about whether this library is ready to go into
            production.
        """
        # Since every library has a public key, the catalog should have provided
        # credentials for future authenticated communication,
        # e.g. through Short Client Tokens or authenticated API
        # requests.
        if not isinstance(catalog, dict):
            return INTEGRATION_ERROR.detailed(  # type: ignore[unreachable]
                _(
                    "Remote service served %(representation)r, which I can't make sense of as an OPDS document.",
                    representation=catalog,
                )
            )
        metadata: Dict[str, str] = catalog.get("metadata", {})
        short_name = metadata.get("short_name")
        encrypted_shared_secret = metadata.get("shared_secret")
        links = catalog.get("links", [])

        web_client_url = None
        for link in links:
            if link.get("rel") == "self" and link.get("type") == "text/html":
                web_client_url = link.get("href")
                break

        if short_name:
            setting = self.setting(ExternalIntegration.USERNAME)
            setting.value = short_name
        if encrypted_shared_secret:
            shared_secret = self._decrypt_shared_secret(cipher, encrypted_shared_secret)
            if isinstance(shared_secret, ProblemDetail):
                return shared_secret

            setting = self.setting(ExternalIntegration.PASSWORD)

            # NOTE: we can only store Unicode data in the
            # ConfigurationSetting.value, so this requires that the
            # shared secret encoded as UTF-8. This works for the
            # library registry product, which uses a long string of
            # hex digits as its shared secret.
            setting.value = shared_secret.decode("utf8")

        # We have successfully completed the registration.
        self.status_field.value = self.SUCCESS_STATUS

        # Our opinion about the proper stage of this library was succesfully
        # communicated to the registry.
        self.stage_field.value = desired_stage

        # Store the web client URL as a ConfigurationSetting.
        if web_client_url:
            self.web_client_field.value = web_client_url

        return True


class LibraryRegistrationScript(LibraryInputScript):
    """Register local libraries with a remote library registry."""

    PROTOCOL = ExternalIntegration.OPDS_REGISTRATION
    GOAL = ExternalIntegration.DISCOVERY_GOAL

    @classmethod
    def arg_parser(cls, _db: Session) -> ArgumentParser:  # type: ignore[override]
        parser = LibraryInputScript.arg_parser(_db)
        parser.add_argument(
            "--registry-url",
            help="Register libraries with the given registry.",
            default=RemoteRegistry.DEFAULT_LIBRARY_REGISTRY_URL,
        )
        parser.add_argument(
            "--stage",
            help="Register these libraries in the 'testing' stage or the 'production' stage.",
            choices=(Registration.TESTING_STAGE, Registration.PRODUCTION_STAGE),
        )
        return parser  # type: ignore[no-any-return]

    def do_run(
        self,
        cmd_args: Optional[List[str]] = None,
        manager: Optional[CirculationManager] = None,
    ) -> PalaceFlask:
        parser = self.arg_parser(self._db)
        parsed = self.parse_command_line(self._db, cmd_args)

        url = parsed.registry_url
        registry = RemoteRegistry.for_protocol_goal_and_url(
            self._db, self.PROTOCOL, self.GOAL, url
        )
        stage = parsed.stage

        # Set up an application context so we have access to url_for.
        from api.app import app

        app.manager = manager or CirculationManager(self._db)
        base_url = ConfigurationSetting.sitewide(
            self._db, Configuration.BASE_URL_KEY
        ).value
        ctx = app.test_request_context(base_url=base_url)
        ctx.push()
        for library in parsed.libraries:
            registration = Registration(registry, library)
            library_stage = stage or registration.stage_field.value
            self.process_library(registration, library_stage, url_for)
        ctx.pop()

        # For testing purposes, return the application object that was
        # created.
        return app

    def process_library(self, registration: Registration, stage: str, url_for: Callable[..., str]) -> bool | ProblemDetail:  # type: ignore[override]
        """Push one Library's registration to the given RemoteRegistry."""

        logger = logging.getLogger(
            "Registration of library %r" % registration.library.short_name
        )
        logger.info(
            "Registering with %s as %s", registration.registry.integration.url, stage
        )
        try:
            result = registration.push(stage, url_for)
        except Exception as e:
            logger.error("Exception during registration", exc_info=e)
            return False
        if isinstance(result, ProblemDetail):
            data, status_code, headers = result.response
            logger.error(
                "Could not complete registration. Problem detail document: %r" % data
            )
            return result
        else:
            logger.info("Success.")
        return result
