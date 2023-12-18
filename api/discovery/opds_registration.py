from __future__ import annotations

import base64
import json
import sys
from collections.abc import Callable
from typing import Any, Literal, overload

from Crypto.Cipher.PKCS1_OAEP import PKCS1OAEP_Cipher
from flask_babel import lazy_gettext as _
from html_sanitizer import Sanitizer
from pydantic import HttpUrl
from requests import Response
from sqlalchemy import select
from sqlalchemy.orm.session import Session

from api.config import Configuration
from api.problem_details import *
from core.integration.base import HasIntegrationConfiguration
from core.integration.goals import Goals
from core.integration.settings import BaseSettings, ConfigurationFormItem, FormField
from core.model import IntegrationConfiguration, Library, get_one, get_one_or_create
from core.model.discovery_service_registration import (
    DiscoveryServiceRegistration,
    RegistrationStage,
    RegistrationStatus,
)
from core.util.http import HTTP
from core.util.problem_detail import ProblemError

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class OpdsRegistrationServiceSettings(BaseSettings):
    url: HttpUrl = FormField(
        ...,
        form=ConfigurationFormItem(
            label=_("URL"),
            required=True,
        ),
    )


class OpdsRegistrationService(
    HasIntegrationConfiguration[OpdsRegistrationServiceSettings]
):
    """A circulation manager's view of a remote service that supports
    the OPDS Directory Registration Protocol:

    https://github.com/NYPL-Simplified/Simplified/wiki/OPDS-Directory-Registration-Protocol

    In practical terms, this is a library registry (which has
    DISCOVERY_GOAL and wants to help patrons find their libraries).
    """

    DEFAULT_LIBRARY_REGISTRY_URL = "https://registry.thepalaceproject.org"
    DEFAULT_LIBRARY_REGISTRY_NAME = "Palace Library Registry"

    OPDS_2_TYPE = "application/opds+json"

    def __init__(
        self,
        integration: IntegrationConfiguration,
        settings: OpdsRegistrationServiceSettings,
    ) -> None:
        """Constructor."""
        self.integration = integration
        self.settings = settings

    @classmethod
    def label(cls) -> str:
        """Get the label of this integration."""
        return "OPDS Registration"

    @classmethod
    def description(cls) -> str:
        """Get the description of this integration."""
        return "Register your library for discovery in the app with a library registry."

    @classmethod
    def protocol_details(cls, db: Session) -> dict[str, Any]:
        return {
            "sitewide": True,
            "supports_registration": True,
            "supports_staging": True,
        }

    @classmethod
    def settings_class(cls) -> type[OpdsRegistrationServiceSettings]:
        """Get the settings for this integration."""
        return OpdsRegistrationServiceSettings

    @classmethod
    @overload
    def for_integration(cls, _db: Session, integration: int) -> Self | None:
        ...

    @classmethod
    @overload
    def for_integration(
        cls, _db: Session, integration: IntegrationConfiguration
    ) -> Self:
        ...

    @classmethod
    def for_integration(
        cls, _db: Session, integration: int | IntegrationConfiguration
    ) -> Self | None:
        """
        Find a OpdsRegistrationService object configured by the given IntegrationConfiguration ID.
        """
        if isinstance(integration, int):
            integration_obj = get_one(_db, IntegrationConfiguration, id=integration)
        else:
            integration_obj = integration
        if integration_obj is None:
            return None

        settings = cls.settings_load(integration_obj)
        return cls(integration_obj, settings)

    @staticmethod
    def get_request(url: str) -> Response:
        return HTTP.debuggable_get(url)

    @staticmethod
    def post_request(
        url: str, payload: str | dict[str, Any], **kwargs: Any
    ) -> Response:
        return HTTP.debuggable_post(url, payload, **kwargs)

    @classmethod
    def for_protocol_goal_and_url(
        cls, _db: Session, protocol: str, goal: Goals, url: str
    ) -> Self | None:
        """Get a LibraryRegistry for the given protocol, goal, and
        URL. Create the corresponding ExternalIntegration if necessary.
        """
        settings = cls.settings_class().construct(url=url)  # type: ignore[arg-type]
        query = select(IntegrationConfiguration).where(
            IntegrationConfiguration.goal == goal,
            IntegrationConfiguration.protocol == protocol,
            IntegrationConfiguration.settings_dict.contains(settings.dict()),
        )
        integration = _db.scalars(query).one_or_none()
        if not integration:
            return None
        return cls(integration, settings)

    @property
    def registrations(self) -> list[DiscoveryServiceRegistration]:
        """Find all of this site's registrations with this OpdsRegistrationService.

        :yield: A sequence of Registration objects.
        """
        session = Session.object_session(self.integration)
        return session.scalars(
            select(DiscoveryServiceRegistration).where(
                DiscoveryServiceRegistration.integration_id == self.integration.id,
            )
        ).all()

    def fetch_catalog(
        self,
    ) -> tuple[str, str]:
        """Fetch the root catalog for this OpdsRegistrationService.

        :return: A ProblemDetail if there's a problem communicating
            with the service or parsing the catalog; otherwise a 2-tuple
            (registration URL, Adobe vendor ID).
        """
        catalog_url = self.settings.url
        response = self.get_request(catalog_url)
        return self._extract_catalog_information(response)

    @classmethod
    def _extract_catalog_information(cls, response: Response) -> tuple[str, str]:
        """From an OPDS catalog, extract information that's essential to
        kickstarting the OPDS Directory Registration Protocol.

        :param response: A requests-style Response object.

        :return A ProblemDetail if there's a problem accessing the
            catalog; otherwise a 2-tuple (registration URL, Adobe vendor
            ID).
        """
        catalog, links = cls._extract_links(response)
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
            raise ProblemError(
                problem_detail=REMOTE_INTEGRATION_FAILED.detailed(
                    _(
                        "The service at %(url)s did not provide a register link.",
                        url=response.url,
                    )
                )
            )
        return register_url, vendor_id

    def fetch_registration_document(
        self,
    ) -> tuple[str | None, str | None]:
        """Fetch a discovery service's registration document and extract
        useful information from it.

        :return: A ProblemDetail if there's a problem accessing the
            service; otherwise, a 2-tuple (terms_of_service_link,
            terms_of_service_html), containing information about the
            Terms of Service that govern a circulation manager's
            registration with the discovery service.
        """
        registration_url, vendor_id = self.fetch_catalog()
        response = self.get_request(registration_url)
        return self._extract_registration_information(response)

    @classmethod
    def _extract_registration_information(
        cls, response: Response
    ) -> tuple[str | None, str | None]:
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
        try:
            catalog, links = cls._extract_links(response)
        except ProblemError:
            return None, None
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
    ) -> tuple[dict[str, Any] | None, list[dict[str, str]]]:
        """Parse an OPDS 2 feed out of a Requests response object.

        :return: A 2-tuple (parsed_catalog, links),
           with `links` being a list of dictionaries, each containing
           one OPDS link.
        """
        # The response must contain an OPDS 2 catalog.
        type = response.headers.get("Content-Type")
        if not (type and type.startswith(cls.OPDS_2_TYPE)):
            raise ProblemError(
                problem_detail=REMOTE_INTEGRATION_FAILED.detailed(
                    _("The service at %(url)s did not return OPDS.", url=response.url)
                )
            )

        catalog = response.json()
        links = catalog.get("links", [])
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

    def register_library(
        self,
        library: Library,
        stage: RegistrationStage,
        url_for: Callable[..., str],
    ) -> Literal[True]:
        """Attempt to register a library with a OpdsRegistrationService.

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

        :return: Raise a ProblemError if there was a problem; otherwise True.
        """
        db = Session.object_session(library)
        registration, _ = get_one_or_create(
            db,
            DiscoveryServiceRegistration,
            library=library,
            integration=self.integration,
        )

        # Assume that the registration will fail.
        #
        # TODO: If a registration has previously succeeded, failure to
        # re-register probably means a maintenance of the status quo,
        # not a change of success to failure. But we don't have any way
        # of being sure.
        registration.status = RegistrationStatus.FAILURE

        # If the library has no private key, we can't register it. This should never
        # happen because the column isn't nullable. We add an assertion here just in
        # case, so we get a stack trace if it does happen.
        assert library.private_key is not None
        cipher = Configuration.cipher(library.private_key)

        # Before we can start the registration protocol, we must fetch
        # the remote catalog's URL and extract the link to the
        # registration resource that kicks off the protocol.
        register_url, vendor_id = self.fetch_catalog()

        if vendor_id:
            registration.vendor_id = vendor_id

        # Build the document we'll be sending to the registration URL.
        payload = self._create_registration_payload(library, stage, url_for)
        headers = self._create_registration_headers(registration)

        # Send the document.
        response = self._send_registration_request(register_url, headers, payload)
        catalog = json.loads(response.content)

        # Process the result.
        return self._process_registration_result(registration, catalog, cipher, stage)

    @staticmethod
    def _create_registration_payload(
        library: Library,
        stage: RegistrationStage,
        url_for: Callable[..., str],
    ) -> dict[str, str]:
        """Collect the key-value pairs to be sent when kicking off the
        registration protocol.

        :param library: The library to be registered.
        :param stage: The registrant's opinion about what stage this
           registration should be in.
        :param url_for: An implementation of Flask url_for.

        :return: A dictionary suitable for passing into requests.post.
        """
        auth_document_url = url_for(
            "authentication_document",
            library_short_name=library.short_name,
            _external=True,
        )
        payload = dict(url=auth_document_url, stage=stage.value)

        # Find the email address the administrator should use if they notice
        # a problem with the way the library is using an integration.
        contact = Configuration.configuration_contact_uri(library)
        if contact:
            payload["contact"] = contact
        return payload

    @staticmethod
    def _create_registration_headers(
        registration: DiscoveryServiceRegistration,
    ) -> dict[str, str]:
        shared_secret = registration.shared_secret
        headers = {}
        if shared_secret:
            headers["Authorization"] = f"Bearer {shared_secret}"
        return headers

    @classmethod
    def _send_registration_request(
        cls,
        register_url: str,
        headers: dict[str, str],
        payload: dict[str, str],
    ) -> Response:
        """Send the request that actually kicks off the OPDS Directory
        Registration Protocol.

        :return: A requests-like Response object or raise a ProblemError on failure.
        """
        response = cls.post_request(
            register_url,
            headers=headers,
            payload=payload,
            timeout=60,
            allowed_response_codes=["2xx", "3xx"],
        )
        return response

    @classmethod
    def _decrypt_shared_secret(
        cls, cipher: PKCS1OAEP_Cipher, cipher_text: str
    ) -> bytes:
        """Attempt to decrypt an encrypted shared secret.

        :param cipher: A Cipher object.

        :param shared_secret: A byte string.

        :return: The decrypted shared secret, as a bytestring, or
        raise as ProblemError if it could not be decrypted.
        """
        try:
            shared_secret = cipher.decrypt(base64.b64decode(cipher_text))
        except ValueError:
            raise ProblemError(
                problem_detail=SHARED_SECRET_DECRYPTION_ERROR.detailed(
                    f"Could not decrypt shared secret: '{cipher_text}'"
                )
            )
        return shared_secret

    @classmethod
    def _process_registration_result(
        cls,
        registration: DiscoveryServiceRegistration,
        catalog: dict[str, Any] | Any,
        cipher: PKCS1OAEP_Cipher,
        desired_stage: RegistrationStage,
    ) -> Literal[True]:
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
            raise ProblemError(
                problem_detail=INTEGRATION_ERROR.detailed(
                    f"Remote service served '{catalog}', which I can't make sense of as an OPDS document.",
                )
            )
        metadata: dict[str, str] = catalog.get("metadata", {})
        short_name = metadata.get("short_name")
        encrypted_shared_secret = metadata.get("shared_secret")
        links = catalog.get("links", [])

        web_client_url = None
        for link in links:
            if link.get("rel") == "self" and link.get("type") == "text/html":
                web_client_url = link.get("href")
                break

        if short_name:
            registration.short_name = short_name
        if encrypted_shared_secret:
            # NOTE: we can only store Unicode data in the
            # ConfigurationSetting.value, so this requires that the
            # shared secret encoded as UTF-8. This works for the
            # library registry product, which uses a long string of
            # hex digits as its shared secret.
            registration.shared_secret = cls._decrypt_shared_secret(
                cipher, encrypted_shared_secret
            ).decode("utf-8")

        # We have successfully completed the registration.
        registration.status = RegistrationStatus.SUCCESS

        # Our opinion about the proper stage of this library was successfully
        # communicated to the registry.
        registration.stage = desired_stage

        # Store the web client URL as a ConfigurationSetting.
        registration.web_client = web_client_url

        return True
