from __future__ import annotations

import json
import os
import urllib.parse
from typing import TYPE_CHECKING, Optional

import requests
from flask_babel import lazy_gettext as _
from requests.auth import HTTPBasicAuth

from api.lcp import utils
from api.lcp.encrypt import LCPEncryptionResult, LCPEncryptorResultJSONEncoder
from api.lcp.hash import HashingAlgorithm
from core.integration.settings import (
    BaseSettings,
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
)
from core.lcp.credential import LCPHashedPassphrase, LCPUnhashedPassphrase
from core.model.configuration import (
    ConfigurationAttributeType,
    ConfigurationGrouping,
    ConfigurationMetadata,
    ConfigurationOption,
)

if TYPE_CHECKING:
    pass


class LCPServerConstants:
    DEFAULT_PAGE_SIZE = 100
    DEFAULT_PASSPHRASE_HINT = (
        "If you do not remember your passphrase, please contact your administrator"
    )
    DEFAULT_ENCRYPTION_ALGORITHM = HashingAlgorithm.SHA256.value


class LCPServerConfiguration(ConfigurationGrouping):
    """Contains LCP License Server's settings"""

    DEFAULT_PAGE_SIZE = 100
    DEFAULT_PASSPHRASE_HINT = (
        "If you do not remember your passphrase, please contact your administrator"
    )
    DEFAULT_ENCRYPTION_ALGORITHM = HashingAlgorithm.SHA256.value

    lcpserver_url = ConfigurationMetadata(
        key="lcpserver_url",
        label=_("LCP License Server's URL"),
        description=_("URL of the LCP License Server"),
        type=ConfigurationAttributeType.TEXT,
        required=True,
    )

    lcpserver_user = ConfigurationMetadata(
        key="lcpserver_user",
        label=_("LCP License Server's user"),
        description=_("Name of the user used to connect to the LCP License Server"),
        type=ConfigurationAttributeType.TEXT,
        required=True,
    )

    lcpserver_password = ConfigurationMetadata(
        key="lcpserver_password",
        label=_("LCP License Server's password"),
        description=_("Password of the user used to connect to the LCP License Server"),
        type=ConfigurationAttributeType.TEXT,
        required=True,
    )

    lcpserver_input_directory = ConfigurationMetadata(
        key="lcpserver_input_directory",
        label=_("LCP License Server's input directory"),
        description=_(
            "Full path to the directory containing encrypted books. "
            "This directory should be the same as lcpencrypt's output directory"
        ),
        type=ConfigurationAttributeType.TEXT,
        required=True,
    )

    lcpserver_page_size = ConfigurationMetadata(
        key="lcpserver_page_size",
        label=_("LCP License Server's page size"),
        description=_("Number of licences returned by the server"),
        type=ConfigurationAttributeType.NUMBER,
        required=False,
        default=DEFAULT_PAGE_SIZE,
    )

    provider_name = ConfigurationMetadata(
        key="provider_name",
        label=_("LCP service provider's identifier"),
        description=_("URI that identifies the provider in an unambiguous way"),
        type=ConfigurationAttributeType.TEXT,
        required=True,
    )

    passphrase_hint = ConfigurationMetadata(
        key="passphrase_hint",
        label=_("Passphrase hint"),
        description=_("Hint proposed to the user for selecting their passphrase"),
        type=ConfigurationAttributeType.TEXT,
        required=False,
        default=DEFAULT_PASSPHRASE_HINT,
    )

    encryption_algorithm = ConfigurationMetadata(
        key="encryption_algorithm",
        label=_("Passphrase encryption algorithm"),
        description=_("Algorithm used for encrypting the passphrase"),
        type=ConfigurationAttributeType.SELECT,
        required=False,
        default=DEFAULT_ENCRYPTION_ALGORITHM,
        options=ConfigurationOption.from_enum(HashingAlgorithm),
    )

    max_printable_pages = ConfigurationMetadata(
        key="max_printable_pages",
        label=_("Maximum number or printable pages"),
        description=_(
            "Maximum number of pages that can be printed over the lifetime of the license"
        ),
        type=ConfigurationAttributeType.NUMBER,
        required=False,
    )

    max_copiable_pages = ConfigurationMetadata(
        key="max_copiable_pages",
        label=_("Maximum number or copiable characters"),
        description=_(
            "Maximum number of characters that can be copied to the clipboard"
        ),
        type=ConfigurationAttributeType.NUMBER,
        required=False,
    )


class LCPServerSettings(BaseSettings):
    lcpserver_url: str = FormField(
        form=ConfigurationFormItem(
            label=_("LCP License Server's URL"),
            description=_("URL of the LCP License Server"),
            type=ConfigurationFormItemType.TEXT,
            required=True,
        )
    )

    lcpserver_user: str = FormField(
        form=ConfigurationFormItem(
            label=_("LCP License Server's user"),
            description=_("Name of the user used to connect to the LCP License Server"),
            type=ConfigurationFormItemType.TEXT,
            required=True,
        )
    )

    lcpserver_password: str = FormField(
        form=ConfigurationFormItem(
            label=_("LCP License Server's password"),
            description=_(
                "Password of the user used to connect to the LCP License Server"
            ),
            type=ConfigurationFormItemType.TEXT,
            required=True,
        )
    )

    lcpserver_input_directory: str = FormField(
        form=ConfigurationFormItem(
            label=_("LCP License Server's input directory"),
            description=_(
                "Full path to the directory containing encrypted books. "
                "This directory should be the same as lcpencrypt's output directory"
            ),
            type=ConfigurationFormItemType.TEXT,
            required=True,
        )
    )

    lcpserver_page_size: Optional[int] = FormField(
        default=LCPServerConstants.DEFAULT_PAGE_SIZE,
        form=ConfigurationFormItem(
            label=_("LCP License Server's page size"),
            description=_("Number of licences returned by the server"),
            type=ConfigurationFormItemType.NUMBER,
            required=False,
        ),
    )

    provider_name: str = FormField(
        form=ConfigurationFormItem(
            label=_("LCP service provider's identifier"),
            description=_("URI that identifies the provider in an unambiguous way"),
            type=ConfigurationFormItemType.TEXT,
            required=True,
        )
    )

    passphrase_hint: Optional[str] = FormField(
        default=LCPServerConstants.DEFAULT_PASSPHRASE_HINT,
        form=ConfigurationFormItem(
            label=_("Passphrase hint"),
            description=_("Hint proposed to the user for selecting their passphrase"),
            type=ConfigurationFormItemType.TEXT,
            required=False,
        ),
    )

    encryption_algorithm: Optional[str] = FormField(
        default=LCPServerConstants.DEFAULT_ENCRYPTION_ALGORITHM,
        form=ConfigurationFormItem(
            label=_("Passphrase encryption algorithm"),
            description=_("Algorithm used for encrypting the passphrase"),
            type=ConfigurationFormItemType.SELECT,
            required=False,
            options=ConfigurationFormItemType.options_from_enum(HashingAlgorithm),
        ),
    )

    max_printable_pages: Optional[int] = FormField(
        form=ConfigurationFormItem(
            label=_("Maximum number or printable pages"),
            description=_(
                "Maximum number of pages that can be printed over the lifetime of the license"
            ),
            type=ConfigurationFormItemType.NUMBER,
            required=False,
        ),
    )

    max_copiable_pages: Optional[int] = FormField(
        form=ConfigurationFormItem(
            label=_("Maximum number or copiable characters"),
            description=_(
                "Maximum number of characters that can be copied to the clipboard"
            ),
            type=ConfigurationFormItemType.NUMBER,
            required=False,
        ),
    )


class LCPServer:
    """Wrapper around LCP License Server's API"""

    def __init__(
        self,
        get_configuration,
        hasher_factory,
        credential_factory,
    ):
        """Initializes a new instance of LCPServer class

        :param hasher_factory: Factory responsible for creating Hasher implementations
        :type hasher_factory: hash.HasherFactory

        :param credential_factory: Factory responsible for creating Hasher implementations
        :type credential_factory: credential.CredentialFactory
        """
        self.get_configuration = get_configuration
        self._hasher_factory = hasher_factory
        self._credential_factory = credential_factory
        self._hasher_instance = None

    def _get_hasher(self):
        """Returns a Hasher instance

        :return: Hasher instance
        :rtype: hash.Hasher
        """
        if self._hasher_instance is None:
            self._hasher_instance = self._hasher_factory.create(
                self.get_configuration().encryption_algorithm
            )

        return self._hasher_instance

    def _create_partial_license(self, db, patron, license_start=None, license_end=None):
        """Creates a partial LCP license used an input by the LCP License Server for generation of LCP licenses

        :param patron: Patron object
        :type patron: Patron

        :param license_start: Date and time when the license begins
        :type license_start: Optional[datetime.datetime]

        :param license_end: Date and time when the license ends
        :type license_end: Optional[datetime.datetime]

        :return: Partial LCP license
        :rtype: Dict
        """
        hasher = self._get_hasher()
        unhashed_passphrase: LCPUnhashedPassphrase = (
            self._credential_factory.get_patron_passphrase(db, patron)
        )
        hashed_passphrase: LCPHashedPassphrase = unhashed_passphrase.hash(hasher)
        self._credential_factory.set_hashed_passphrase(db, patron, hashed_passphrase)

        config = self.get_configuration()
        partial_license = {
            "provider": config.provider_name,
            "encryption": {
                "user_key": {
                    "text_hint": config.passphrase_hint,
                    "hex_value": hashed_passphrase.hashed,
                }
            },
        }

        if patron:
            partial_license["user"] = {
                "id": self._credential_factory.get_patron_id(db, patron)
            }

        rights_fields = [
            license_start,
            license_end,
            config.max_printable_pages,
            config.max_copiable_pages,
        ]

        if any(
            [
                rights_field is not None and rights_field != ""
                for rights_field in rights_fields
            ]
        ):
            partial_license["rights"] = {}

        if license_start:
            partial_license["rights"]["start"] = utils.format_datetime(license_start)
        if license_end:
            partial_license["rights"]["end"] = utils.format_datetime(license_end)
        if config.max_printable_pages is not None and config.max_printable_pages != "":
            partial_license["rights"]["print"] = int(config.max_printable_pages)
        if config.max_copiable_pages is not None and config.max_copiable_pages != "":
            partial_license["rights"]["copy"] = int(config.max_copiable_pages)

        return partial_license

    @staticmethod
    def _send_request(configuration, method, path, payload, json_encoder=None):
        """Sends a request to the LCP License Server

        :param path: URL path part
        :type path: string

        :param payload: Dictionary containing request's payload (should be JSON compatible)
        :type payload: Union[Dict, object]

        :param json_encoder: JSON encoder
        :type json_encoder: JSONEncoder

        :return: Dictionary containing LCP License Server's response
        :rtype: Dict
        """
        json_payload = json.dumps(payload, cls=json_encoder)
        url = urllib.parse.urljoin(configuration.lcpserver_url, path)
        response = requests.request(
            method,
            url,
            data=json_payload,
            headers={"Content-Type": "application/json"},
            auth=HTTPBasicAuth(
                configuration.lcpserver_user, configuration.lcpserver_password
            ),
        )

        response.raise_for_status()

        return response

    def add_content(self, db, encrypted_content):
        """Notifies LCP License Server about new encrypted content

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param encrypted_content: LCPEncryptionResult object containing information about encrypted content
        :type encrypted_content: LCPEncryptionResult
        """
        config = self.get_configuration()
        content_location = os.path.join(
            config.lcpserver_input_directory,
            encrypted_content.protected_content_disposition,
        )
        payload = LCPEncryptionResult(
            content_id=encrypted_content.content_id,
            content_encryption_key=encrypted_content.content_encryption_key,
            protected_content_location=content_location,
            protected_content_disposition=encrypted_content.protected_content_disposition,
            protected_content_type=encrypted_content.protected_content_type,
            protected_content_length=encrypted_content.protected_content_length,
            protected_content_sha256=encrypted_content.protected_content_sha256,
        )
        path = f"/contents/{encrypted_content.content_id}"

        self._send_request(config, "put", path, payload, LCPEncryptorResultJSONEncoder)

    def generate_license(self, db, content_id, patron, license_start, license_end):
        """Generates a new LCP license

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param content_id: Unique content ID
        :type content_id: string

        :param patron: Patron object
        :type patron: Patron

        :param license_start: Unique patron ID
        :type license_start: string

        :param license_start: Date and time when the license begins
        :type license_start: datetime.datetime

        :param license_end: Date and time when the license ends
        :type license_end: datetime.datetime

        :return: LCP license
        :rtype: Dict
        """
        partial_license_payload = self._create_partial_license(
            db, patron, license_start, license_end
        )
        path = f"contents/{content_id}/license"
        response = self._send_request(
            self.get_configuration(), "post", path, partial_license_payload
        )

        return response.json()

    def get_license(self, db, license_id, patron):
        """Returns an existing license

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param license_id: License's ID
        :type license_id: int

        :param patron: Patron object
        :type patron: Patron

        :return: Existing license
        :rtype: string
        """
        partial_license_payload = self._create_partial_license(db, patron)
        path = f"licenses/{license_id}"

        response = self._send_request(
            self.get_configuration(), "post", path, partial_license_payload
        )

        return response.json()
