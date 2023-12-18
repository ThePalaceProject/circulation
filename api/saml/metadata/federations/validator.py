import datetime
import logging
from abc import ABCMeta

from onelogin.saml2.utils import OneLogin_Saml2_Utils
from onelogin.saml2.xmlparser import fromstring

from api.saml.metadata.federations.model import SAMLFederation
from core.exceptions import BaseError
from core.util.datetime_helpers import from_timestamp, utc_now


class SAMLFederatedMetadataValidationError(BaseError):
    """Raised in the case of any errors happened during SAML metadata validation."""


class SAMLFederatedMetadataValidator(metaclass=ABCMeta):
    """Base class for all validators checking correctness of SAML federated metadata."""

    def validate(self, federation, metadata):
        """Validate SAML federated metadata.

        :param federation: SAML federation
        :type federation: api.saml.metadata.federations.model.SAMLFederation

        :param metadata: SAML federation's aggregated metadata
        :type metadata: str

        :raises SAMLFederatedMetadataValidationError: in the case of validation errors
        """
        raise NotImplementedError


class SAMLFederatedMetadataValidatorChain(SAMLFederatedMetadataValidator):
    def __init__(self, validators):
        """Initialize a new instance of SAMLFederatedMetadataValidatorChain class.

        :param validators: List of validators
        :type validators: List[SAMLFederatedMetadataValidator]
        """
        if not validators or not isinstance(validators, list):
            raise ValueError("Argument 'validators' must be a non-empty list")

        for validator in validators:
            if not isinstance(validator, SAMLFederatedMetadataValidator):
                raise ValueError(
                    "Argument 'validators' must contain only instances of {} class".format(
                        SAMLFederatedMetadataValidator
                    )
                )

        self._validators = validators

    def validate(self, federation, metadata):
        """Validate SAML federated metadata using a chain of inner validators.

        :param federation: SAML federation
        :type federation: api.saml.metadata.federations.model.SAMLFederation

        :param metadata: SAML federation's aggregated metadata
        :type metadata: str

        :raises SAMLFederatedMetadataValidationError: in the case of validation errors
        """
        for validator in self._validators:
            validator.validate(federation, metadata)


class SAMLFederatedMetadataExpirationValidator(SAMLFederatedMetadataValidator):
    """Verifies that federated SAML metadata has not expired."""

    # We allow the metadata's expiration time to be only 5 minutes behind.
    MAX_CLOCK_SKEW = datetime.timedelta(minutes=5)

    # We allow the metadata's expiration time to be only 4 week ahead.
    MAX_VALID_TIME = datetime.timedelta(weeks=4)

    def __init__(self):
        """Initialize a new instance of SAMLFederatedMetadataExpirationValidator class."""
        self._logger = logging.getLogger(__name__)

    @staticmethod
    def _parse_saml_date_time(saml_date_time):
        """Parse the string containing date & time information in the SAML format into datetime object.

        :param saml_date_time: String containing date & time information in the SAML format
        :type saml_date_time: str
        """
        unix_timestamp = OneLogin_Saml2_Utils.parse_SAML_to_time(saml_date_time)
        parsed_date_time = from_timestamp(unix_timestamp)

        return parsed_date_time

    def validate(self, federation: SAMLFederation, metadata: str | bytes) -> None:
        """Verify that federated SAML metadata has not expired.

        :param federation: SAML federation

        :param metadata: SAML federation's aggregated metadata

        :raises SAMLFederatedMetadataValidationError: in the case of validation errors
        """
        self._logger.info(
            "Started validating the expiration time of the metadata belonging to {}".format(
                federation
            )
        )

        if isinstance(metadata, str):
            metadata = metadata.encode()

        try:
            root = fromstring(metadata)
        except Exception as exception:
            raise SAMLFederatedMetadataValidationError(
                "Metadata's XML is not valid", exception
            )

        if "EntitiesDescriptor" not in root.tag:
            raise SAMLFederatedMetadataValidationError(
                'Metadata\'s root element is not "EntitiesDescriptor"'
            )

        valid_until = root.get("validUntil", None)
        if not valid_until:
            raise SAMLFederatedMetadataValidationError(
                'Metadata does not contain "validUntil" attribute'
            )

        valid_until = self._parse_saml_date_time(valid_until)
        now = utc_now()

        if valid_until < now and (now - valid_until) > self.MAX_CLOCK_SKEW:
            raise SAMLFederatedMetadataValidationError(
                "Metadata has already expired. "
                '"validUntil" is {} while the current time is {}'.format(
                    valid_until, now
                )
            )

        if valid_until > now and (valid_until - now) > self.MAX_VALID_TIME:
            raise SAMLFederatedMetadataValidationError(
                "Expiration time is unexpectedly far into the future. "
                '"validUntil" is {} while the current time is {}'.format(
                    valid_until, now
                )
            )

        self._logger.info(
            "Finished validating the expiration time of the metadata belonging to {}".format(
                federation
            )
        )


class SAMLMetadataSignatureValidator(SAMLFederatedMetadataValidator):
    """Verifies the validity of federated SAML metadata's signature."""

    def __init__(self):
        """Initialize a new instance of SAMLMetadataSignatureValidator class."""
        self._logger = logging.getLogger(__name__)

    def validate(self, federation, metadata):
        """Verify the validity of the SAML federated metadata's signature.

        :param federation: SAML federation
        :type federation: api.saml.metadata.federations.model.SAMLFederation

        :param metadata: SAML federation's aggregated metadata
        :type metadata: str

        :raises SAMLFederatedMetadataValidationError: in the case of validation errors
        """
        self._logger.info(
            "Started verifying the validity of the metadata's signature belonging to {}".format(
                federation
            )
        )

        try:
            OneLogin_Saml2_Utils.validate_metadata_sign(
                metadata, federation.certificate, raise_exceptions=True
            )
        except Exception as exception:
            raise SAMLFederatedMetadataValidationError(str(exception), exception)

        self._logger.info(
            "Finished verifying the validity of the metadata's signature belonging to {}".format(
                federation
            )
        )
