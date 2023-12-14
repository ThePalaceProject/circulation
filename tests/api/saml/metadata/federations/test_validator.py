import datetime
import os

import pytest
from freezegun import freeze_time

import tests.api.saml.saml_strings as fixtures
from api.saml.metadata.federations import incommon
from api.saml.metadata.federations.model import SAMLFederation
from api.saml.metadata.federations.validator import (
    SAMLFederatedMetadataExpirationValidator,
    SAMLFederatedMetadataValidationError,
    SAMLMetadataSignatureValidator,
)
from core.util.datetime_helpers import datetime_utc, utc_now


class TestSAMLFederatedMetadataExpirationValidator:
    @pytest.mark.parametrize(
        "_,current_time,metadata,expected_exception",
        [
            (
                "incorrect_xml_str_type",
                utc_now(),
                fixtures.INCORRECT_XML,
                SAMLFederatedMetadataValidationError,
            ),
            (
                "incorrect_xml_bytes_type",
                utc_now(),
                fixtures.INCORRECT_XML.encode(),
                SAMLFederatedMetadataValidationError,
            ),
            (
                "without_valid_until_attribute_metadata_str_type",
                utc_now(),
                fixtures.FEDERATED_METADATA_WITHOUT_VALID_UNTIL_ATTRIBUTE,
                SAMLFederatedMetadataValidationError,
            ),
            (
                "without_valid_until_attribute_metadata_bytes_type",
                utc_now(),
                fixtures.FEDERATED_METADATA_WITHOUT_VALID_UNTIL_ATTRIBUTE.encode(),
                SAMLFederatedMetadataValidationError,
            ),
            (
                "with_expired_valid_until_attribute_metadata_str_type",
                fixtures.FEDERATED_METADATA_VALID_UNTIL
                + SAMLFederatedMetadataExpirationValidator.MAX_CLOCK_SKEW
                + datetime.timedelta(minutes=1),
                fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE,
                SAMLFederatedMetadataValidationError,
            ),
            (
                "with_expired_valid_until_attribute_metadata_bytes_type",
                fixtures.FEDERATED_METADATA_VALID_UNTIL
                + SAMLFederatedMetadataExpirationValidator.MAX_CLOCK_SKEW
                + datetime.timedelta(minutes=1),
                fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE.encode(),
                SAMLFederatedMetadataValidationError,
            ),
            (
                "with_valid_until_attribute_too_far_in_the_future_metadata_str_type",
                fixtures.FEDERATED_METADATA_VALID_UNTIL
                - SAMLFederatedMetadataExpirationValidator.MAX_VALID_TIME
                - datetime.timedelta(minutes=1),
                fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE,
                SAMLFederatedMetadataValidationError,
            ),
            (
                "with_valid_until_attribute_too_far_in_the_future_metadata_bytes_type",
                fixtures.FEDERATED_METADATA_VALID_UNTIL
                - SAMLFederatedMetadataExpirationValidator.MAX_VALID_TIME
                - datetime.timedelta(minutes=1),
                fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE.encode(),
                SAMLFederatedMetadataValidationError,
            ),
            (
                "with_valid_until_attribute_less_than_current_time_and_less_than_max_clock_skew_metadata_str_type",
                fixtures.FEDERATED_METADATA_VALID_UNTIL
                + SAMLFederatedMetadataExpirationValidator.MAX_CLOCK_SKEW,
                fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE,
                None,
            ),
            (
                "with_valid_until_attribute_less_than_current_time_and_less_than_max_clock_skew_metadata_bytes_type",
                fixtures.FEDERATED_METADATA_VALID_UNTIL
                + SAMLFederatedMetadataExpirationValidator.MAX_CLOCK_SKEW,
                fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE.encode(),
                None,
            ),
            (
                "with_valid_until_attribute_greater_than_current_time_and_less_than_max_valid_time_metadata_str_type",
                fixtures.FEDERATED_METADATA_VALID_UNTIL
                - SAMLFederatedMetadataExpirationValidator.MAX_VALID_TIME
                + datetime.timedelta(minutes=1),
                fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE,
                None,
            ),
            (
                "with_valid_until_attribute_greater_than_current_time_and_less_than_max_valid_time_metadata_bytes_type",
                fixtures.FEDERATED_METADATA_VALID_UNTIL
                - SAMLFederatedMetadataExpirationValidator.MAX_VALID_TIME
                + datetime.timedelta(minutes=1),
                fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE.encode(),
                None,
            ),
            (
                "with_real_incommon_metadata_str_type",
                datetime_utc(2020, 11, 26, 14, 32, 42),
                open(
                    os.path.join(
                        os.path.dirname(os.path.abspath(__file__)),
                        "../../../files/saml/incommon-metadata-idp-only.xml",
                    )
                ).read(),
                None,
            ),
            (
                "with_real_incommon_metadata_bytes_type",
                datetime_utc(2020, 11, 26, 14, 32, 42),
                open(
                    os.path.join(
                        os.path.dirname(os.path.abspath(__file__)),
                        "../../../files/saml/incommon-metadata-idp-only.xml",
                    )
                )
                .read()
                .encode(),
                None,
            ),
        ],
    )
    def test_validate(
        self,
        _,
        current_time: datetime.datetime,
        metadata: str | bytes,
        expected_exception: type[Exception] | None,
    ):
        # Arrange
        validator = SAMLFederatedMetadataExpirationValidator()
        federation = SAMLFederation(
            incommon.FEDERATION_TYPE, incommon.IDP_METADATA_SERVICE_URL
        )

        # Act, assert
        with freeze_time(current_time):
            if expected_exception:
                with pytest.raises(expected_exception):
                    validator.validate(federation, metadata)
            else:
                validator.validate(federation, metadata)


class TestSAMLMetadataSignatureValidator:
    @pytest.mark.parametrize(
        "_,certificate,metadata,expected_exception",
        [
            (
                "without_signature",
                fixtures.FEDERATED_METADATA_CERTIFICATE,
                fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE,
                SAMLFederatedMetadataValidationError,
            ),
            (
                "with_invalid_signature",
                fixtures.FEDERATED_METADATA_CERTIFICATE.strip(),
                fixtures.FEDERATED_METADATA_WITH_INVALID_SIGNATURE,
                SAMLFederatedMetadataValidationError,
            ),
            (
                "with_valid_signature",
                fixtures.FEDERATED_METADATA_CERTIFICATE.strip(),
                open(
                    os.path.join(
                        os.path.dirname(os.path.abspath(__file__)),
                        "../../../files/saml/incommon-metadata-idp-only.xml",
                    )
                ).read(),
                None,
            ),
        ],
    )
    def test_validate(self, _, certificate, metadata, expected_exception):
        # Arrange
        validator = SAMLMetadataSignatureValidator()
        federation = SAMLFederation(
            incommon.FEDERATION_TYPE, incommon.IDP_METADATA_SERVICE_URL
        )
        federation.certificate = certificate

        # Act, assert
        if expected_exception:
            with pytest.raises(expected_exception):
                validator.validate(federation, metadata)
        else:
            validator.validate(federation, metadata)
