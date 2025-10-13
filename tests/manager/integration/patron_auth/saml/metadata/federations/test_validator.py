import datetime

import pytest
from freezegun import freeze_time

import tests.mocks.saml_strings as fixtures
from palace.manager.integration.patron_auth.saml.metadata.federations import incommon
from palace.manager.integration.patron_auth.saml.metadata.federations.validator import (
    SAMLFederatedMetadataExpirationValidator,
    SAMLFederatedMetadataValidationError,
    SAMLMetadataSignatureValidator,
)
from palace.manager.sqlalchemy.model.saml import SAMLFederation
from palace.manager.util.datetime_helpers import datetime_utc, utc_now
from tests.fixtures.files import FilesFixture

SamlFilesFixture = FilesFixture("saml")


class TestSAMLFederatedMetadataExpirationValidator:
    @pytest.mark.parametrize(
        "current_time,metadata,expected_exception",
        [
            pytest.param(
                utc_now(),
                fixtures.EMPTY_XML,
                SAMLFederatedMetadataValidationError,
                id="incorrect_xml_str_type",
            ),
            pytest.param(
                utc_now(),
                fixtures.EMPTY_XML.encode(),
                SAMLFederatedMetadataValidationError,
                id="incorrect_xml_bytes_type",
            ),
            pytest.param(
                utc_now(),
                fixtures.FEDERATED_METADATA_WITHOUT_VALID_UNTIL_ATTRIBUTE,
                SAMLFederatedMetadataValidationError,
                id="without_valid_until_attribute_metadata_str_type",
            ),
            pytest.param(
                utc_now(),
                fixtures.FEDERATED_METADATA_WITHOUT_VALID_UNTIL_ATTRIBUTE.encode(),
                SAMLFederatedMetadataValidationError,
                id="without_valid_until_attribute_metadata_bytes_type",
            ),
            pytest.param(
                fixtures.FEDERATED_METADATA_VALID_UNTIL
                + SAMLFederatedMetadataExpirationValidator.MAX_CLOCK_SKEW
                + datetime.timedelta(minutes=1),
                fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE,
                SAMLFederatedMetadataValidationError,
                id="with_expired_valid_until_attribute_metadata_str_type",
            ),
            pytest.param(
                fixtures.FEDERATED_METADATA_VALID_UNTIL
                + SAMLFederatedMetadataExpirationValidator.MAX_CLOCK_SKEW
                + datetime.timedelta(minutes=1),
                fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE.encode(),
                SAMLFederatedMetadataValidationError,
                id="with_expired_valid_until_attribute_metadata_bytes_type",
            ),
            pytest.param(
                fixtures.FEDERATED_METADATA_VALID_UNTIL
                - SAMLFederatedMetadataExpirationValidator.MAX_VALID_TIME
                - datetime.timedelta(minutes=1),
                fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE,
                SAMLFederatedMetadataValidationError,
                id="with_valid_until_attribute_too_far_in_the_future_metadata_str_type",
            ),
            pytest.param(
                fixtures.FEDERATED_METADATA_VALID_UNTIL
                - SAMLFederatedMetadataExpirationValidator.MAX_VALID_TIME
                - datetime.timedelta(minutes=1),
                fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE.encode(),
                SAMLFederatedMetadataValidationError,
                id="with_valid_until_attribute_too_far_in_the_future_metadata_bytes_type",
            ),
            pytest.param(
                fixtures.FEDERATED_METADATA_VALID_UNTIL
                + SAMLFederatedMetadataExpirationValidator.MAX_CLOCK_SKEW,
                fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE,
                None,
                id="with_valid_until_attribute_less_than_current_time_and_less_than_max_clock_skew_metadata_str_type",
            ),
            pytest.param(
                fixtures.FEDERATED_METADATA_VALID_UNTIL
                + SAMLFederatedMetadataExpirationValidator.MAX_CLOCK_SKEW,
                fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE.encode(),
                None,
                id="with_valid_until_attribute_less_than_current_time_and_less_than_max_clock_skew_metadata_bytes_type",
            ),
            pytest.param(
                fixtures.FEDERATED_METADATA_VALID_UNTIL
                - SAMLFederatedMetadataExpirationValidator.MAX_VALID_TIME
                + datetime.timedelta(minutes=1),
                fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE,
                None,
                id="with_valid_until_attribute_greater_than_current_time_and_less_than_max_valid_time_metadata_str_type",
            ),
            pytest.param(
                fixtures.FEDERATED_METADATA_VALID_UNTIL
                - SAMLFederatedMetadataExpirationValidator.MAX_VALID_TIME
                + datetime.timedelta(minutes=1),
                fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE.encode(),
                None,
                id="with_valid_until_attribute_greater_than_current_time_and_less_than_max_valid_time_metadata_bytes_type",
            ),
            pytest.param(
                datetime_utc(2020, 11, 26, 14, 32, 42),
                SamlFilesFixture.sample_text("incommon-metadata-idp-only.xml"),
                None,
                id="with_real_incommon_metadata_str_type",
            ),
            pytest.param(
                datetime_utc(2020, 11, 26, 14, 32, 42),
                SamlFilesFixture.sample_data("incommon-metadata-idp-only.xml"),
                None,
                id="with_real_incommon_metadata_bytes_type",
            ),
        ],
    )
    def test_validate(
        self,
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
        "certificate,metadata,expected_exception",
        [
            pytest.param(
                fixtures.FEDERATED_METADATA_CERTIFICATE,
                fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE,
                SAMLFederatedMetadataValidationError,
                id="without_signature",
            ),
            pytest.param(
                fixtures.FEDERATED_METADATA_CERTIFICATE.strip(),
                fixtures.FEDERATED_METADATA_WITH_INVALID_SIGNATURE,
                SAMLFederatedMetadataValidationError,
                id="with_invalid_signature",
            ),
            pytest.param(
                fixtures.FEDERATED_METADATA_CERTIFICATE.strip(),
                SamlFilesFixture.sample_text("incommon-metadata-idp-only.xml"),
                None,
                id="with_valid_signature",
            ),
        ],
    )
    def test_validate(self, certificate, metadata, expected_exception):
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
