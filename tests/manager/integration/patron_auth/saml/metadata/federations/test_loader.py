from unittest.mock import MagicMock, create_autospec, patch

import pytest

from palace.manager.integration.patron_auth.saml.metadata.federations import incommon
from palace.manager.integration.patron_auth.saml.metadata.federations.loader import (
    SAMLFederatedIdentityProviderLoader,
    SAMLMetadataLoader,
    SAMLMetadataLoadingError,
)
from palace.manager.integration.patron_auth.saml.metadata.federations.validator import (
    SAMLFederatedMetadataValidator,
)
from palace.manager.integration.patron_auth.saml.metadata.parser import (
    SAMLMetadataParser,
)
from palace.manager.sqlalchemy.model.saml import SAMLFederation
from tests.mocks import saml_strings


class TestSAMLMetadataLoader:
    @patch("urllib.request.urlopen")
    def test_load_idp_metadata_raises_error_when_xml_is_incorrect(self, urlopen_mock):
        # Arrange
        url = "http://md.incommon.org/InCommon/metadata.xml"
        incorrect_xml = saml_strings.INCORRECT_XML
        urlopen_response_mock = MagicMock()
        urlopen_response_mock.read = MagicMock(return_value=incorrect_xml)
        urlopen_mock.return_value = urlopen_response_mock
        metadata_loader = SAMLMetadataLoader()

        # Act
        with pytest.raises(SAMLMetadataLoadingError):
            metadata_loader.load_idp_metadata(url)

    @patch("urllib.request.Request")
    @patch("urllib.request.urlopen")
    def test_load_idp_metadata_correctly_loads_one_descriptor(
        self, urlopen_mock, request_mock
    ):
        # Arrange
        url = "http://md.incommon.org/InCommon/metadata.xml"
        incorrect_xml = saml_strings.CORRECT_XML_WITH_IDP_1
        urlopen_response_mock = MagicMock()
        urlopen_response_mock.read = MagicMock(return_value=incorrect_xml)
        urlopen_mock.return_value = urlopen_response_mock
        metadata_loader = SAMLMetadataLoader()

        # Act
        xml_metadata = metadata_loader.load_idp_metadata(url)

        # Assert
        request_mock.assert_called_with(url, headers={})
        urlopen_mock.assert_called_with(request_mock(), timeout=None)
        assert saml_strings.CORRECT_XML_WITH_IDP_1 == xml_metadata

    @patch("urllib.request.Request")
    @patch("urllib.request.urlopen")
    def test_load_idp_metadata_correctly_loads_multiple_descriptors(
        self, urlopen_mock, request_mock
    ):
        # Arrange
        url = "http://md.incommon.org/InCommon/metadata.xml"
        incorrect_xml = saml_strings.CORRECT_XML_WITH_MULTIPLE_IDPS
        urlopen_response_mock = MagicMock()
        urlopen_response_mock.read = MagicMock(return_value=incorrect_xml)
        urlopen_mock.return_value = urlopen_response_mock
        metadata_loader = SAMLMetadataLoader()

        # Act
        xml_metadata = metadata_loader.load_idp_metadata(url)

        # Assert
        request_mock.assert_called_with(url, headers={})
        urlopen_mock.assert_called_with(request_mock(), timeout=None)
        assert saml_strings.CORRECT_XML_WITH_MULTIPLE_IDPS == xml_metadata


class TestSAMLFederatedIdentityProviderLoader:
    def test_load(self):
        # Arrange
        federation_type = incommon.FEDERATION_TYPE
        federation_idp_metadata_service_url = incommon.IDP_METADATA_SERVICE_URL
        xml_metadata = saml_strings.CORRECT_XML_WITH_MULTIPLE_IDPS

        metadata_loader = create_autospec(spec=SAMLMetadataLoader)
        metadata_validator = create_autospec(spec=SAMLFederatedMetadataValidator)
        metadata_parser = SAMLMetadataParser()
        idp_loader = SAMLFederatedIdentityProviderLoader(
            metadata_loader, metadata_validator, metadata_parser
        )
        saml_federation = SAMLFederation(
            federation_type, federation_idp_metadata_service_url
        )

        metadata_loader.load_idp_metadata = MagicMock(return_value=xml_metadata)
        metadata_parser.parse = MagicMock(side_effect=metadata_parser.parse)

        # Act
        idps = idp_loader.load(saml_federation)

        # Assert
        assert 2 == len(idps)

        assert saml_strings.IDP_1_ENTITY_ID == idps[0].entity_id
        assert saml_strings.IDP_1_UI_INFO_EN_DISPLAY_NAME == idps[0].display_name
        assert saml_federation == idps[0].federation

        assert saml_strings.IDP_2_ENTITY_ID == idps[1].entity_id
        assert saml_strings.IDP_2_UI_INFO_EN_DISPLAY_NAME == idps[1].display_name
        assert saml_federation == idps[1].federation

        metadata_loader.load_idp_metadata.assert_called_once_with(
            federation_idp_metadata_service_url
        )
        metadata_parser.parse.assert_called_once_with(xml_metadata)
