from unittest.mock import patch

from palace.manager.data_layer.format import FormatData
from palace.manager.data_layer.link import LinkData
from palace.manager.integration.license.opds.bearer_token_drm import BearerTokenDrmMixin
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism, RightsStatus
from palace.manager.sqlalchemy.model.resource import Hyperlink


class TestBearerTokenDrmMixin:
    def test__supported_bearer_token_media_types(self) -> None:
        # If the default client supports media type X with the
        # BEARER_TOKEN access control scheme, then X is a supported
        # media type for an OPDS For Distributors collection.
        supported = BearerTokenDrmMixin._SUPPORTED_BEARER_TOKEN_MEDIA_TYPES
        for format, drm in DeliveryMechanism.default_client_can_fulfill_lookup:
            if drm == (DeliveryMechanism.BEARER_TOKEN) and format is not None:
                assert format in supported

        # Here's a media type that sometimes shows up in OPDS For
        # Distributors collections but is _not_ supported. Incoming
        # items with this media type will _not_ be imported.
        assert MediaTypes.JPEG_MEDIA_TYPE not in supported

    # Mock SUPPORTED_MEDIA_TYPES for purposes of test.
    GOOD_MEDIA_TYPE = "media/type"

    @patch.object(
        BearerTokenDrmMixin,
        "_SUPPORTED_BEARER_TOKEN_MEDIA_TYPES",
        frozenset({GOOD_MEDIA_TYPE}),
    )
    def test__bearer_token_format_data(self) -> None:
        good_rel = Hyperlink.GENERIC_OPDS_ACQUISITION
        good_media_type = self.GOOD_MEDIA_TYPE

        # The correct media type with incorrect rel should return None.
        assert (
            BearerTokenDrmMixin._bearer_token_format_data(
                LinkData(
                    rel="http://wrong/rel/",
                    media_type=good_media_type,
                    href="http://url1/",
                )
            )
            is None
        )

        # The correct rel with incorrect media type should return None.
        assert (
            BearerTokenDrmMixin._bearer_token_format_data(
                LinkData(
                    rel=good_rel, media_type="wrong/media type", href="http://url2/"
                )
            )
            is None
        )

        # The correct rel and media type should return a FormatData object.
        good_link_data = LinkData(
            rel=good_rel, media_type=good_media_type, href="http://url3/"
        )
        assert BearerTokenDrmMixin._bearer_token_format_data(
            good_link_data
        ) == FormatData(
            content_type=good_media_type,
            drm_scheme=DeliveryMechanism.BEARER_TOKEN,
            link=good_link_data,
            rights_uri=RightsStatus.IN_COPYRIGHT,
        )
