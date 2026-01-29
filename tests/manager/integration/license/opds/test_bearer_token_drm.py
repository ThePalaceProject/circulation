from unittest.mock import patch

import pytest

from palace.manager.data_layer.format import FormatData
from palace.manager.data_layer.link import LinkData
from palace.manager.integration.license.opds.bearer_token_drm import BearerTokenDrmMixin
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.edition import Edition
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

    def test__streaming_format_data_returns_none_for_non_streaming_links(self) -> None:
        """Non-streaming links should return None."""
        # Link without streaming profile in media type
        link = LinkData(
            rel=Hyperlink.GENERIC_OPDS_ACQUISITION,
            media_type=MediaTypes.EPUB_MEDIA_TYPE,
            href="http://example.com/book.epub",
        )
        assert BearerTokenDrmMixin._streaming_format_data(link) is None

        # Link with streaming profile but wrong rel
        link = LinkData(
            rel=Hyperlink.OPEN_ACCESS_DOWNLOAD,
            media_type=DeliveryMechanism.STREAMING_MEDIA_LINK_TYPE,
            href="http://example.com/viewer",
        )
        assert BearerTokenDrmMixin._streaming_format_data(link) is None

        # Link with None media_type
        link = LinkData(
            rel=Hyperlink.GENERIC_OPDS_ACQUISITION,
            media_type=None,
            href="http://example.com/viewer",
        )
        assert BearerTokenDrmMixin._streaming_format_data(link) is None

    @pytest.mark.parametrize(
        "medium,expected_content_type",
        [
            pytest.param(
                Edition.AUDIO_MEDIUM,
                DeliveryMechanism.STREAMING_AUDIO_CONTENT_TYPE,
                id="audio medium",
            ),
            pytest.param(
                Edition.BOOK_MEDIUM,
                DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
                id="book medium",
            ),
            pytest.param(
                None,
                DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
                id="no medium defaults to text",
            ),
        ],
    )
    def test__streaming_format_data_returns_correct_format(
        self, medium: str | None, expected_content_type: str
    ) -> None:
        """Streaming links should return FormatData with STREAMING_DRM."""
        link = LinkData(
            rel=Hyperlink.GENERIC_OPDS_ACQUISITION,
            media_type=DeliveryMechanism.STREAMING_MEDIA_LINK_TYPE,
            href="http://example.com/viewer/book123",
        )

        result = BearerTokenDrmMixin._streaming_format_data(link, medium)

        assert result is not None
        assert result == FormatData(
            content_type=expected_content_type,
            drm_scheme=DeliveryMechanism.STREAMING_DRM,
            link=link,
            rights_uri=RightsStatus.IN_COPYRIGHT,
        )
