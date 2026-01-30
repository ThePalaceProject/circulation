import pytest

from palace.manager.data_layer.format import FormatData
from palace.manager.data_layer.link import LinkData
from palace.manager.integration.license.opds.for_distributors.utils import (
    STREAMING_MEDIA_LINK_TYPE,
    streaming_format_data,
)
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism, RightsStatus
from palace.manager.sqlalchemy.model.resource import Hyperlink


def test_streaming_format_data_returns_none_for_non_streaming_links() -> None:
    """Non-streaming links should return None."""
    # Link without streaming profile in media type
    link = LinkData(
        rel=Hyperlink.GENERIC_OPDS_ACQUISITION,
        media_type=MediaTypes.EPUB_MEDIA_TYPE,
        href="http://example.com/book.epub",
    )
    assert streaming_format_data(link) is None

    # Link with streaming profile but wrong rel
    link = LinkData(
        rel=Hyperlink.OPEN_ACCESS_DOWNLOAD,
        media_type=STREAMING_MEDIA_LINK_TYPE,
        href="http://example.com/viewer",
    )
    assert streaming_format_data(link) is None

    # Link with None media_type
    link = LinkData(
        rel=Hyperlink.GENERIC_OPDS_ACQUISITION,
        media_type=None,
        href="http://example.com/viewer",
    )
    assert streaming_format_data(link) is None


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
def test_streaming_format_data_returns_correct_format(
    medium: str | None, expected_content_type: str
) -> None:
    """Streaming links should return FormatData with STREAMING_DRM."""
    link = LinkData(
        rel=Hyperlink.GENERIC_OPDS_ACQUISITION,
        media_type=STREAMING_MEDIA_LINK_TYPE,
        href="http://example.com/viewer/book123",
    )

    result = streaming_format_data(link, medium)

    assert result is not None
    assert result == FormatData(
        content_type=expected_content_type,
        drm_scheme=DeliveryMechanism.STREAMING_DRM,
        link=link,
        rights_uri=RightsStatus.IN_COPYRIGHT,
    )
