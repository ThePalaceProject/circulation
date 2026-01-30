from palace.manager.data_layer.format import FormatData
from palace.manager.data_layer.link import LinkData
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism, RightsStatus
from palace.manager.sqlalchemy.model.resource import Hyperlink

# This is the exact streaming media URI used in Biblioboard OPDS for Distributors feeds.
# It's technically incorrect because the profile contains special characters, so it should
# be wrapped in quotes. However, this is how it's present in the OPDS1 feeds, so we use it
# as is. This should be corrected when moving to OPDS2.
STREAMING_MEDIA_LINK_TYPE = f"{MediaTypes.TEXT_HTML_MEDIA_TYPE};profile={DeliveryMechanism.STREAMING_MEDIA_PROFILE_URI}"


def streaming_format_data(
    link_data: LinkData,
    medium: str | None = None,
) -> FormatData | None:
    """
    Detect streaming media links in OPDS for Distributors feeds.

    Streaming media links are identified by having a media_type that matches
    the streaming media link type (text/html;profile=http://librarysimplified.org/terms/profiles/streaming-media).

    :param link_data: The link data from the feed entry.
    :param medium: The medium type of the publication (e.g., Edition.AUDIO_MEDIUM or Edition.BOOK_MEDIUM).
    :return: FormatData for streaming content, or None if not a streaming link.
    """
    media_type = link_data.media_type

    if (
        media_type == STREAMING_MEDIA_LINK_TYPE
        and link_data.rel == Hyperlink.GENERIC_OPDS_ACQUISITION
    ):
        # Determine content type based on medium
        if medium == Edition.AUDIO_MEDIUM:
            content_type = DeliveryMechanism.STREAMING_AUDIO_CONTENT_TYPE
        else:
            content_type = DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE

        return FormatData(
            content_type=content_type,
            drm_scheme=DeliveryMechanism.STREAMING_DRM,
            link=link_data,
            rights_uri=RightsStatus.IN_COPYRIGHT,
        )

    return None
