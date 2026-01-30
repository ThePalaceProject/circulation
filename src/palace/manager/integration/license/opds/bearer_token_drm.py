from palace.manager.data_layer.format import FormatData
from palace.manager.data_layer.link import LinkData
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism, RightsStatus
from palace.manager.sqlalchemy.model.resource import Hyperlink


class BearerTokenDrmMixin:
    _SUPPORTED_BEARER_TOKEN_MEDIA_TYPES = frozenset(
        (
            frmt
            for frmt, drm in DeliveryMechanism.default_client_can_fulfill_lookup
            if drm == DeliveryMechanism.BEARER_TOKEN and frmt is not None
        )
    )

    @classmethod
    def _bearer_token_format_data(
        cls,
        link_data: LinkData,
        content_type: str | None = None,
        drm_scheme: str | None = None,
    ) -> FormatData | None:
        """Format the data for Bearer Token DRM."""
        if content_type is None:
            content_type = link_data.media_type

        if (
            content_type in cls._SUPPORTED_BEARER_TOKEN_MEDIA_TYPES
            and drm_scheme is None
            and link_data.rel == Hyperlink.GENERIC_OPDS_ACQUISITION
        ):
            # Links to items with a non-open access acquisition type cannot be directly accessed
            # if the feed is protected by OAuth. So we need to add a BEARER_TOKEN delivery mechanism
            # to the formats, so we know we are able to fulfill these items indirectly via a bearer token.
            return FormatData(
                content_type=content_type,
                drm_scheme=DeliveryMechanism.BEARER_TOKEN,
                link=link_data,
                rights_uri=RightsStatus.IN_COPYRIGHT,
            )

        return None
