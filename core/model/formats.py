import sys
from typing import List, Mapping, Optional

from flask_babel import lazy_gettext as _

from core.integration.settings import (
    BaseSettings,
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
)
from core.model import (
    DeliveryMechanism,
    LicensePool,
    LicensePoolDeliveryMechanism,
    MediaTypes,
)


class FormatPriorities:
    """Functions for prioritizing delivery mechanisms based on content type and DRM scheme."""

    PRIORITIZED_DRM_SCHEMES_KEY: str = "prioritized_drm_schemes"
    PRIORITIZED_CONTENT_TYPES_KEY: str = "prioritized_content_types"
    DEPRIORITIZE_LCP_NON_EPUBS_KEY: str = "deprioritize_lcp_non_epubs"

    _prioritized_drm_schemes: Mapping[str, int]
    _prioritized_content_types: Mapping[str, int]
    _hidden_content_types: List[str]
    _deprioritize_lcp_non_epubs: bool

    def __init__(
        self,
        prioritized_drm_schemes: List[str],
        prioritized_content_types: List[str],
        hidden_content_types: List[str],
        deprioritize_lcp_non_epubs: bool,
    ):
        """
        :param prioritized_drm_schemes: The set of DRM schemes to prioritize; items earlier in the list are higher priority.
        :param prioritized_content_types: The set of content types to prioritize; items earlier in the list are higher priority.
        :param hidden_content_types: The set of content types to remove entirely
        :param deprioritize_lcp_non_epubs: Should LCP audiobooks/PDFs be deprioritized in an ad-hoc manner?
        """

        # Assign priorities to each content type and DRM scheme based on their position
        # in the given lists. Higher priorities are assigned to items that appear earlier.
        self._prioritized_content_types = {}
        for index, content_type in enumerate(reversed(prioritized_content_types)):
            self._prioritized_content_types[content_type] = index + 1

        self._prioritized_drm_schemes = {}
        for index, drm_scheme in enumerate(reversed(prioritized_drm_schemes)):
            self._prioritized_drm_schemes[drm_scheme] = index + 1

        self._hidden_content_types = hidden_content_types
        self._deprioritize_lcp_non_epubs = deprioritize_lcp_non_epubs

    def prioritize_for_pool(
        self, pool: LicensePool
    ) -> List[LicensePoolDeliveryMechanism]:
        """
        Filter and prioritize the delivery mechanisms in the given pool.
        :param pool: The license pool
        :return: A list of suitable delivery mechanisms in priority order, highest priority first
        """
        return self.prioritize_mechanisms(pool.delivery_mechanisms)

    def prioritize_mechanisms(
        self, mechanisms: List[LicensePoolDeliveryMechanism]
    ) -> List[LicensePoolDeliveryMechanism]:
        """
        Filter and prioritize the delivery mechanisms in the given pool.
        :param mechanisms: The list of delivery mechanisms
        :return: A list of suitable delivery mechanisms in priority order, highest priority first
        """

        # First, filter out all hidden content types.
        mechanisms_filtered: List[LicensePoolDeliveryMechanism] = []
        for delivery in mechanisms:
            delivery_mechanism = delivery.delivery_mechanism
            if delivery_mechanism:
                if delivery_mechanism.content_type not in self._hidden_content_types:
                    mechanisms_filtered.append(delivery)

        # If there are any prioritized DRM schemes or content types, then
        # sort the list of mechanisms accordingly.
        if (
            len(self._prioritized_drm_schemes) != 0
            or len(self._prioritized_content_types) != 0
        ):
            mechanisms_filtered.sort(
                key=lambda mechanism: self._content_type_priority(
                    mechanism.delivery_mechanism.content_type or ""
                ),
                reverse=True,
            )
            mechanisms_filtered.sort(
                key=lambda mechanism: self._drm_scheme_priority(
                    mechanism.delivery_mechanism.drm_scheme
                ),
                reverse=True,
            )

        if self._deprioritize_lcp_non_epubs:
            mechanisms_filtered.sort(
                key=lambda mechanism: FormatPriorities._artificial_lcp_content_priority(
                    drm_scheme=mechanism.delivery_mechanism.drm_scheme,
                    content_type=mechanism.delivery_mechanism.content_type,
                ),
                reverse=True,
            )

        return mechanisms_filtered

    @staticmethod
    def _artificial_lcp_content_priority(
        drm_scheme: Optional[str], content_type: Optional[str]
    ) -> int:
        """A comparison function that arbitrarily deflates the priority of LCP content. The comparison function
        treats all other DRM mechanisms and content types as equal."""
        if (
            drm_scheme == DeliveryMechanism.LCP_DRM
            and content_type != MediaTypes.EPUB_MEDIA_TYPE
        ):
            return -1
        else:
            return 0

    def _drm_scheme_priority(self, drm_scheme: Optional[str]) -> int:
        """Determine the priority of a DRM scheme. A lack of DRM is always
        prioritized over having DRM, and prioritized schemes are always
        higher priority than non-prioritized schemes."""

        if not drm_scheme:
            return sys.maxsize
        return self._prioritized_drm_schemes.get(drm_scheme, 0)

    def _content_type_priority(self, content_type: str) -> int:
        """Determine the priority of a content type. Prioritized content
        types are always of a higher priority than non-prioritized types."""
        return self._prioritized_content_types.get(content_type, 0)


class FormatPrioritiesSettings(BaseSettings):
    prioritized_drm_schemes: Optional[list] = FormField(
        default=[],
        form=ConfigurationFormItem(
            label=_("Prioritized DRM schemes"),
            description=_(
                "A list of DRM schemes that will be prioritized when OPDS links are generated. "
                "DRM schemes specified earlier in the list will be prioritized over schemes specified later. "
                f"Example schemes include <tt>{DeliveryMechanism.LCP_DRM}</tt> for LCP, and <tt>{DeliveryMechanism.ADOBE_DRM}</tt> "
                "for Adobe DRM. "
                "An empty list here specifies backwards-compatible behavior where no schemes are prioritized."
                "<br/>"
                "<br/>"
                "<b>Note:</b> Adding any DRM scheme will cause acquisition links to be reordered into a predictable "
                "order that prioritizes DRM-free content over content with DRM. If a book exists with <i>both</i> DRM-free "
                "<i>and</i> DRM-encumbered formats, the DRM-free version will become preferred, which might not be how your "
                "collection originally behaved."
            ),
            type=ConfigurationFormItemType.LIST,
            required=False,
        ),
    )

    prioritized_content_types: Optional[list] = FormField(
        default=[],
        form=ConfigurationFormItem(
            label=_("Prioritized content types"),
            description=_(
                "A list of content types that will be prioritized when OPDS links are generated. "
                "Content types specified earlier in the list will be prioritized over types specified later. "
                f"Example types include <tt>{MediaTypes.EPUB_MEDIA_TYPE}</tt> for EPUB, and <tt>{MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE}</tt> "
                "for audiobook manifests. "
                "An empty list here specifies backwards-compatible behavior where no types are prioritized."
                "<br/>"
                "<br/>"
                "<b>Note:</b> Adding any content type here will cause acquisition links to be reordered into a predictable "
                "order that prioritizes DRM-free content over content with DRM. If a book exists with <i>both</i> DRM-free "
                "<i>and</i> DRM-encumbered formats, the DRM-free version will become preferred, which might not be how your "
                "collection originally behaved."
            ),
            type=ConfigurationFormItemType.LIST,
            required=False,
        ),
    )

    deprioritize_lcp_non_epubs: Optional[str] = FormField(
        default="false",
        form=ConfigurationFormItem(
            label=_("De-prioritize LCP non-EPUBs"),
            description=_(
                "De-prioritize all LCP content except for EPUBs. Setting this configuration option to "
                "<i>De-prioritize</i> will preserve any priorities specified above, but will artificially "
                "push (for example) LCP audiobooks and PDFs to the lowest priority."
                "<br/>"
                "<br/>"
                "<b>Note:</b> This option is a temporary solution and will be removed in future releases!"
            ),
            type=ConfigurationFormItemType.SELECT,
            required=False,
            options={
                "true": _("De-prioritize"),
                "false": _("Do not de-prioritize"),
            },
        ),
    )
