from __future__ import annotations

import sys
from collections.abc import Mapping
from typing import Annotated

from flask_babel import lazy_gettext as _

from palace.manager.integration.settings import (
    BaseSettings,
    FormFieldType,
    FormMetadata,
)
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePoolDeliveryMechanism,
)


class FormatPriorities:
    """Functions for prioritizing delivery mechanisms based on content type and DRM scheme."""

    def __init__(
        self,
        prioritized_drm_schemes: list[str],
        prioritized_content_types: list[str],
        deprioritize_lcp_non_epubs: bool,
    ):
        """
        :param prioritized_drm_schemes: The set of DRM schemes to prioritize; items earlier in the list are higher priority.
        :param prioritized_content_types: The set of content types to prioritize; items earlier in the list are higher priority.
        :param deprioritize_lcp_non_epubs: Should LCP audiobooks/PDFs be deprioritized in an ad-hoc manner?
        """

        # Assign priorities to each content type and DRM scheme based on their position
        # in the given lists. Higher priorities are assigned to items that appear earlier.
        self._prioritized_content_types: Mapping[str, int] = {}
        for index, content_type in enumerate(reversed(prioritized_content_types)):
            self._prioritized_content_types[content_type] = index + 1

        self._prioritized_drm_schemes: Mapping[str, int] = {}
        for index, drm_scheme in enumerate(reversed(prioritized_drm_schemes)):
            self._prioritized_drm_schemes[drm_scheme] = index + 1

        self._deprioritize_lcp_non_epubs = deprioritize_lcp_non_epubs

    def prioritize_mechanisms(
        self, mechanisms: list[LicensePoolDeliveryMechanism]
    ) -> list[LicensePoolDeliveryMechanism]:
        """
        Prioritize the delivery mechanisms given.
        :param mechanisms: The list of delivery mechanisms
        :return: A list of suitable delivery mechanisms in priority order, highest priority first
        """
        # If there are any prioritized DRM schemes or content types, then
        # sort the list of mechanisms accordingly.
        mechanisms = mechanisms.copy()
        if (
            len(self._prioritized_drm_schemes) != 0
            or len(self._prioritized_content_types) != 0
        ):
            mechanisms.sort(
                key=lambda mechanism: self._content_type_priority(
                    mechanism.delivery_mechanism.content_type or ""
                ),
                reverse=True,
            )
            mechanisms.sort(
                key=lambda mechanism: self._drm_scheme_priority(
                    mechanism.delivery_mechanism.drm_scheme
                ),
                reverse=True,
            )

        if self._deprioritize_lcp_non_epubs:
            mechanisms.sort(
                key=lambda mechanism: self._artificial_lcp_content_priority(
                    drm_scheme=mechanism.delivery_mechanism.drm_scheme,
                    content_type=mechanism.delivery_mechanism.content_type,
                ),
                reverse=True,
            )

        return mechanisms

    @staticmethod
    def _artificial_lcp_content_priority(
        drm_scheme: str | None, content_type: str | None
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

    def _drm_scheme_priority(self, drm_scheme: str | None) -> int:
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
    prioritized_drm_schemes: Annotated[
        list[str],
        FormMetadata(
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
            type=FormFieldType.LIST,
            required=False,
        ),
    ] = []

    prioritized_content_types: Annotated[
        list[str],
        FormMetadata(
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
            type=FormFieldType.LIST,
            required=False,
        ),
    ] = []

    deprioritize_lcp_non_epubs: Annotated[
        bool,
        FormMetadata(
            label=_("De-prioritize LCP non-EPUBs"),
            description=_(
                "De-prioritize all LCP content except for EPUBs. Setting this configuration option to "
                "<i>De-prioritize</i> will preserve any priorities specified above, but will artificially "
                "push (for example) LCP audiobooks and PDFs to the lowest priority."
                "<br/>"
                "<br/>"
                "<b>Note:</b> This option is a temporary solution and will be removed in future releases!"
            ),
            type=FormFieldType.SELECT,
            required=False,
            options={
                True: _("De-prioritize"),
                False: _("Do not de-prioritize"),
            },
        ),
    ] = False
