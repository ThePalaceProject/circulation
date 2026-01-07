from __future__ import annotations

import json
from collections.abc import Callable, Generator, Iterable, Sequence
from datetime import date, datetime
from typing import Any
from urllib.parse import urljoin

from frozendict import frozendict
from pydantic import ValidationError

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.contributor import ContributorData
from palace.manager.data_layer.format import FormatData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.license import LicenseData
from palace.manager.data_layer.link import LinkData
from palace.manager.data_layer.subject import SubjectData
from palace.manager.integration.license.opds.bearer_token_drm import BearerTokenDrmMixin
from palace.manager.integration.license.opds.data import FailedPublication
from palace.manager.integration.license.opds.extractor import (
    OpdsExtractor,
)
from palace.manager.integration.license.opds.odl.constants import FEEDBOOKS_AUDIO
from palace.manager.opds import opds2, rwpm
from palace.manager.opds.lcp.status import LoanStatus
from palace.manager.opds.odl import odl
from palace.manager.opds.odl.info import LicenseInfo, LicenseStatus
from palace.manager.opds.opds2 import (
    AcquisitionObject,
    BasePublicationFeed,
    PublicationFeedNoValidation,
)
from palace.manager.sqlalchemy.constants import LinkRelations, MediaTypes
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePoolStatus,
    LicensePoolType,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.resource import Hyperlink
from palace.manager.util import first_or_default


class OPDS2WithODLExtractor[PublicationType: opds2.BasePublication](
    OpdsExtractor[PublicationFeedNoValidation, PublicationType], BearerTokenDrmMixin
):

    _CONTRIBUTOR_ROLE_MAPPING: frozendict[str, str] = frozendict(
        {
            # We reverse the mapping because there are some roles that have the same code, and we
            # want to prioritize the first time the code appears in the list.
            code.lower(): role
            for role, code in reversed(Contributor.MARC_ROLE_CODES.items())
        }
        | {role.lower(): role for role in Contributor.Role}
    )

    def __init__(
        self,
        parse_publication: Callable[[dict[str, Any]], PublicationType],
        base_url: str,
        data_source: str,
        skipped_license_formats: Iterable[str] | None = None,
        bearer_token_drm: bool = False,
    ):
        self._parse_publication = parse_publication
        self._base_url = base_url
        self._data_source = data_source
        self._bearer_token_drm = bearer_token_drm
        self._skipped_license_formats = (
            set(skipped_license_formats) if skipped_license_formats else set()
        )

    @classmethod
    def _extract_subjects(cls, subjects: Sequence[rwpm.Subject]) -> list[SubjectData]:
        """Extract a list of SubjectData objects from the rwpm.Subject.

        :param subjects: Parsed subject object
        :return: List of subjects metadata
        """
        cls.logger().debug("Started extracting subjects metadata")

        parsed_subjects = []

        for subject in subjects:
            cls.logger().debug(
                f"Started extracting subject metadata from {subject.model_dump_json()}"
            )

            scheme = subject.scheme
            subject_type = Subject.by_uri.get(scheme) if scheme is not None else None
            if not subject_type:
                # We can't represent this subject because we don't
                # know its scheme. Just treat it as a tag.
                subject_type = Subject.TAG

            subject_data = SubjectData(
                type=subject_type,
                identifier=subject.code,
                name=str(subject.name),
                weight=1,
            )

            parsed_subjects.append(subject_data)

            cls.logger().debug(
                "Finished extracting subject metadata from {}: {}".format(
                    subject.model_dump_json(), subject_data
                )
            )

        cls.logger().debug(f"Finished extracting subjects metadata: {parsed_subjects}")

        return parsed_subjects

    @classmethod
    def _extract_contributor_roles(
        cls, roles: Sequence[str], default: str
    ) -> list[str]:
        """
        Normalize the contributor roles from the OPDS2 feed to our internal representation.
        """
        mapped_roles = set()
        for role in roles:
            if (lowercased_role := role.lower()) not in cls._CONTRIBUTOR_ROLE_MAPPING:
                cls.logger().warning(f"Unknown contributor role: {role}")
            mapped_roles.add(
                cls._CONTRIBUTOR_ROLE_MAPPING.get(lowercased_role, default)
            )

        if not mapped_roles:
            return [default]

        return list(mapped_roles)

    @classmethod
    def _extract_contributor_list(
        cls,
        contributors: Sequence[rwpm.Contributor],
        default_role: str,
    ) -> list[ContributorData]:
        """Extract a list of ContributorData objects from sequence of rwpm.Contributor.

        :param contributors: Parsed contributor objects
        :param default_role: Default role
        :return: List of contributors metadata
        """
        cls.logger().debug("Started extracting contributors metadata")

        parsed_contributors = []

        for contributor in contributors:
            cls.logger().debug(
                f"Started extracting contributor metadata from {contributor.model_dump_json()}"
            )

            if isinstance(contributor, rwpm.ContributorWithRole):
                roles = cls._extract_contributor_roles(contributor.roles, default_role)
            else:
                roles = [default_role]

            contributor_data = ContributorData(
                sort_name=contributor.sort_as,
                display_name=str(contributor.name),
                family_name=None,
                wikipedia_name=None,
                roles=roles,
            )

            cls.logger().debug(
                f"Finished extracting contributor metadata from {contributor.model_dump_json()}: {contributor_data}"
            )

            parsed_contributors.append(contributor_data)

        cls.logger().debug(
            f"Finished extracting contributors metadata: {parsed_contributors}"
        )

        return parsed_contributors

    @classmethod
    def _extract_contributors(
        cls, publication: opds2.BasePublication
    ) -> list[ContributorData]:
        """Extract a list of ContributorData objects from Publication.

        :param publication: Publication object
        :return: List of contributors metadata
        """

        return (
            cls._extract_contributor_list(
                publication.metadata.authors, Contributor.Role.AUTHOR
            )
            + cls._extract_contributor_list(
                publication.metadata.translators, Contributor.Role.TRANSLATOR
            )
            + cls._extract_contributor_list(
                publication.metadata.editors, Contributor.Role.EDITOR
            )
            + cls._extract_contributor_list(
                publication.metadata.artists, Contributor.Role.ARTIST
            )
            + cls._extract_contributor_list(
                publication.metadata.illustrators, Contributor.Role.ILLUSTRATOR
            )
            + cls._extract_contributor_list(
                publication.metadata.letterers, Contributor.Role.LETTERER
            )
            + cls._extract_contributor_list(
                publication.metadata.pencilers, Contributor.Role.PENCILER
            )
            + cls._extract_contributor_list(
                publication.metadata.colorists, Contributor.Role.COLORIST
            )
            + cls._extract_contributor_list(
                publication.metadata.inkers, Contributor.Role.INKER
            )
            + cls._extract_contributor_list(
                publication.metadata.narrators, Contributor.Role.NARRATOR
            )
            + cls._extract_contributor_list(
                publication.metadata.contributors, Contributor.Role.CONTRIBUTOR
            )
        )

    @classmethod
    def _extract_description_link(
        cls, publication: opds2.BasePublication
    ) -> LinkData | None:
        """Extract description from the publication object and create a Hyperlink.DESCRIPTION link containing it.

        :param publication: Publication object
        :return: LinkData object containing publication's description
        """
        cls.logger().debug(
            "Started extracting a description link from {}".format(
                publication.metadata.description
            )
        )

        description_link = None

        if publication.metadata.description:
            description_link = LinkData(
                rel=Hyperlink.DESCRIPTION,
                media_type=MediaTypes.TEXT_PLAIN,
                content=publication.metadata.description,
            )

        cls.logger().debug(
            "Finished extracting a description link from {}: {}".format(
                publication.metadata.description, description_link
            )
        )

        return description_link

    @classmethod
    def _extract_media_types_and_drm_scheme_from_link(
        cls, link: opds2.Link
    ) -> list[tuple[str, str | None]]:
        """Extract information about content's media type and used DRM schema from the link.

        :param link: Link object
        :return: 2-tuple containing information about the content's media type and its DRM schema
        """
        cls.logger().debug(
            f"Started extracting media types and a DRM scheme from {link.model_dump_json()}"
        )

        media_types_and_drm_scheme: list[tuple[str, str | None]] = []

        # We need to take into account indirect acquisition links
        if link.properties.indirect_acquisition:
            # We make the assumption that when we have nested indirect acquisition links
            # that the most deeply nested link is the content type, and the link at the nesting
            # level above that is the DRM. We discard all other levels of indirection, assuming
            # that they don't matter for us.
            #
            # This may not cover all cases, but it lets us deal with CM style acquisition links
            # where the top level link is a OPDS feed and the common case of a single
            # indirect_acquisition link.
            for acquisition_object in link.properties.indirect_acquisition:
                nested_acquisition: AcquisitionObject | None = acquisition_object
                nested_types = [link.type]
                while nested_acquisition:
                    nested_types.append(nested_acquisition.type)
                    nested_acquisition = first_or_default(nested_acquisition.children)
                [drm_type, media_type] = nested_types[-2:]
                if media_type is not None:
                    media_types_and_drm_scheme.append((media_type, drm_type))

        # There are no indirect links, then the link type points to the media, and
        # there is no DRM for this link.
        elif link.type is not None:
            media_types_and_drm_scheme.append((link.type, DeliveryMechanism.NO_DRM))

        cls.logger().debug(
            "Finished extracting media types and a DRM scheme from {}: {}".format(
                link, media_types_and_drm_scheme
            )
        )

        return media_types_and_drm_scheme

    @classmethod
    def _extract_supported_available_formats_from_link(
        cls, link: opds2.Link
    ) -> list[tuple[str, str | None]]:
        """Extract information about content's media type and used DRM schema from the link.

        :param link: Link object
        :return: 2-tuple containing information about the content's media type and its DRM schema
        """
        if not link.properties.availability.available:
            cls.logger().info(f"Link unavailable. Skipping. {link.model_dump_json()}")
            return []

        return [
            (media_type, drm_scheme)
            for media_type, drm_scheme in cls._extract_media_types_and_drm_scheme_from_link(
                link
            )
            if (
                media_type in MediaTypes.BOOK_MEDIA_TYPES
                or media_type in MediaTypes.AUDIOBOOK_MEDIA_TYPES
            )
            and (
                drm_scheme in DeliveryMechanism.KNOWN_DRM_TYPES
                or drm_scheme is DeliveryMechanism.NO_DRM
            )
        ]

    @classmethod
    def _extract_medium(
        cls,
        publication: opds2.BasePublication,
    ) -> str | None:
        """Extract the publication's medium from its metadata.

        :param publication: Publication object
        :return: Publication's medium
        """
        media_types: list[str] = []

        # Extract content types from license formats
        if isinstance(publication, odl.Publication) and publication.licenses:
            for license_info in publication.licenses:
                media_types.extend(license_info.metadata.formats)

        # Extract content types from links
        for link in publication.links:
            if not link.rels or not link.type or not cls._is_acquisition_link(link):
                continue
            for (
                media_type,
                drm_scheme,
            ) in cls._extract_media_types_and_drm_scheme_from_link(link):
                media_types.append(media_type)

        for media_type in media_types:
            medium = Edition.medium_from_media_type(media_type)
            if medium:
                return medium

        # Our fallback is to use the medium based on the type of the document, this seems like
        # it should be the way we determine the medium generally, but in practice we get all
        # sorts of types supplied, and they don't always correlate to the medium of the
        # files contained in the publication.
        return Edition.additional_type_to_medium.get(publication.metadata.type)

    @classmethod
    def _extract_published_date(cls, published: datetime | date | None) -> date | None:
        if isinstance(published, datetime):
            return published.date()
        return published

    @staticmethod
    def _is_acquisition_link(link: opds2.Link) -> bool:
        """Return a boolean value indicating whether a link can be considered an acquisition link.

        :param link: Link object
        :return: Boolean value indicating whether a link can be considered an acquisition link
        """
        return any([rel in LinkRelations.CIRCULATION_ALLOWED for rel in link.rels])

    @staticmethod
    def _is_bibliographic_link(link: opds2.Link) -> bool:
        """Return a boolean value indicating whether a link can be considered a bibliographic link.

        :param link: Link object
        """
        return any([rel in LinkRelations.BIBLIOGRAPHIC_ALLOWED for rel in link.rels])

    @staticmethod
    def _is_open_access_link(link: opds2.Link) -> bool:
        """Return a boolean value indicating whether the specified Link object describes an open-access link.

        :param link: Link object
        """
        return any([rel == Hyperlink.OPEN_ACCESS_DOWNLOAD for rel in link.rels])

    @classmethod
    def _extract_odl_license_data(
        cls,
        license_info_document: LicenseInfo,
        odl_license: odl.License,
    ) -> LicenseData:

        checkout_link = odl_license.links.get(
            rel=opds2.AcquisitionLinkRelations.borrow,
            type=LoanStatus.content_type(),
            raising=True,
        ).href

        license_info_document_link = odl_license.links.get(
            rel=rwpm.LinkRelations.self,
            type=LicenseInfo.content_type(),
            raising=True,
        ).href

        status = license_info_document.status

        if license_info_document.terms.expires != odl_license.metadata.terms.expires:
            cls.logger().error(
                f"License identifier {odl_license.metadata.identifier}. Mismatch between license "
                f"expiry in the feed ({odl_license.metadata.terms.expires}) and the expiry in the license "
                f"info document ({license_info_document.terms.expires}) setting license status "
                f"to unavailable."
            )
            status = LicenseStatus.unavailable

        if (
            license_info_document.terms.concurrency
            != odl_license.metadata.terms.concurrency
        ):
            cls.logger().error(
                f"License identifier {odl_license.metadata.identifier}. Mismatch between license "
                f"concurrency in the feed ({odl_license.metadata.terms.concurrency}) and the "
                f"concurrency in the license info document ("
                f"{license_info_document.terms.concurrency}) setting license status "
                f"to unavailable."
            )
            status = LicenseStatus.unavailable

        return LicenseData(
            identifier=license_info_document.identifier,
            checkout_url=checkout_link,
            status_url=license_info_document_link,
            expires=license_info_document.terms.expires,
            checkouts_left=license_info_document.checkouts.left,
            checkouts_available=license_info_document.checkouts.available,
            status=status,
            terms_concurrency=license_info_document.terms.concurrency,
            content_types=license_info_document.formats,
        )

    def _absolute_link_href(self, link: opds2.Link) -> str:
        """Construct the full URL for a link, using the base URL if the href is relative."""
        href = link.href
        if not href:
            raise PalaceValueError("Link href cannot be empty")
        return urljoin(self._base_url, href)

    def _extract_link(
        self, link: opds2.Link, default_link_rel: str | None = None
    ) -> LinkData:
        """Extract a LinkData object from opds2.Link.

        :param link: link
        :param default_link_rel: Default link's relation

        :return: Link metadata
        """
        self.log.debug(
            f"Started extracting link metadata from {link.model_dump_json()}"
        )

        # FIXME: It seems that OPDS 2.0 spec doesn't contain information about rights so we use the default one.
        rights_uri = RightsStatus.rights_uri_from_string("")
        rel = first_or_default(link.rels, default_link_rel)
        media_type = link.type
        href = self._absolute_link_href(link)

        link_metadata = LinkData(
            rel=rel,
            href=href,
            media_type=media_type,
            rights_uri=rights_uri,
            content=None,
        )

        self.log.debug(
            f"Finished extracting link metadata from {link.model_dump_json()}: {link_metadata}"
        )

        return link_metadata

    def _extract_image_links(
        self, publication: opds2.BasePublication
    ) -> list[LinkData]:
        """Extracts a list of LinkData objects containing information about artwork.

        :param publication: Publication object
        :return: List of links metadata
        """
        self.log.debug(f"Started extracting image links from {publication.images}")

        # FIXME: This code most likely will not work in general.
        # There's no guarantee that these images have the same media type,
        # or that the second-largest image isn't far too large to use as a thumbnail.
        # Instead of using the second-largest image as a thumbnail,
        # find the image that would make the best thumbnail
        # because of its dimensions, media type, and aspect ratio:
        #       IDEAL_COVER_ASPECT_RATIO = 2.0/3
        #       IDEAL_IMAGE_HEIGHT = 240
        #       IDEAL_IMAGE_WIDTH = 160

        sorted_raw_image_links = list(
            reversed(
                sorted(
                    publication.images,
                    key=lambda link: (link.width or 0, link.height or 0),
                )
            )
        )
        image_links = []

        if len(sorted_raw_image_links) > 0:
            cover_link = self._extract_link(
                sorted_raw_image_links[0],
                default_link_rel=Hyperlink.IMAGE,
            )
            image_links.append(cover_link)

        if len(sorted_raw_image_links) > 1:
            cover_link = self._extract_link(
                sorted_raw_image_links[1],
                default_link_rel=Hyperlink.THUMBNAIL_IMAGE,
            )
            image_links.append(cover_link)

        self.log.debug(
            f"Finished extracting image links from {publication.images}: {image_links}"
        )

        return image_links

    def _extract_bibliographic_links(
        self, publication: opds2.BasePublication
    ) -> list[LinkData]:
        """Extract a list of LinkData objects from Publication.

        :param publication: Publication object
        :return: List of links metadata
        """
        self.log.debug(f"Started extracting links from {publication.links}")

        links = []

        for link in publication.links:
            if self._is_bibliographic_link(link):
                links.append(self._extract_link(link))

        description_link = self._extract_description_link(publication)
        if description_link:
            links.append(description_link)

        image_links = self._extract_image_links(publication)
        links.extend(image_links)

        self.log.debug(f"Finished extracting links from {publication.links}: {links}")

        return links

    def _extract_opds2_formats(
        self,
        links: Sequence[opds2.StrictLink],
        rights_uri: str,
    ) -> list[FormatData]:
        """Find circulation formats in non open-access acquisition links.

        :param ast_link_list: List of Link objects
        :param rights_uri: Rights URI
        :return: List of additional circulation formats found in non-open access links
        """
        formats = []

        for link in links:
            if not self._is_acquisition_link(link):
                continue
            if self._is_open_access_link(link):
                continue

            link_data = self._extract_link(link)

            for (
                content_type,
                drm_scheme,
            ) in self._extract_supported_available_formats_from_link(link):
                if (
                    self._bearer_token_drm
                    and (
                        format_data := self._bearer_token_format_data(
                            link_data, content_type, drm_scheme
                        )
                    )
                    is not None
                ):
                    # Links to items with a non-open access acquisition type cannot be directly accessed
                    # if the feed is protected by OAuth. So we need to add a BEARER_TOKEN delivery mechanism
                    # to the formats, so we know we are able to fulfill these items indirectly via a bearer token.
                    formats.append(format_data)
                else:
                    formats.append(
                        FormatData(
                            content_type=content_type,
                            drm_scheme=drm_scheme,
                            link=link_data,
                            rights_uri=rights_uri,
                        )
                    )

        return formats

    def _extract_odl_circulation_data(
        self,
        publication: odl.Publication,
        license_info_documents: dict[str, LicenseInfo],
        identifier: IdentifierData,
        medium: str | None,
    ) -> CirculationData:
        formats = []
        licenses = []

        publication_available = publication.metadata.availability.available

        for odl_license in publication.licenses:
            license_identifier = odl_license.metadata.identifier
            license_info_document = license_info_documents.get(license_identifier)

            if (
                license_info_document is None
                and odl_license.metadata.availability.available
                and publication_available
            ):
                self.log.warning(
                    f"No License info document for license {license_identifier}, skipping."
                )
                continue

            parsed_license = (
                LicenseData(
                    identifier=license_identifier,
                    checkout_url=None,
                    status_url=odl_license.links.get(
                        rel=rwpm.LinkRelations.self,
                        type=LicenseInfo.content_type(),
                        raising=True,
                    ).href,
                    status=LicenseStatus.unavailable,
                    checkouts_available=0,
                )
                if license_info_document is None
                else self._extract_odl_license_data(
                    license_info_document,
                    odl_license,
                )
            )
            licenses.append(parsed_license)

            license_formats = set(odl_license.metadata.formats)
            for license_format in license_formats:
                if license_format in self._skipped_license_formats:
                    continue

                if license_format == FEEDBOOKS_AUDIO:
                    # Handle DeMarque audiobooks which include the protection
                    # in the content type. When we see a license format of
                    # application/audiobook+json; protection=http://www.feedbooks.com/audiobooks/access-restriction
                    # it means that this audiobook title is available through the DeMarque streaming manifest
                    # endpoint.
                    formats.append(
                        FormatData(
                            content_type=MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
                            drm_scheme=DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM,
                            rights_uri=RightsStatus.IN_COPYRIGHT,
                        )
                    )
                elif license_format == MediaTypes.TEXT_HTML_MEDIA_TYPE:
                    # Handle the case of a web reader. Web readers are assumed to have a content type of
                    # MediaTypes.TEXT_HTML_MEDIA_TYPE and not to be protected with DRM.
                    streaming_content_type = (
                        DeliveryMechanism.STREAMING_AUDIO_CONTENT_TYPE
                        if medium == Edition.AUDIO_MEDIUM
                        else DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE
                    )

                    # Handle the case where we want to skip the derived license format
                    if streaming_content_type in self._skipped_license_formats:
                        continue

                    formats.append(
                        FormatData(
                            content_type=streaming_content_type,
                            drm_scheme=DeliveryMechanism.STREAMING_DRM,
                            rights_uri=RightsStatus.IN_COPYRIGHT,
                        )
                    )
                else:
                    drm_schemes: Sequence[str | None] = (
                        odl_license.metadata.protection.formats or [None]
                    )
                    for drm_scheme in drm_schemes:
                        formats.append(
                            FormatData(
                                content_type=license_format,
                                drm_scheme=drm_scheme,
                                rights_uri=RightsStatus.IN_COPYRIGHT,
                            )
                        )

        status = (
            LicensePoolStatus.ACTIVE
            if publication_available
            and any(l.status == LicenseStatus.available for l in licenses)
            else LicensePoolStatus.EXHAUSTED
        )

        return CirculationData(
            data_source_name=self._data_source,
            primary_identifier_data=identifier,
            type=LicensePoolType.AGGREGATED,
            status=status,
            licenses=licenses,
            licenses_owned=None,
            licenses_available=None,
            licenses_reserved=None,
            patrons_in_hold_queue=None,
            formats=formats,
        )

    def _extract_opds2_circulation_data(
        self,
        publication: opds2.BasePublication,
        identifier: IdentifierData,
        medium: str | None,
    ) -> CirculationData:
        # FIXME: It seems that OPDS 2.0 spec doesn't contain information about rights so we use the default one
        rights_uri = RightsStatus.rights_uri_from_string("")

        license_type = LicensePoolType.UNLIMITED
        if publication.metadata.availability.available:
            license_status = LicensePoolStatus.ACTIVE
        else:
            license_status = LicensePoolStatus.REMOVED

        formats = self._extract_opds2_formats(publication.links, rights_uri)
        links = [
            self._extract_link(link)
            for link in publication.links
            if self._is_acquisition_link(link)
        ]

        time_tracking = publication.metadata.time_tracking
        if medium != Edition.AUDIO_MEDIUM and time_tracking is True:
            time_tracking = False
            self.log.warning(f"Ignoring the time tracking flag for entry {identifier}")

        return CirculationData(
            default_rights_uri=rights_uri,
            data_source_name=self._data_source,
            primary_identifier_data=identifier,
            links=links,
            type=license_type,
            status=license_status,
            formats=formats,
            should_track_playtime=time_tracking,
        )

    def _extract_bibliographic_data(
        self,
        publication: opds2.BasePublication,
        identifier: IdentifierData,
        medium: str | None,
    ) -> BibliographicData:
        title = str(publication.metadata.title)
        subtitle = (
            str(publication.metadata.subtitle)
            if publication.metadata.subtitle
            else None
        )
        languages = first_or_default(publication.metadata.languages)
        links = self._extract_bibliographic_links(publication)

        first_publisher = first_or_default(publication.metadata.publishers)
        publisher = str(first_publisher.name) if first_publisher else None

        first_imprint = first_or_default(publication.metadata.imprints)
        imprint = str(first_imprint.name) if first_imprint else None
        published = self._extract_published_date(publication.metadata.published)
        subjects = self._extract_subjects(publication.metadata.subjects)

        contributors = self._extract_contributors(publication)

        # FIXME: There are no measurements in OPDS 2.0
        measurements: list[Any] = []

        # FIXME: There is no series information in OPDS 2.0
        series = None
        series_position = None

        last_opds_update = publication.metadata.modified

        # Audiobook duration
        duration = publication.metadata.duration

        return BibliographicData(
            data_source_name=self._data_source,
            title=title,
            subtitle=subtitle,
            language=languages,
            medium=medium,
            publisher=publisher,
            published=published,
            imprint=imprint,
            primary_identifier_data=identifier,
            subjects=subjects,
            contributors=contributors,
            measurements=measurements,
            series=series,
            series_position=series_position,
            links=links,
            data_source_last_updated=last_opds_update,
            duration=duration,
        )

    @classmethod
    def feed_parse(cls, feed: bytes) -> PublicationFeedNoValidation:
        return PublicationFeedNoValidation.model_validate_json(feed)

    @classmethod
    def feed_next_url(cls, feed: BasePublicationFeed[Any]) -> str | None:
        """Get the next page URL from the feed."""
        next_link = feed.links.get(rel="next", type=BasePublicationFeed.content_type())
        if not next_link:
            return None
        return next_link.href

    def feed_publications(
        self, feed: PublicationFeedNoValidation
    ) -> Generator[PublicationType | FailedPublication]:
        for publication_dict in feed.publications:
            try:
                yield self._parse_publication(publication_dict)
            except ValidationError as e:
                yield self.failure_from_publication(
                    publication=publication_dict,
                    error=e,
                    error_message="Error validating publication",
                )

    @classmethod
    def publication_licenses(
        cls, publication: opds2.BasePublication
    ) -> list[odl.License]:
        if not isinstance(publication, odl.Publication):
            return []
        return publication.licenses

    @classmethod
    def publication_available(cls, publication: opds2.BasePublication) -> bool:
        """Check if the publication is available."""
        return publication.metadata.availability.available

    @classmethod
    def publication_identifier(
        cls, publication: opds2.BasePublication
    ) -> IdentifierData:
        """
        Extract the publication's identifier from its metadata.

        Raises PalaceValueError if the identifier cannot be parsed.
        """
        return IdentifierData.parse_urn(publication.metadata.identifier)

    @classmethod
    def failure_from_publication(
        cls,
        publication: opds2.BasePublication | dict[str, Any],
        error: Exception,
        error_message: str,
    ) -> FailedPublication:
        """Create a FailedPublication object from the given publication."""
        if isinstance(publication, dict):
            identifier = publication.get("metadata", {}).get("identifier")
            title = publication.get("metadata", {}).get("title")
            publication_data = publication
        else:
            identifier = publication.metadata.identifier
            title = str(publication.metadata.title)
            publication_data = publication.model_dump(mode="json")

        return FailedPublication(
            error=error,
            error_message=error_message,
            identifier=identifier,
            title=title,
            publication_data=json.dumps(publication_data, indent=2),
        )

    def publication_bibliographic(
        self,
        identifier: IdentifierData,
        publication: opds2.BasePublication,
        license_info_documents: dict[str, LicenseInfo] | None = None,
    ) -> BibliographicData:
        """Extract a BibliographicData object from OPDS2 or OPDS2+ODL Publication.

        :return: Publication's BibliographicData
        """
        self.log.debug(f"Started extracting data from publication {publication}")

        if license_info_documents is None:
            license_info_documents = {}

        medium = self._extract_medium(publication)
        if isinstance(publication, odl.Publication):
            circulation = self._extract_odl_circulation_data(
                publication, license_info_documents, identifier, medium
            )
        else:
            circulation = self._extract_opds2_circulation_data(
                publication, identifier, medium
            )
        bibliographic = self._extract_bibliographic_data(
            publication, identifier, medium
        )

        bibliographic.circulation = circulation

        self.log.debug(
            "Finished extracting bibliographic data from publication {}: {}".format(
                publication, bibliographic
            )
        )

        return bibliographic
