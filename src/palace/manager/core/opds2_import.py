from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from functools import cached_property
from typing import Any
from urllib.parse import urljoin, urlparse

from flask_babel import lazy_gettext as _
from pydantic import ValidationError
from requests import Response
from sqlalchemy.orm import Session
from typing_extensions import Unpack
from uritemplate import URITemplate

from palace.manager.api.circulation.base import BaseCirculationAPI
from palace.manager.api.circulation.exceptions import CannotFulfill
from palace.manager.api.circulation.fulfillment import RedirectFulfillment
from palace.manager.core.coverage import CoverageFailure
from palace.manager.core.opds_import import (
    BaseOPDSAPI,
    BaseOPDSImporter,
    OPDSImporterLibrarySettings,
    OPDSImporterSettings,
    OPDSImportMonitor,
)
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.contributor import ContributorData
from palace.manager.data_layer.format import FormatData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.link import LinkData
from palace.manager.data_layer.subject import SubjectData
from palace.manager.integration.settings import (
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
)
from palace.manager.opds import opds2, rwpm
from palace.manager.opds.opds2 import AcquisitionObject
from palace.manager.opds.types.link import CompactCollection
from palace.manager.sqlalchemy.constants import (
    IdentifierType,
    LinkRelations,
    MediaTypes,
)
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    LicensePoolDeliveryMechanism,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.util import first_or_default
from palace.manager.util.http import HTTP, BadResponseException


class OPDS2ImporterSettings(OPDSImporterSettings):
    custom_accept_header: str = FormField(
        default="{}, {};q=0.9, */*;q=0.1".format(
            opds2.PublicationFeed.content_type(), "application/json"
        ),
        form=ConfigurationFormItem(
            label=_("Custom accept header"),
            description=_(
                "Some servers expect an accept header to decide which file to send. You can use */* if the server doesn't expect anything."
            ),
            type=ConfigurationFormItemType.TEXT,
            required=False,
        ),
    )

    ignored_identifier_types: list[str] = FormField(
        default=[],
        form=ConfigurationFormItem(
            label=_("List of identifiers that will be skipped"),
            description=_(
                "Circulation Manager will not be importing publications with identifiers having one of the selected types."
            ),
            type=ConfigurationFormItemType.MENU,
            required=False,
            options={
                identifier_type.value: identifier_type.value
                for identifier_type in IdentifierType
            },
            format="narrow",
        ),
    )


class OPDS2ImporterLibrarySettings(OPDSImporterLibrarySettings):
    pass


class OPDS2API(BaseOPDSAPI):
    TOKEN_AUTH_CONFIG_KEY = "token_auth_endpoint"

    @classmethod
    def settings_class(cls) -> type[OPDS2ImporterSettings]:
        return OPDS2ImporterSettings

    @classmethod
    def library_settings_class(cls) -> type[OPDS2ImporterLibrarySettings]:
        return OPDS2ImporterLibrarySettings

    @classmethod
    def label(cls) -> str:
        return "OPDS 2.0 Import"

    @classmethod
    def description(cls) -> str:
        return "Import books from a publicly-accessible OPDS 2.0 feed."

    def __init__(self, _db: Session, collection: Collection):
        super().__init__(_db, collection)
        self.token_auth_configuration: str | None = (
            collection.integration_configuration.context.get(self.TOKEN_AUTH_CONFIG_KEY)
        )

    @classmethod
    def get_authentication_token(
        cls, patron: Patron, datasource: DataSource, token_auth_url: str
    ) -> str:
        """Get the authentication token for a patron"""
        log = cls.logger()

        patron_id = patron.identifier_to_remote_service(datasource)
        url = URITemplate(token_auth_url).expand(patron_id=patron_id)
        response = HTTP.get_with_timeout(url)
        if response.status_code != 200:
            log.error(
                f"Could not authenticate the patron (authorization identifier: '{patron.authorization_identifier}' "
                f"external identifier: '{patron_id}'): {str(response.content)}"
            )
            raise CannotFulfill()

        # The response should be the JWT token, not wrapped in any format like JSON
        token = response.text
        if not token:
            log.error(
                f"Could not authenticate the patron({patron_id}): {str(response.content)}"
            )
            raise CannotFulfill()

        return token

    def fulfill_token_auth(
        self,
        patron: Patron,
        licensepool: LicensePool,
        fulfillment: RedirectFulfillment,
    ) -> RedirectFulfillment:
        templated = URITemplate(fulfillment.content_link)
        if "authentication_token" not in templated.variable_names:
            self.log.warning(
                "No authentication_token variable found in content_link, unable to fulfill via OPDS2 token auth."
            )
            return fulfillment

        if not self.token_auth_configuration:
            self.log.warning(
                "No token auth configuration found, unable to fulfill via OPDS2 token auth."
            )
            return fulfillment

        token = self.get_authentication_token(
            patron, licensepool.data_source, self.token_auth_configuration
        )
        fulfillment.content_link = templated.expand(authentication_token=token)
        return fulfillment

    def fulfill(
        self,
        patron: Patron,
        pin: str,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism,
        **kwargs: Unpack[BaseCirculationAPI.FulfillKwargs],
    ) -> RedirectFulfillment:
        fulfillment = super().fulfill(
            patron, pin, licensepool, delivery_mechanism, **kwargs
        )
        if self.token_auth_configuration:
            fulfillment = self.fulfill_token_auth(patron, licensepool, fulfillment)
        return fulfillment


class OPDS2Importer(BaseOPDSImporter[OPDS2ImporterSettings]):
    """Imports editions and license pools from an OPDS 2.0 feed."""

    NAME: str = OPDS2API.label()
    DESCRIPTION: str = _("Import books from a publicly-accessible OPDS 2.0 feed.")
    NEXT_LINK_RELATION: str = "next"

    @classmethod
    def settings_class(cls) -> type[OPDS2ImporterSettings]:
        return OPDS2ImporterSettings

    def __init__(
        self,
        db: Session,
        collection: Collection,
        data_source_name: str | None = None,
    ):
        """Initialize a new instance of OPDS2Importer class.

        :param db: Database session

        :param collection: Circulation Manager's collection.
            LicensePools created by this OPDS2Import class will be associated with the given Collection.
            If this is None, no LicensePools will be created -- only Editions.
        :param data_source_name: Name of the source of this OPDS feed.
            All Editions created by this import will be associated with this DataSource.
            If there is no DataSource with this name, one will be created.
            NOTE: If `collection` is provided, its .data_source will take precedence over any value provided here.
            This is only for use when you are importing OPDS metadata without any particular Collection in mind.
        """
        super().__init__(db, collection, data_source_name)
        self.ignored_identifier_types = self.settings.ignored_identifier_types

    def _is_identifier_allowed(self, identifier: Identifier) -> bool:
        """Check the identifier and return a boolean value indicating whether CM can import it.

        :param identifier: Identifier object
        :return: Boolean value indicating whether CM can import the identifier
        """
        return identifier.type not in self.ignored_identifier_types

    def _extract_subjects(self, subjects: Sequence[rwpm.Subject]) -> list[SubjectData]:
        """Extract a list of SubjectData objects from the rwpm.Subject.

        :param subjects: Parsed subject object
        :return: List of subjects metadata
        """
        self.log.debug("Started extracting subjects metadata")

        subject_metadata_list = []

        for subject in subjects:
            self.log.debug(
                f"Started extracting subject metadata from {subject.model_dump_json()}"
            )

            scheme = subject.scheme
            subject_type = Subject.by_uri.get(scheme) if scheme is not None else None
            if not subject_type:
                # We can't represent this subject because we don't
                # know its scheme. Just treat it as a tag.
                subject_type = Subject.TAG

            subject_metadata = SubjectData(
                type=subject_type,
                identifier=subject.code,
                name=str(subject.name),
                weight=1,
            )

            subject_metadata_list.append(subject_metadata)

            self.log.debug(
                "Finished extracting subject metadata from {}: {}".format(
                    subject.model_dump_json(), subject_metadata
                )
            )

        self.log.debug(
            f"Finished extracting subjects metadata: {subject_metadata_list}"
        )

        return subject_metadata_list

    @cached_property
    def _contributor_roles(self) -> Mapping[str, str]:
        """
        Return a mapping of OPDS2 contributor roles to our internal contributor role representation.
        This mapping accepts MARC role codes and our internal contributor roles.
        """
        # We reverse the mapping because there are some roles that have the same code, and we
        # want to prioritize the first time the code appears in the list.
        marc_code_mapping = {
            code.lower(): role
            for role, code in reversed(Contributor.MARC_ROLE_CODES.items())
        }
        return marc_code_mapping | {role.lower(): role for role in Contributor.Role}

    def _extract_contributor_roles(
        self, roles: Sequence[str], default: str
    ) -> list[str]:
        """
        Normalize the contributor roles from the OPDS2 feed to our internal representation.
        """
        mapped_roles = set()
        for role in roles:
            if (lowercased_role := role.lower()) not in self._contributor_roles:
                self.log.warning(f"Unknown contributor role: {role}")
            mapped_roles.add(self._contributor_roles.get(lowercased_role, default))

        if not mapped_roles:
            return [default]

        return list(mapped_roles)

    def _extract_contributors(
        self,
        contributors: Sequence[rwpm.Contributor],
        default_role: str,
    ) -> list[ContributorData]:
        """Extract a list of ContributorData objects from rwpm.Contributor.

        :param contributors: Parsed contributor object
        :param default_role: Default role
        :return: List of contributors metadata
        """
        self.log.debug("Started extracting contributors metadata")

        contributor_metadata_list = []

        for contributor in contributors:
            self.log.debug(
                f"Started extracting contributor metadata from {contributor.model_dump_json()}"
            )

            if isinstance(contributor, rwpm.ContributorWithRole):
                roles = self._extract_contributor_roles(contributor.roles, default_role)
            else:
                roles = [default_role]

            contributor_metadata = ContributorData(
                sort_name=contributor.sort_as,
                display_name=str(contributor.name),
                family_name=None,
                wikipedia_name=None,
                roles=roles,
            )

            self.log.debug(
                f"Finished extracting contributor metadata from {contributor.model_dump_json()}: {contributor_metadata}"
            )

            contributor_metadata_list.append(contributor_metadata)

        self.log.debug(
            f"Finished extracting contributors metadata: {contributor_metadata_list}"
        )

        return contributor_metadata_list

    def _extract_link(
        self, link: opds2.Link, feed_self_url: str, default_link_rel: str | None = None
    ) -> LinkData:
        """Extract a LinkData object from opds2.Link.

        :param link: link
        :param feed_self_url: Feed's self URL
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
        href = link.href

        if feed_self_url and not urlparse(href).netloc:
            # This link is relative, so we need to get the absolute url
            href = urljoin(feed_self_url, href)

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

    def _extract_description_link(
        self, publication: opds2.BasePublication
    ) -> LinkData | None:
        """Extract description from the publication object and create a Hyperlink.DESCRIPTION link containing it.

        :param publication: Publication object
        :return: LinkData object containing publication's description
        """
        self.log.debug(
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

        self.log.debug(
            "Finished extracting a description link from {}: {}".format(
                publication.metadata.description, description_link
            )
        )

        return description_link

    def _extract_image_links(
        self, publication: opds2.BasePublication, feed_self_url: str
    ) -> list[LinkData]:
        """Extracts a list of LinkData objects containing information about artwork.

        :param publication: Publication object
        :param feed_self_url: Feed's self URL
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
                feed_self_url,
                default_link_rel=Hyperlink.IMAGE,
            )
            image_links.append(cover_link)

        if len(sorted_raw_image_links) > 1:
            cover_link = self._extract_link(
                sorted_raw_image_links[1],
                feed_self_url,
                default_link_rel=Hyperlink.THUMBNAIL_IMAGE,
            )
            image_links.append(cover_link)

        self.log.debug(
            f"Finished extracting image links from {publication.images}: {image_links}"
        )

        return image_links

    def _extract_links(
        self, publication: opds2.BasePublication, feed_self_url: str
    ) -> list[LinkData]:
        """Extract a list of LinkData objects from opds2.Publication.

        :param publication: Publication object
        :param feed_self_url: Feed's self URL
        :return: List of links metadata
        """
        self.log.debug(f"Started extracting links from {publication.links}")

        links = []

        for link in publication.links:
            link_metadata = self._extract_link(link, feed_self_url)
            links.append(link_metadata)

        description_link = self._extract_description_link(publication)
        if description_link:
            links.append(description_link)

        image_links = self._extract_image_links(publication, feed_self_url)
        links.extend(image_links)

        self.log.debug(f"Finished extracting links from {publication.links}: {links}")

        return links

    def _extract_media_types_and_drm_scheme_from_link(
        self, link: opds2.Link
    ) -> list[tuple[str, str | None]]:
        """Extract information about content's media type and used DRM schema from the link.

        :param link: Link object
        :return: 2-tuple containing information about the content's media type and its DRM schema
        """
        self.log.debug(
            f"Started extracting media types and a DRM scheme from {link.model_dump_json()}"
        )

        media_types_and_drm_scheme: list[tuple[str, str | None]] = []

        if not link.properties.availability.available:
            self.log.info(f"Link unavailable. Skipping. {link.model_dump_json()}")
            return []

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

                # We then check this returned pair of content types to make sure they match known
                # book or audiobook and DRM types. If they do not match known types, then we skip
                # this link.
                if (
                    media_type in MediaTypes.BOOK_MEDIA_TYPES
                    or media_type in MediaTypes.AUDIOBOOK_MEDIA_TYPES
                ) and drm_type in DeliveryMechanism.KNOWN_DRM_TYPES:
                    media_types_and_drm_scheme.append((media_type, drm_type))

        # There are no indirect links, then the link type points to the media, and
        # there is no DRM for this link.
        else:
            if (
                link.type in MediaTypes.BOOK_MEDIA_TYPES
                or link.type in MediaTypes.AUDIOBOOK_MEDIA_TYPES
            ):
                media_types_and_drm_scheme.append((link.type, DeliveryMechanism.NO_DRM))

        self.log.debug(
            "Finished extracting media types and a DRM scheme from {}: {}".format(
                link, media_types_and_drm_scheme
            )
        )

        return media_types_and_drm_scheme

    def _extract_medium_from_links(
        self, links: CompactCollection[opds2.Link]
    ) -> str | None:
        """Extract the publication's medium from its links.

        :param links: List of links
        :return: Publication's medium
        """
        derived = None

        for link in links:
            if not link.rels or not link.type or not self._is_acquisition_link(link):
                continue

            link_media_type, _ = first_or_default(
                self._extract_media_types_and_drm_scheme_from_link(link),
                default=(None, None),
            )
            derived = Edition.medium_from_media_type(link_media_type)

            if derived:
                break

        return derived

    @staticmethod
    def _extract_medium(
        publication: opds2.BasePublication,
        default_medium: str | None = Edition.BOOK_MEDIUM,
    ) -> str | None:
        """Extract the publication's medium from its metadata.

        :param publication: Publication object
        :return: Publication's medium
        """
        medium = default_medium

        if publication.metadata.type:
            medium = Edition.additional_type_to_medium.get(
                publication.metadata.type, default_medium
            )

        return medium

    def _extract_identifier(self, publication: opds2.BasePublication) -> Identifier:
        """Extract the publication's identifier from its metadata.

        :param publication: Publication object
        :return: Identifier object
        """
        return self.parse_identifier(publication.metadata.identifier)

    @classmethod
    def _extract_published_date(cls, published: datetime | date | None) -> date | None:
        if isinstance(published, datetime):
            return published.date()
        return published

    def _extract_publication_bibliographic_data(
        self,
        publication: opds2.BasePublication,
        data_source_name: str,
        feed_self_url: str,
    ) -> BibliographicData:
        """Extract a BibliographicData object from opds2.Publication.

        :param publication: Feed object
        :param publication: Publication object
        :param data_source_name: Data source's name
        :return: Publication's BibliographicData
        """
        self.log.debug(
            f"Started extracting bibliographic data from publication {publication}"
        )

        title = str(publication.metadata.title)
        subtitle = (
            str(publication.metadata.subtitle)
            if publication.metadata.subtitle
            else None
        )

        languages = first_or_default(publication.metadata.languages)
        derived_medium = self._extract_medium_from_links(publication.links)
        medium = self._extract_medium(publication, derived_medium)

        first_publisher = first_or_default(publication.metadata.publishers)
        publisher = str(first_publisher.name) if first_publisher else None

        first_imprint = first_or_default(publication.metadata.imprints)
        imprint = str(first_imprint.name) if first_imprint else None

        published = self._extract_published_date(publication.metadata.published)

        subjects = self._extract_subjects(publication.metadata.subjects)
        contributors = (
            self._extract_contributors(
                publication.metadata.authors, Contributor.Role.AUTHOR
            )
            + self._extract_contributors(
                publication.metadata.translators, Contributor.Role.TRANSLATOR
            )
            + self._extract_contributors(
                publication.metadata.editors, Contributor.Role.EDITOR
            )
            + self._extract_contributors(
                publication.metadata.artists, Contributor.Role.ARTIST
            )
            + self._extract_contributors(
                publication.metadata.illustrators, Contributor.Role.ILLUSTRATOR
            )
            + self._extract_contributors(
                publication.metadata.letterers, Contributor.Role.LETTERER
            )
            + self._extract_contributors(
                publication.metadata.pencilers, Contributor.Role.PENCILER
            )
            + self._extract_contributors(
                publication.metadata.colorists, Contributor.Role.COLORIST
            )
            + self._extract_contributors(
                publication.metadata.inkers, Contributor.Role.INKER
            )
            + self._extract_contributors(
                publication.metadata.narrators, Contributor.Role.NARRATOR
            )
            + self._extract_contributors(
                publication.metadata.contributors, Contributor.Role.CONTRIBUTOR
            )
        )
        # Audiobook duration
        duration = publication.metadata.duration
        # Not all parsers support time_tracking
        time_tracking = getattr(publication.metadata, "time_tracking", False)
        if medium != Edition.AUDIO_MEDIUM and time_tracking is True:
            time_tracking = False
            self.log.warning(
                f"Ignoring the time tracking flag for entry {publication.metadata.identifier}"
            )

        links = self._extract_links(publication, feed_self_url)

        last_opds_update = publication.metadata.modified

        identifier = self._extract_identifier(publication)
        identifier_data = IdentifierData.from_identifier(identifier)

        # FIXME: There are no measurements in OPDS 2.0
        measurements: list[Any] = []

        # FIXME: There is no series information in OPDS 2.0
        series = None
        series_position = None

        # FIXME: It seems that OPDS 2.0 spec doesn't contain information about rights so we use the default one
        rights_uri = RightsStatus.rights_uri_from_string("")

        if publication.metadata.availability.available:
            licenses_owned = LicensePool.UNLIMITED_ACCESS
            licenses_available = LicensePool.UNLIMITED_ACCESS
        else:
            licenses_owned = 0
            licenses_available = 0

        circulation_data = CirculationData(
            default_rights_uri=rights_uri,
            data_source_name=data_source_name,
            primary_identifier_data=identifier_data,
            links=links,
            licenses_owned=licenses_owned,
            licenses_available=licenses_available,
            licenses_reserved=0,
            patrons_in_hold_queue=0,
            formats=[],
            should_track_playtime=time_tracking,
        )

        formats = self._find_formats_in_non_open_access_acquisition_links(
            publication.links, links, rights_uri, circulation_data
        )
        circulation_data.formats.extend(formats)

        bibliographic = BibliographicData(
            data_source_name=data_source_name,
            title=title,
            subtitle=subtitle,
            language=languages,
            medium=medium,
            publisher=publisher,
            published=published,
            imprint=imprint,
            primary_identifier_data=identifier_data,
            subjects=subjects,
            contributors=contributors,
            measurements=measurements,
            series=series,
            series_position=series_position,
            links=links,
            data_source_last_updated=last_opds_update,
            duration=duration,
            circulation=circulation_data,
        )

        self.log.debug(
            "Finished extracting bibliographic data from publication {}: {}".format(
                publication, bibliographic
            )
        )

        return bibliographic

    def _find_formats_in_non_open_access_acquisition_links(
        self,
        ast_link_list: Sequence[opds2.StrictLink],
        link_data_list: list[LinkData],
        rights_uri: str,
        circulation_data: CirculationData,
    ) -> list[FormatData]:
        """Find circulation formats in non open-access acquisition links.

        :param ast_link_list: List of Link objects
        :param link_data_list: List of LinkData objects
        :param rights_uri: Rights URI
        :param circulation_data: Circulation data
        :return: List of additional circulation formats found in non-open access links
        """
        formats = []

        for ast_link, parsed_link in zip(ast_link_list, link_data_list):
            if not self._is_acquisition_link(ast_link):
                continue
            if self._is_open_access_link_(parsed_link, circulation_data):
                continue

            for (
                content_type,
                drm_scheme,
            ) in self._extract_media_types_and_drm_scheme_from_link(ast_link):
                formats.append(
                    FormatData(
                        content_type=content_type,
                        drm_scheme=drm_scheme,
                        link=parsed_link,
                        rights_uri=rights_uri,
                    )
                )

        return formats

    def _get_publication(
        self,
        publication: dict[str, Any],
    ) -> opds2.BasePublication:
        try:
            return opds2.Publication.model_validate(publication)
        except ValidationError as e:
            raw_identifier = publication.get("metadata", {}).get("identifier")
            self.log.exception(
                f"Error validating publication (identifier: {raw_identifier}): {e}"
            )
            raise

    @staticmethod
    def _is_acquisition_link(link: opds2.Link) -> bool:
        """Return a boolean value indicating whether a link can be considered an acquisition link.

        :param link: Link object
        :return: Boolean value indicating whether a link can be considered an acquisition link
        """
        return any(
            [rel for rel in link.rels if rel in LinkRelations.CIRCULATION_ALLOWED]
        )

    @staticmethod
    def _is_open_access_link_(
        link_data: LinkData, circulation_data: CirculationData
    ) -> bool:
        """Return a boolean value indicating whether the specified LinkData object describes an open-access link.

        :param link_data: LinkData object
        :param circulation_data: CirculationData object
        """
        open_access_link = (
            link_data.rel == Hyperlink.OPEN_ACCESS_DOWNLOAD and link_data.href
        )

        if open_access_link:
            return True

        # Try to deduce if the ast_link is open-access, even if it doesn't explicitly say it is
        rights_uri = link_data.rights_uri or circulation_data.default_rights_uri
        open_access_rights_link = (
            link_data.media_type in Representation.BOOK_MEDIA_TYPES
            and bool(link_data.href)
            and rights_uri in RightsStatus.OPEN_ACCESS
        )

        return open_access_rights_link

    def _record_coverage_failure(
        self,
        failures: dict[str, list[CoverageFailure]],
        identifier: Identifier,
        error_message: str,
        transient: bool = True,
    ) -> CoverageFailure:
        """Record a new coverage failure.

        :param failures: Dictionary mapping publication identifiers to corresponding CoverageFailure objects
        :param identifier: Publication's identifier
        :param error_message: Message describing the failure
        :param transient: Boolean value indicating whether the failure is final or it can go away in the future
        :return: CoverageFailure object describing the error
        """
        if identifier.identifier is None:
            raise ValueError

        if identifier not in failures:
            failures[identifier.identifier] = []

        failure = CoverageFailure(
            identifier,
            error_message,
            data_source=self.data_source,
            transient=transient,
            collection=self.collection,
        )
        failures[identifier.identifier].append(failure)

        return failure

    def _record_publication_unrecognizable_identifier(
        self, identifier: str | None, title: str | None
    ) -> None:
        """Record a publication's unrecognizable identifier, i.e. identifier that has an unknown format
            and could not be parsed by CM.

        :param publication: OPDS 2.x publication object
        """
        if identifier is None:
            self.log.warning(f"Publication '{title}' does not have an identifier.")
        else:
            self.log.warning(
                f"Publication # {identifier} ('{title}') has an unrecognizable identifier."
            )

    def _parse_feed(self, feed: str | bytes) -> opds2.PublicationFeedNoValidation:
        try:
            return opds2.PublicationFeedNoValidation.model_validate_json(feed)
        except ValidationError as e:
            self.log.exception(f"Error parsing feed: {e}")
            raise

    def extract_next_links(self, feed: str | bytes) -> list[str]:
        """Extracts "next" links from the feed.

        :param feed: OPDS 2.0 feed
        :return: List of "next" links
        """
        try:
            parsed_feed = self._parse_feed(feed)
        except ValidationError:
            return []

        next_links = [
            next_link.href
            for next_link in parsed_feed.links.get_collection(
                rel=self.NEXT_LINK_RELATION
            )
        ]

        return next_links

    def extract_last_update_dates(
        self, feed: str | bytes
    ) -> list[tuple[str | None, datetime | None]]:
        """Extract last update date of the feed.

        :param feed: OPDS 2.0 feed
        :return: A list of 2-tuples containing publication's identifiers and their last modified dates
        """
        last_update_dates: list[tuple[str | None, datetime | None]] = []
        try:
            parsed_feed = self._parse_feed(feed)
        except ValidationError:
            return last_update_dates

        for publication_dict in parsed_feed.publications:
            try:
                publication = self._get_publication(publication_dict)
            except ValidationError:
                continue
            last_update_dates.append(
                (publication.metadata.identifier, publication.metadata.modified)
            )
        return last_update_dates

    def _parse_feed_links(self, links: CompactCollection[opds2.StrictLink]) -> None:
        """Parse the global feed links. Currently only parses the token endpoint link"""
        token_auth_link = links.get(rel=Hyperlink.TOKEN_AUTH)
        if token_auth_link is not None:
            self.collection.integration_configuration.context_update(
                {OPDS2API.TOKEN_AUTH_CONFIG_KEY: token_auth_link.href}
            )

    def _get_allowed_identifier(
        self, identifier: str | None, title: str | None
    ) -> Identifier | None:
        recognized_identifier = self.parse_identifier(identifier)
        if not recognized_identifier or not self._is_identifier_allowed(
            recognized_identifier
        ):
            self._record_publication_unrecognizable_identifier(identifier, title)
            return None
        return recognized_identifier

    def extract_feed_data(
        self, feed: str | bytes, feed_url: str | None = None
    ) -> tuple[dict[str, BibliographicData], dict[str, list[CoverageFailure]]]:
        """Turn an OPDS 2.0 feed into lists of BibliographicData and CirculationData objects.
        :param feed: OPDS 2.0 feed
        :param feed_url: Feed URL used to resolve relative links
        """
        try:
            parsed_feed = self._parse_feed(feed)
        except ValidationError:
            return {}, {}

        publication_bibliographic_dictionary = {}
        failures: dict[str, list[CoverageFailure]] = {}

        if parsed_feed.links:
            self._parse_feed_links(parsed_feed.links)

        for publication_dict in parsed_feed.publications:
            try:
                publication = self._get_publication(publication_dict)
            except ValidationError as e:
                raw_identifier = publication_dict.get("metadata", {}).get("identifier")
                raw_title = publication_dict.get("metadata", {}).get("title")
                recognized_identifier = self._get_allowed_identifier(
                    raw_identifier, raw_title
                )
                if recognized_identifier:
                    self._record_coverage_failure(
                        failures, recognized_identifier, str(e)
                    )

                continue
            recognized_identifier = self._get_allowed_identifier(
                publication.metadata.identifier, str(publication.metadata.title)
            )

            if not recognized_identifier:
                continue

            feed_self_url = parsed_feed.links.get(
                rel=rwpm.LinkRelations.self, raising=True
            ).href
            publication_bibliographic = self._extract_publication_bibliographic_data(
                publication, self.data_source_name, feed_self_url
            )

            # Make sure we have a primary identifier before trying to use it
            if publication_bibliographic.primary_identifier_data is not None:
                publication_bibliographic_dictionary[
                    publication_bibliographic.primary_identifier_data.identifier
                ] = publication_bibliographic

        return publication_bibliographic_dictionary, failures


class OPDS2ImportMonitor(OPDSImportMonitor):
    PROTOCOL = OPDS2API.label()
    MEDIA_TYPE = opds2.PublicationFeed.content_type(), "application/json"

    def _verify_media_type(self, url: str, resp: Response) -> None:
        # Make sure we got an OPDS feed, and not an error page that was
        # sent with a 200 status code.
        media_type = resp.headers.get("content-type")
        if not media_type or not any(x in media_type for x in self.MEDIA_TYPE):
            message = "Expected {} OPDS 2.0 feed, got {}".format(
                self.MEDIA_TYPE, media_type
            )

            raise BadResponseException(url, message=message, response=resp)

    def _get_accept_header(self) -> str:
        return "{}, {};q=0.9, */*;q=0.1".format(
            opds2.PublicationFeed.content_type(), "application/json"
        )
