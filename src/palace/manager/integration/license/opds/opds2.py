from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from typing import Any, cast
from urllib.parse import urljoin, urlparse

from celery.canvas import Signature
from flask_babel import lazy_gettext as _
from frozendict import frozendict
from pydantic import ValidationError
from sqlalchemy.orm import Session
from typing_extensions import Self, Unpack
from uritemplate import URITemplate

from palace.manager.api.circulation.base import BaseCirculationAPI
from palace.manager.api.circulation.exceptions import CannotFulfill
from palace.manager.api.circulation.fulfillment import RedirectFulfillment
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.contributor import ContributorData
from palace.manager.data_layer.format import FormatData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.link import LinkData
from palace.manager.data_layer.subject import SubjectData
from palace.manager.integration.base import integration_settings_load
from palace.manager.integration.license.opds.opds1 import (
    BaseOPDSAPI,
    OPDSImporterLibrarySettings,
    OPDSImporterSettings,
)
from palace.manager.integration.license.opds.requests import (
    OPDS2AuthType,
    get_opds_requests,
)
from palace.manager.integration.settings import (
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
)
from palace.manager.opds import opds2, rwpm
from palace.manager.opds.opds2 import AcquisitionObject, PublicationFeedNoValidation
from palace.manager.opds.types.link import CompactCollection
from palace.manager.service.integration_registry.license_providers import (
    LicenseProvidersRegistry,
)
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
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    LicensePoolDeliveryMechanism,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.util import first_or_default
from palace.manager.util.http import HTTP
from palace.manager.util.log import LoggerMixin


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

    @classmethod
    def import_task(cls, collection_id: int, force: bool = False) -> Signature:
        from palace.manager.celery.tasks.opds2 import import_collection

        return import_collection.s(collection_id, force=force)


class OPDS2Importer(LoggerMixin):

    def __init__(
        self,
        *,
        username: str | None,
        password: str | None,
        feed_base_url: str,
        data_source: str,
        ignored_identifier_types: list[str],
        max_retry_count: int,
        accept_header: str,
    ) -> None:
        """
        Constructor.
        """
        self._request = get_opds_requests(
            (OPDS2AuthType.BASIC if username and password else OPDS2AuthType.NONE),
            username,
            password,
            feed_base_url,
        )

        self._data_source_name = data_source
        self._ignored_identifier_types = ignored_identifier_types
        self._feed_base_url = feed_base_url
        self._max_retry_count = max_retry_count
        self._accept_header = accept_header

    @classmethod
    def from_collection(
        cls, collection: Collection, registry: LicenseProvidersRegistry
    ) -> Self:
        """Create an instance from a Collection."""
        if not registry.equivalent(collection.protocol, OPDS2API):
            raise PalaceValueError(
                f"Collection {collection.name} [id={collection.id} protocol={collection.protocol}] is not a OPDS2 collection."
            )
        settings = integration_settings_load(
            OPDS2API.settings_class(), collection.integration_configuration
        )
        return cls(
            username=settings.username,
            password=settings.password,
            feed_base_url=settings.external_account_id,
            data_source=settings.data_source,
            ignored_identifier_types=settings.ignored_identifier_types,
            max_retry_count=settings.max_retry_count,
            accept_header=settings.custom_accept_header,
        )

    def get_feed(self, url: str | None) -> PublicationFeedNoValidation:
        joined_url = urljoin(self._feed_base_url, url)
        self.log.info(f"Fetching OPDS2 feed page: {joined_url}")
        return self._request(
            "GET",
            joined_url,
            parser=PublicationFeedNoValidation.model_validate_json,
            allowed_response_codes=["2xx"],
            headers={"Accept": self._accept_header},
            max_retry_count=self._max_retry_count,
        )

    @classmethod
    def next_page(cls, feed: PublicationFeedNoValidation) -> str | None:
        """Get the next page URL from the feed."""
        next_link = feed.links.get(
            rel="next", type=PublicationFeedNoValidation.content_type()
        )
        if not next_link:
            return None
        return next_link.href

    def _is_identifier_allowed(self, identifier: IdentifierData) -> bool:
        """Check the identifier and return a boolean value indicating whether CM can import it.

        :param identifier: Identifier object
        :return: Boolean value indicating whether CM can import the identifier
        """
        return identifier.type not in self._ignored_identifier_types

    @classmethod
    def _get_publication(
        cls,
        publication: dict[str, Any],
    ) -> opds2.BasePublication:
        try:
            return opds2.Publication.model_validate(publication)
        except ValidationError as e:
            raw_identifier = publication.get("metadata", {}).get("identifier")
            cls.logger().exception(
                f"Error validating publication (identifier: {raw_identifier}): {e}"
            )
            raise

    @classmethod
    def update_integration_context(
        cls, feed: PublicationFeedNoValidation, collection: Collection
    ) -> bool:
        """Parse the global feed links. Currently only parses the token endpoint link"""
        links = feed.links
        token_auth_link = links.get(rel=Hyperlink.TOKEN_AUTH)
        if token_auth_link is None:
            return False

        integration = collection.integration_configuration
        if (
            integration.context.get(OPDS2API.TOKEN_AUTH_CONFIG_KEY)
            == token_auth_link.href
        ):
            # No change, so we don't need to update the context.
            return False

        integration.context_update(
            {OPDS2API.TOKEN_AUTH_CONFIG_KEY: token_auth_link.href}
        )
        return True

    def extract_feed_data(
        self, feed: PublicationFeedNoValidation
    ) -> list[BibliographicData]:
        """
        Turn an OPDS 2.0 feed into lists of BibliographicData and CirculationData objects.
        """
        results = []
        for publication_dict in feed.publications:
            try:
                publication = self._get_publication(publication_dict)
            except ValidationError as e:
                raw_identifier = publication_dict.get("metadata", {}).get("identifier")
                raw_title = publication_dict.get("metadata", {}).get("title")
                self.log.error(
                    f"Error validating publication (identifier: {raw_identifier}, title: {raw_title}): {e}"
                )
                continue

            feed_self_url = feed.links.get(
                rel=rwpm.LinkRelations.self, raising=True
            ).href
            try:
                publication_bibliographic = Opds2Extractor.extract_publication_data(
                    publication, self._data_source_name, feed_self_url
                )
            except PalaceValueError:
                self.log.exception(
                    "Error extracting publication data. Most likely the publications identifier could not be parsed. Skipping publication."
                )
                continue

            # We cast here because we know that Opds2Extractor.extract_publication_data always sets
            # the primary_identifier_data field to an IdentifierData object.
            # TODO: Maybe we can tighten up the type hint for BibliographicData to reflect this?
            identifier = cast(
                IdentifierData, publication_bibliographic.primary_identifier_data
            )
            if identifier is None or not self._is_identifier_allowed(identifier):
                self.log.warning(
                    f"Publication {identifier} not imported because its identifier type is not allowed: {identifier.type}"
                )
                continue

            results.append(publication_bibliographic)

        return results

    @classmethod
    def is_changed(cls, session: Session, bibliographic: BibliographicData) -> bool:
        edition = bibliographic.load_edition(session)
        if not edition:
            return True

        # If we don't have any information about the last update time, assume we need to update.
        if edition.updated_at is None or bibliographic.data_source_last_updated is None:
            return True

        if bibliographic.data_source_last_updated > edition.updated_at:
            return True

        cls.logger().info(
            f"Publication {bibliographic.primary_identifier_data} is unchanged. Last updated at "
            f"{edition.updated_at}, data source last updated at {bibliographic.data_source_last_updated}"
        )
        return False


class Opds2Extractor(LoggerMixin):
    _CONTRIBUTOR_ROLE_MAPPING: frozendict[str, str] = frozendict(
        {
            # We reverse the mapping because there are some roles that have the same code, and we
            # want to prioritize the first time the code appears in the list.
            code.lower(): role
            for role, code in reversed(Contributor.MARC_ROLE_CODES.items())
        }
        | {role.lower(): role for role in Contributor.Role}
    )

    @classmethod
    def _extract_subjects(cls, subjects: Sequence[rwpm.Subject]) -> list[SubjectData]:
        """Extract a list of SubjectData objects from the rwpm.Subject.

        :param subjects: Parsed subject object
        :return: List of subjects metadata
        """
        cls.logger().debug("Started extracting subjects metadata")

        subject_metadata_list = []

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

            subject_metadata = SubjectData(
                type=subject_type,
                identifier=subject.code,
                name=str(subject.name),
                weight=1,
            )

            subject_metadata_list.append(subject_metadata)

            cls.logger().debug(
                "Finished extracting subject metadata from {}: {}".format(
                    subject.model_dump_json(), subject_metadata
                )
            )

        cls.logger().debug(
            f"Finished extracting subjects metadata: {subject_metadata_list}"
        )

        return subject_metadata_list

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
    def _extract_contributors(
        cls,
        contributors: Sequence[rwpm.Contributor],
        default_role: str,
    ) -> list[ContributorData]:
        """Extract a list of ContributorData objects from rwpm.Contributor.

        :param contributors: Parsed contributor object
        :param default_role: Default role
        :return: List of contributors metadata
        """
        cls.logger().debug("Started extracting contributors metadata")

        contributor_metadata_list = []

        for contributor in contributors:
            cls.logger().debug(
                f"Started extracting contributor metadata from {contributor.model_dump_json()}"
            )

            if isinstance(contributor, rwpm.ContributorWithRole):
                roles = cls._extract_contributor_roles(contributor.roles, default_role)
            else:
                roles = [default_role]

            contributor_metadata = ContributorData(
                sort_name=contributor.sort_as,
                display_name=str(contributor.name),
                family_name=None,
                wikipedia_name=None,
                roles=roles,
            )

            cls.logger().debug(
                f"Finished extracting contributor metadata from {contributor.model_dump_json()}: {contributor_metadata}"
            )

            contributor_metadata_list.append(contributor_metadata)

        cls.logger().debug(
            f"Finished extracting contributors metadata: {contributor_metadata_list}"
        )

        return contributor_metadata_list

    @classmethod
    def _extract_link(
        cls, link: opds2.Link, feed_self_url: str, default_link_rel: str | None = None
    ) -> LinkData:
        """Extract a LinkData object from opds2.Link.

        :param link: link
        :param feed_self_url: Feed's self URL
        :param default_link_rel: Default link's relation

        :return: Link metadata
        """
        cls.logger().debug(
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

        cls.logger().debug(
            f"Finished extracting link metadata from {link.model_dump_json()}: {link_metadata}"
        )

        return link_metadata

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
    def _extract_image_links(
        cls, publication: opds2.BasePublication, feed_self_url: str
    ) -> list[LinkData]:
        """Extracts a list of LinkData objects containing information about artwork.

        :param publication: Publication object
        :param feed_self_url: Feed's self URL
        :return: List of links metadata
        """
        cls.logger().debug(f"Started extracting image links from {publication.images}")

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
            cover_link = cls._extract_link(
                sorted_raw_image_links[0],
                feed_self_url,
                default_link_rel=Hyperlink.IMAGE,
            )
            image_links.append(cover_link)

        if len(sorted_raw_image_links) > 1:
            cover_link = cls._extract_link(
                sorted_raw_image_links[1],
                feed_self_url,
                default_link_rel=Hyperlink.THUMBNAIL_IMAGE,
            )
            image_links.append(cover_link)

        cls.logger().debug(
            f"Finished extracting image links from {publication.images}: {image_links}"
        )

        return image_links

    @classmethod
    def _extract_links(
        cls, publication: opds2.BasePublication, feed_self_url: str
    ) -> list[LinkData]:
        """Extract a list of LinkData objects from opds2.Publication.

        :param publication: Publication object
        :param feed_self_url: Feed's self URL
        :return: List of links metadata
        """
        cls.logger().debug(f"Started extracting links from {publication.links}")

        links = []

        for link in publication.links:
            link_metadata = cls._extract_link(link, feed_self_url)
            links.append(link_metadata)

        description_link = cls._extract_description_link(publication)
        if description_link:
            links.append(description_link)

        image_links = cls._extract_image_links(publication, feed_self_url)
        links.extend(image_links)

        cls.logger().debug(
            f"Finished extracting links from {publication.links}: {links}"
        )

        return links

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

        if not link.properties.availability.available:
            cls.logger().info(f"Link unavailable. Skipping. {link.model_dump_json()}")
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

        cls.logger().debug(
            "Finished extracting media types and a DRM scheme from {}: {}".format(
                link, media_types_and_drm_scheme
            )
        )

        return media_types_and_drm_scheme

    @classmethod
    def _extract_medium_from_links(
        cls, links: CompactCollection[opds2.Link]
    ) -> str | None:
        """Extract the publication's medium from its links.

        :param links: List of links
        :return: Publication's medium
        """
        derived = None

        for link in links:
            if not link.rels or not link.type or not cls._is_acquisition_link(link):
                continue

            link_media_type, _ = first_or_default(
                cls._extract_media_types_and_drm_scheme_from_link(link),
                default=(None, None),
            )
            derived = Edition.medium_from_media_type(link_media_type)

            if derived:
                break

        return derived

    @classmethod
    def _extract_medium(
        cls,
        publication: opds2.BasePublication,
    ) -> str | None:
        """Extract the publication's medium from its metadata.

        :param publication: Publication object
        :return: Publication's medium
        """
        medium = Edition.additional_type_to_medium.get(
            publication.metadata.type, cls._extract_medium_from_links(publication.links)
        )

        return medium

    @classmethod
    def _extract_identifier(cls, identifier: str) -> IdentifierData:
        """
        Extract the publication's identifier from its metadata.

        Raises PalaceValueError if the identifier cannot be parsed.
        """
        return IdentifierData.parse_urn(identifier)

    @classmethod
    def _find_formats_in_non_open_access_acquisition_links(
        cls,
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
            if not cls._is_acquisition_link(ast_link):
                continue
            if cls._is_open_access_link_(parsed_link, circulation_data):
                continue

            for (
                content_type,
                drm_scheme,
            ) in cls._extract_media_types_and_drm_scheme_from_link(ast_link):
                formats.append(
                    FormatData(
                        content_type=content_type,
                        drm_scheme=drm_scheme,
                        link=parsed_link,
                        rights_uri=rights_uri,
                    )
                )

        return formats

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

    @classmethod
    def _extract_published_date(cls, published: datetime | date | None) -> date | None:
        if isinstance(published, datetime):
            return published.date()
        return published

    @classmethod
    def _extract_circulation_data(
        cls,
        publication: opds2.BasePublication,
        identifier: IdentifierData,
        data_source_name: str,
        links: list[LinkData],
    ) -> CirculationData:
        # FIXME: It seems that OPDS 2.0 spec doesn't contain information about rights so we use the default one
        rights_uri = RightsStatus.rights_uri_from_string("")

        if publication.metadata.availability.available:
            licenses_owned = LicensePool.UNLIMITED_ACCESS
            licenses_available = LicensePool.UNLIMITED_ACCESS
        else:
            licenses_owned = 0
            licenses_available = 0

        time_tracking = publication.metadata.time_tracking
        circulation_data = CirculationData(
            default_rights_uri=rights_uri,
            data_source_name=data_source_name,
            primary_identifier_data=identifier,
            links=links,
            licenses_owned=licenses_owned,
            licenses_available=licenses_available,
            licenses_reserved=0,
            patrons_in_hold_queue=0,
            formats=[],
            should_track_playtime=time_tracking,
        )
        formats = cls._find_formats_in_non_open_access_acquisition_links(
            publication.links, links, rights_uri, circulation_data
        )
        circulation_data.formats.extend(formats)
        return circulation_data

    @classmethod
    def _extract_bibliographic_data(
        cls,
        publication: opds2.BasePublication,
        identifier: IdentifierData,
        data_source_name: str,
        links: list[LinkData],
    ) -> BibliographicData:
        title = str(publication.metadata.title)
        subtitle = (
            str(publication.metadata.subtitle)
            if publication.metadata.subtitle
            else None
        )
        languages = first_or_default(publication.metadata.languages)
        medium = cls._extract_medium(publication)

        first_publisher = first_or_default(publication.metadata.publishers)
        publisher = str(first_publisher.name) if first_publisher else None

        first_imprint = first_or_default(publication.metadata.imprints)
        imprint = str(first_imprint.name) if first_imprint else None
        published = cls._extract_published_date(publication.metadata.published)
        subjects = cls._extract_subjects(publication.metadata.subjects)

        contributors = (
            cls._extract_contributors(
                publication.metadata.authors, Contributor.Role.AUTHOR
            )
            + cls._extract_contributors(
                publication.metadata.translators, Contributor.Role.TRANSLATOR
            )
            + cls._extract_contributors(
                publication.metadata.editors, Contributor.Role.EDITOR
            )
            + cls._extract_contributors(
                publication.metadata.artists, Contributor.Role.ARTIST
            )
            + cls._extract_contributors(
                publication.metadata.illustrators, Contributor.Role.ILLUSTRATOR
            )
            + cls._extract_contributors(
                publication.metadata.letterers, Contributor.Role.LETTERER
            )
            + cls._extract_contributors(
                publication.metadata.pencilers, Contributor.Role.PENCILER
            )
            + cls._extract_contributors(
                publication.metadata.colorists, Contributor.Role.COLORIST
            )
            + cls._extract_contributors(
                publication.metadata.inkers, Contributor.Role.INKER
            )
            + cls._extract_contributors(
                publication.metadata.narrators, Contributor.Role.NARRATOR
            )
            + cls._extract_contributors(
                publication.metadata.contributors, Contributor.Role.CONTRIBUTOR
            )
        )

        # FIXME: There are no measurements in OPDS 2.0
        measurements: list[Any] = []

        # FIXME: There is no series information in OPDS 2.0
        series = None
        series_position = None

        last_opds_update = publication.metadata.modified

        # Audiobook duration
        duration = publication.metadata.duration

        return BibliographicData(
            data_source_name=data_source_name,
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
    def extract_publication_data(
        cls,
        publication: opds2.BasePublication,
        data_source_name: str,
        feed_self_url: str,
    ) -> BibliographicData:
        """Extract a BibliographicData object from OPDS2 Publication.

        :param publication: Publication object
        :param data_source_name: Data source's name
        :param feed_self_url: Feed's self URL

        :return: Publication's BibliographicData
        """
        cls.logger().debug(f"Started extracting data from publication {publication}")

        identifier = cls._extract_identifier(publication.metadata.identifier)
        links = cls._extract_links(publication, feed_self_url)
        circulation = cls._extract_circulation_data(
            publication, identifier, data_source_name, links
        )
        bibliographic = cls._extract_bibliographic_data(
            publication, identifier, data_source_name, links
        )

        bibliographic.circulation = circulation

        if (
            bibliographic.medium != Edition.AUDIO_MEDIUM
            and circulation.should_track_playtime is True
        ):
            circulation.should_track_playtime = False
            cls.logger().warning(
                f"Ignoring the time tracking flag for entry {identifier}"
            )

        cls.logger().debug(
            "Finished extracting bibliographic data from publication {}: {}".format(
                publication, bibliographic
            )
        )

        return bibliographic
