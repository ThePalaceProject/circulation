from abc import ABC, abstractmethod
from collections.abc import Iterable

from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.integration.license.opds.data import FailedPublication
from palace.manager.opds.odl.info import LicenseInfo
from palace.manager.opds.odl.odl import License
from palace.manager.util.log import LoggerMixin


class OpdsExtractor[FeedType, PublicationType](LoggerMixin, ABC):
    """
    Base class for OPDS extractors.

    This class defines the interface for extracting bibliographic data from OPDS feeds.
    """

    @abstractmethod
    def feed_parse(self, feed: bytes) -> FeedType:
        """
        Parse the feed from bytes to a FeedType object.
        """

    @abstractmethod
    def feed_next_url(self, feed: FeedType) -> str | None:
        """
        Get the next page URL from the feed.
        """

    @abstractmethod
    def feed_publications(
        self, feed: FeedType
    ) -> Iterable[PublicationType | FailedPublication]:
        """
        Extracts the publications from the feed.

        Returns an iterable of PublicationType objects or FailedPublication objects
        if extraction fails.
        """

    @abstractmethod
    def publication_licenses(self, publication: PublicationType) -> Iterable[License]:
        """
        Extract the licenses from the publication.

        Returns an iterable of License objects.
        """

    @abstractmethod
    def publication_available(self, publication: PublicationType) -> bool:
        """Check if the publication is available."""

    @abstractmethod
    def publication_identifier(self, publication: PublicationType) -> IdentifierData:
        """
        Extract the publication's identifier from its metadata.

        Raises PalaceValueError if the identifier cannot be parsed.
        """

    @abstractmethod
    def failure_from_publication(
        self, publication: PublicationType, error: Exception, error_message: str
    ) -> FailedPublication:
        """
        Create a FailedPublication from a publication and an error.
        """

    @abstractmethod
    def publication_bibliographic(
        self,
        identifier: IdentifierData,
        publication: PublicationType,
        license_info_documents: dict[str, LicenseInfo] | None = None,
    ) -> BibliographicData:
        """
        Extract bibliographic data from the publication.

        Returns a BibliographicData object containing the extracted data.
        """
