import datetime
import json
from collections.abc import Generator
from contextlib import nullcontext
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError
from requests import Response

from palace.manager.api.circulation import (
    CirculationAPI,
    Fulfillment,
    RedirectFulfillment,
)
from palace.manager.api.circulation_exceptions import CannotFulfill
from palace.manager.core.opds2_import import OPDS2API, OPDS2Importer, OPDS2ImportMonitor
from palace.manager.sqlalchemy.constants import (
    EditionConstants,
    IdentifierType,
    MediaTypes,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.contributor import Contribution, Contributor
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.patron import Loan
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.http import BadResponseException
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import OPDS2FilesFixture
from tests.mocks.mock import MockRequestsResponse


class OPDS2Test:
    @staticmethod
    def _get_edition_by_identifier(editions, identifier):
        """Find an edition in the list by its identifier.

        :param editions: List of editions
        :type editions: List[Edition]

        :return: Edition with the specified id (if any)
        :rtype: Optional[Edition]
        """
        for edition in editions:
            if edition.primary_identifier.urn == identifier:
                return edition

        return None

    @staticmethod
    def _get_license_pool_by_identifier(pools, identifier):
        """Find a license pool in the list by its identifier.

        :param pools: List of license pools
        :type pools: List[LicensePool]

        :return: Edition with the specified id (if any)
        :rtype: Optional[LicensePool]
        """
        for pool in pools:
            if pool.identifier.urn == identifier:
                return pool

        return None

    @staticmethod
    def _get_work_by_identifier(works, identifier):
        """Find a license pool in the list by its identifier.

        :param works: List of license pools
        :type works: List[Work]

        :return: Edition with the specified id (if any)
        :rtype: Optional[Work]
        """
        for work in works:
            if work.presentation_edition.primary_identifier.urn == identifier:
                return work

        return None


class OPDS2ImporterFixture:
    def __init__(self, db: DatabaseTransactionFixture) -> None:
        self.transaction = db
        self.collection = db.collection(
            protocol=OPDS2API,
            settings=db.opds_settings(
                external_account_id="http://opds2.example.org/feed",
                data_source="OPDS 2.0 Data Source",
            ),
        )
        self.library = db.default_library()
        self.collection.associated_libraries.append(self.library)
        self.data_source = DataSource.lookup(
            db.session, "OPDS 2.0 Data Source", autocreate=True
        )
        self.collection.data_source = self.data_source
        self.importer = OPDS2Importer(db.session, self.collection)

    @staticmethod
    def get_delivery_mechanisms(
        license_pool: LicensePool,
    ) -> set[tuple[str | None, str | None]]:
        return {
            (dm.delivery_mechanism.content_type, dm.delivery_mechanism.drm_scheme)
            for dm in license_pool.delivery_mechanisms
        }


@pytest.fixture
def opds2_importer_fixture(
    db: DatabaseTransactionFixture,
) -> OPDS2ImporterFixture:
    return OPDS2ImporterFixture(db)


class TestOPDS2Importer(OPDS2Test):
    MOBY_DICK_ISBN_IDENTIFIER = "urn:isbn:978-3-16-148410-0"
    HUCKLEBERRY_FINN_URI_IDENTIFIER = "http://example.org/huckleberry-finn"
    POSTMODERNISM_PROQUEST_IDENTIFIER = (
        "urn:librarysimplified.org/terms/id/ProQuest%20Doc%20ID/181639"
    )

    def test__get_publication(
        self,
        opds2_importer_fixture: OPDS2ImporterFixture,
        opds2_files_fixture: OPDS2FilesFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        importer = opds2_importer_fixture.importer

        # Normally _get_publication just turns a publications dict into a Publication model
        opds2_feed = json.loads(opds2_files_fixture.sample_text("feed.json"))
        publication_dict = opds2_feed["publications"][0]
        publication = importer._get_publication(publication_dict)
        assert publication.metadata.identifier == "urn:isbn:978-3-16-148410-0"

        # However if there is a validation error, it adds a helpful log message
        # before raising the validation error
        with pytest.raises(
            ValidationError, match="3 validation errors for Publication"
        ):
            importer._get_publication({})

        assert "3 validation errors for Publication" in caplog.text

    @pytest.mark.parametrize(
        "name,manifest_type",
        [
            ("manifest encoded as a string", "string"),
            ("manifest encoded as a byte-string", "bytes"),
        ],
    )
    def test_opds2_importer_correctly_imports_valid_opds2_feed(
        self,
        opds2_importer_fixture: OPDS2ImporterFixture,
        opds2_files_fixture: OPDS2FilesFixture,
        name: str,
        manifest_type: str,
    ):
        """Ensure that OPDS2Importer correctly imports valid OPDS 2.x feeds.
        :param manifest_type: Manifest's type: string or binary
        """
        # Arrange
        data, transaction, session = (
            opds2_importer_fixture,
            opds2_importer_fixture.transaction,
            opds2_importer_fixture.transaction.session,
        )
        content_server_feed_text = opds2_files_fixture.sample_text("feed.json")
        content_server_feed: str | bytes

        if manifest_type == "bytes":
            content_server_feed = content_server_feed_text.encode()
        else:
            content_server_feed = content_server_feed_text

        # Act
        imported_editions, pools, works, failures = data.importer.import_from_feed(
            content_server_feed
        )

        # Assert

        # 1. Make sure that editions contain all required metadata
        assert isinstance(imported_editions, list)
        assert 3 == len(imported_editions)

        # 1.1. Edition with open-access links (Moby-Dick)
        moby_dick_edition = self._get_edition_by_identifier(
            imported_editions, self.MOBY_DICK_ISBN_IDENTIFIER
        )
        assert isinstance(moby_dick_edition, Edition)

        assert "Moby-Dick" == moby_dick_edition.title
        assert "eng" == moby_dick_edition.language
        assert "eng" == moby_dick_edition.language
        assert EditionConstants.AUDIO_MEDIUM == moby_dick_edition.medium
        assert "Herman Melville" == moby_dick_edition.author
        assert moby_dick_edition.duration == 100.2

        assert 1 == len(moby_dick_edition.author_contributors)
        [moby_dick_author] = moby_dick_edition.author_contributors
        assert isinstance(moby_dick_author, Contributor)
        assert "Herman Melville" == moby_dick_author.display_name
        assert "Melville, Herman" == moby_dick_author.sort_name

        assert 1 == len(moby_dick_author.contributions)
        [moby_dick_author_contribution] = moby_dick_author.contributions
        assert isinstance(moby_dick_author_contribution, Contribution)
        assert moby_dick_author == moby_dick_author_contribution.contributor
        assert moby_dick_edition == moby_dick_author_contribution.edition
        assert Contributor.Role.AUTHOR == moby_dick_author_contribution.role

        assert data.data_source == moby_dick_edition.data_source

        assert "Test Publisher" == moby_dick_edition.publisher
        assert datetime.date(2015, 9, 29) == moby_dick_edition.published

        assert "http://example.org/cover.jpg" == moby_dick_edition.cover_full_url
        assert (
            "http://example.org/cover-small.jpg"
            == moby_dick_edition.cover_thumbnail_url
        )

        # 1.2. Edition with non open-access acquisition links (Adventures of Huckleberry Finn)
        huckleberry_finn_edition = self._get_edition_by_identifier(
            imported_editions, self.HUCKLEBERRY_FINN_URI_IDENTIFIER
        )
        assert isinstance(huckleberry_finn_edition, Edition)

        assert "Adventures of Huckleberry Finn" == huckleberry_finn_edition.title
        assert "eng" == huckleberry_finn_edition.language
        assert EditionConstants.BOOK_MEDIUM == huckleberry_finn_edition.medium
        assert "Samuel Langhorne Clemens, Mark Twain" == huckleberry_finn_edition.author

        assert 2 == len(huckleberry_finn_edition.author_contributors)
        huckleberry_finn_authors = huckleberry_finn_edition.author_contributors

        assert isinstance(huckleberry_finn_authors[0], Contributor)
        assert "Mark Twain" == huckleberry_finn_authors[0].display_name
        assert "Twain, Mark" == huckleberry_finn_authors[0].sort_name

        assert 1 == len(huckleberry_finn_authors[0].contributions)
        [huckleberry_finn_author_contribution] = huckleberry_finn_authors[
            0
        ].contributions
        assert isinstance(huckleberry_finn_author_contribution, Contribution)
        assert (
            huckleberry_finn_authors[0]
            == huckleberry_finn_author_contribution.contributor
        )
        assert huckleberry_finn_edition == huckleberry_finn_author_contribution.edition
        assert Contributor.Role.AUTHOR == huckleberry_finn_author_contribution.role

        assert isinstance(huckleberry_finn_authors[1], Contributor)
        assert "Samuel Langhorne Clemens" == huckleberry_finn_authors[1].display_name
        assert "Clemens, Samuel Langhorne" == huckleberry_finn_authors[1].sort_name

        assert 1 == len(huckleberry_finn_authors[1].contributions)
        [huckleberry_finn_author_contribution] = huckleberry_finn_authors[
            1
        ].contributions
        assert isinstance(huckleberry_finn_author_contribution, Contribution)
        assert (
            huckleberry_finn_authors[1]
            == huckleberry_finn_author_contribution.contributor
        )
        assert huckleberry_finn_edition == huckleberry_finn_author_contribution.edition
        assert Contributor.Role.AUTHOR == huckleberry_finn_author_contribution.role

        assert data.data_source == huckleberry_finn_edition.data_source

        assert "Test Publisher" == huckleberry_finn_edition.publisher
        assert datetime.date(2014, 9, 28) == huckleberry_finn_edition.published

        assert "http://example.org/cover.jpg" == moby_dick_edition.cover_full_url

        # 2. Make sure that license pools have correct configuration
        assert isinstance(pools, list)
        assert 3 == len(pools)

        # 2.1. Edition with open-access links (Moby-Dick)
        moby_dick_license_pool = self._get_license_pool_by_identifier(
            pools, self.MOBY_DICK_ISBN_IDENTIFIER
        )
        assert isinstance(moby_dick_license_pool, LicensePool)
        assert moby_dick_license_pool.open_access
        assert LicensePool.UNLIMITED_ACCESS == moby_dick_license_pool.licenses_owned
        assert LicensePool.UNLIMITED_ACCESS == moby_dick_license_pool.licenses_available
        assert True == moby_dick_license_pool.should_track_playtime

        assert opds2_importer_fixture.get_delivery_mechanisms(
            moby_dick_license_pool
        ) == {(MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE, DeliveryMechanism.NO_DRM)}

        # 2.2. Edition with non open-access acquisition links (Adventures of Huckleberry Finn)
        huckleberry_finn_license_pool = self._get_license_pool_by_identifier(
            pools, self.HUCKLEBERRY_FINN_URI_IDENTIFIER
        )
        assert True == isinstance(huckleberry_finn_license_pool, LicensePool)
        assert False == huckleberry_finn_license_pool.open_access
        assert (
            LicensePool.UNLIMITED_ACCESS == huckleberry_finn_license_pool.licenses_owned
        )
        assert (
            LicensePool.UNLIMITED_ACCESS
            == huckleberry_finn_license_pool.licenses_available
        )
        assert False == huckleberry_finn_license_pool.should_track_playtime

        assert opds2_importer_fixture.get_delivery_mechanisms(
            huckleberry_finn_license_pool
        ) == {
            (MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
            (MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.LCP_DRM),
        }

        # 2.3 Edition with non open-access acquisition links (The Politics of Postmodernism)
        postmodernism_license_pool = self._get_license_pool_by_identifier(
            pools, self.POSTMODERNISM_PROQUEST_IDENTIFIER
        )
        assert True == isinstance(postmodernism_license_pool, LicensePool)
        assert False == postmodernism_license_pool.open_access
        assert LicensePool.UNLIMITED_ACCESS == postmodernism_license_pool.licenses_owned
        assert (
            LicensePool.UNLIMITED_ACCESS
            == postmodernism_license_pool.licenses_available
        )

        assert opds2_importer_fixture.get_delivery_mechanisms(
            postmodernism_license_pool
        ) == {
            (MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
            (MediaTypes.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
        }

        # 3. Make sure that work objects contain all the required metadata
        assert isinstance(works, list)
        assert 3 == len(works)

        # 3.1. Work (Moby-Dick)
        moby_dick_work = self._get_work_by_identifier(
            works, self.MOBY_DICK_ISBN_IDENTIFIER
        )
        assert isinstance(moby_dick_work, Work)
        assert moby_dick_edition == moby_dick_work.presentation_edition
        assert 1 == len(moby_dick_work.license_pools)
        assert moby_dick_license_pool == moby_dick_work.license_pools[0]

        # 3.2. Work (Adventures of Huckleberry Finn)
        huckleberry_finn_work = self._get_work_by_identifier(
            works, self.HUCKLEBERRY_FINN_URI_IDENTIFIER
        )
        assert isinstance(huckleberry_finn_work, Work)
        assert huckleberry_finn_edition == huckleberry_finn_work.presentation_edition
        assert 1 == len(huckleberry_finn_work.license_pools)
        assert huckleberry_finn_license_pool == huckleberry_finn_work.license_pools[0]
        assert (
            "Adventures of Huckleberry Finn is a novel by Mark Twain, first published in the United Kingdom in "
            "December 1884 and in the United States in February 1885."
            == huckleberry_finn_work.summary_text
        )

    @pytest.mark.parametrize(
        "this_identifier_type,ignore_identifier_type,identifier",
        [
            (
                IdentifierType.ISBN,
                [IdentifierType.URI, IdentifierType.PROQUEST_ID],
                MOBY_DICK_ISBN_IDENTIFIER,
            ),
            (
                IdentifierType.URI,
                [IdentifierType.ISBN, IdentifierType.PROQUEST_ID],
                HUCKLEBERRY_FINN_URI_IDENTIFIER,
            ),
            (
                IdentifierType.PROQUEST_ID,
                [IdentifierType.ISBN, IdentifierType.URI],
                POSTMODERNISM_PROQUEST_IDENTIFIER,
            ),
        ],
    )
    def test_opds2_importer_skips_publications_with_unsupported_identifier_types(
        self,
        opds2_importer_fixture: OPDS2ImporterFixture,
        opds2_files_fixture: OPDS2FilesFixture,
        this_identifier_type,
        ignore_identifier_type: list[IdentifierType],
        identifier: str,
    ) -> None:
        """Ensure that OPDS2Importer imports only publications having supported identifier types.
        This test imports the feed consisting of two publications,
        each having a different identifier type: ISBN and URI.
        First, it tries to import the feed marking ISBN as the only supported identifier type. Secondly, it uses URI.
        Each time it checks that CM imported only the publication having the selected identifier type.
        """
        data, transaction, session = (
            opds2_importer_fixture,
            opds2_importer_fixture.transaction,
            opds2_importer_fixture.transaction.session,
        )

        # Arrange
        # Update the list of supported identifier types in the collection's configuration settings
        # and set the identifier type passed as a parameter as the only supported identifier type.
        data.importer.ignored_identifier_types = [
            t.value for t in ignore_identifier_type
        ]

        content_server_feed = opds2_files_fixture.sample_text("feed.json")

        # Act
        imported_editions, pools, works, failures = data.importer.import_from_feed(
            content_server_feed
        )

        # Assert

        # Ensure that that CM imported only the edition having the selected identifier type.
        assert isinstance(imported_editions, list)
        assert 1 == len(imported_editions)
        assert (
            imported_editions[0].primary_identifier.type == this_identifier_type.value
        )

        # Ensure that it was parsed correctly and available by its identifier.
        edition = self._get_edition_by_identifier(imported_editions, identifier)
        assert edition is not None

    def test_auth_token_feed(
        self,
        opds2_importer_fixture: OPDS2ImporterFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ):
        data, transaction, session = (
            opds2_importer_fixture,
            opds2_importer_fixture.transaction,
            opds2_importer_fixture.transaction.session,
        )

        content = opds2_files_fixture.sample_text("auth_token_feed.json")
        imported_editions, pools, works, failures = data.importer.import_from_feed(
            content
        )
        token_endpoint = data.collection.integration_configuration.context.get(
            OPDS2API.TOKEN_AUTH_CONFIG_KEY
        )

        # Did the token endpoint get stored correctly?
        assert token_endpoint == "http://example.org/auth?userName={patron_id}"

    def test_opds2_importer_imports_feeds_with_availability_info(
        self,
        opds2_importer_fixture: OPDS2ImporterFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ):
        """Ensure that OPDS2Importer correctly imports feeds with availability information."""
        data, transaction, session = (
            opds2_importer_fixture,
            opds2_importer_fixture.transaction,
            opds2_importer_fixture.transaction.session,
        )
        feed_json = json.loads(opds2_files_fixture.sample_text("feed.json"))

        moby_dick_metadata = feed_json["publications"][0]["metadata"]
        huckleberry_finn_metadata = feed_json["publications"][1]["metadata"]
        postmodernism_metadata = feed_json["publications"][2]["metadata"]

        week_ago = utc_now() - datetime.timedelta(days=7)
        moby_dick_metadata["availability"] = {
            "state": "unavailable",
        }
        huckleberry_finn_metadata["availability"] = {
            "state": "available",
        }
        postmodernism_metadata["availability"] = {
            "state": "unavailable",
            "until": week_ago.isoformat(),
        }

        imported_editions, pools, works, failures = data.importer.import_from_feed(
            json.dumps(feed_json)
        )

        # Make we have the correct number of editions
        assert isinstance(imported_editions, list)
        assert len(imported_editions) == 3

        # Make we have the correct number of licensepools
        assert isinstance(pools, list)
        assert len(pools) == 3

        # Moby dick should be imported but is unavailable
        moby_dick_edition = self._get_edition_by_identifier(
            imported_editions, self.MOBY_DICK_ISBN_IDENTIFIER
        )
        assert isinstance(moby_dick_edition, Edition)

        assert moby_dick_edition.title == "Moby-Dick"

        moby_dick_license_pool = self._get_license_pool_by_identifier(
            pools, self.MOBY_DICK_ISBN_IDENTIFIER
        )
        assert isinstance(moby_dick_license_pool, LicensePool)
        assert moby_dick_license_pool.open_access
        assert moby_dick_license_pool.licenses_owned == 0
        assert moby_dick_license_pool.licenses_available == 0

        # Adventures of Huckleberry Finn is imported and is available
        huckleberry_finn_edition = self._get_edition_by_identifier(
            imported_editions, self.HUCKLEBERRY_FINN_URI_IDENTIFIER
        )
        assert isinstance(huckleberry_finn_edition, Edition)

        assert huckleberry_finn_edition.title == "Adventures of Huckleberry Finn"

        huckleberry_finn_license_pool = self._get_license_pool_by_identifier(
            pools, self.HUCKLEBERRY_FINN_URI_IDENTIFIER
        )
        assert isinstance(huckleberry_finn_license_pool, LicensePool) is True
        assert huckleberry_finn_license_pool.open_access is False
        assert (
            huckleberry_finn_license_pool.licenses_owned == LicensePool.UNLIMITED_ACCESS
        )
        assert (
            huckleberry_finn_license_pool.licenses_available
            == LicensePool.UNLIMITED_ACCESS
        )

        # Politics of postmodernism is unavailable, but it is past the until date, so it
        # should be available
        postmodernism_edition = self._get_edition_by_identifier(
            imported_editions, self.POSTMODERNISM_PROQUEST_IDENTIFIER
        )
        assert isinstance(postmodernism_edition, Edition)

        assert postmodernism_edition.title == "The Politics of Postmodernism"

        postmodernism_license_pool = self._get_license_pool_by_identifier(
            pools, self.POSTMODERNISM_PROQUEST_IDENTIFIER
        )
        assert isinstance(postmodernism_license_pool, LicensePool) is True
        assert postmodernism_license_pool.open_access is False
        assert postmodernism_license_pool.licenses_owned == LicensePool.UNLIMITED_ACCESS
        assert (
            postmodernism_license_pool.licenses_available
            == LicensePool.UNLIMITED_ACCESS
        )

        # We harvest the feed again but this time the availability has changed
        moby_dick_metadata["availability"]["state"] = "available"
        moby_dick_metadata["modified"] = utc_now().isoformat()

        huckleberry_finn_metadata["availability"]["state"] = "unavailable"
        huckleberry_finn_metadata["modified"] = utc_now().isoformat()

        del postmodernism_metadata["availability"]
        postmodernism_metadata["modified"] = utc_now().isoformat()

        imported_editions, pools, works, failures = data.importer.import_from_feed(
            json.dumps(feed_json)
        )

        # Make we have the correct number of editions
        assert isinstance(imported_editions, list)
        assert len(imported_editions) == 3

        # Make we have the correct number of licensepools
        assert isinstance(pools, list)
        assert len(pools) == 3

        # Moby dick should be imported and is now available
        moby_dick_edition = self._get_edition_by_identifier(
            imported_editions, self.MOBY_DICK_ISBN_IDENTIFIER
        )
        assert isinstance(moby_dick_edition, Edition)

        assert moby_dick_edition.title == "Moby-Dick"

        moby_dick_license_pool = self._get_license_pool_by_identifier(
            pools, self.MOBY_DICK_ISBN_IDENTIFIER
        )
        assert isinstance(moby_dick_license_pool, LicensePool)
        assert moby_dick_license_pool.open_access
        assert moby_dick_license_pool.licenses_owned == LicensePool.UNLIMITED_ACCESS
        assert moby_dick_license_pool.licenses_available == LicensePool.UNLIMITED_ACCESS

        # Adventures of Huckleberry Finn is imported and is now unavailable
        huckleberry_finn_edition = self._get_edition_by_identifier(
            imported_editions, self.HUCKLEBERRY_FINN_URI_IDENTIFIER
        )
        assert isinstance(huckleberry_finn_edition, Edition)

        assert huckleberry_finn_edition.title == "Adventures of Huckleberry Finn"

        huckleberry_finn_license_pool = self._get_license_pool_by_identifier(
            pools, self.HUCKLEBERRY_FINN_URI_IDENTIFIER
        )
        assert isinstance(huckleberry_finn_license_pool, LicensePool) is True
        assert huckleberry_finn_license_pool.open_access is False
        assert huckleberry_finn_license_pool.licenses_owned == 0
        assert huckleberry_finn_license_pool.licenses_available == 0

        # Politics of postmodernism is still available
        postmodernism_edition = self._get_edition_by_identifier(
            imported_editions, self.POSTMODERNISM_PROQUEST_IDENTIFIER
        )
        assert isinstance(postmodernism_edition, Edition)

        assert postmodernism_edition.title == "The Politics of Postmodernism"

        postmodernism_license_pool = self._get_license_pool_by_identifier(
            pools, self.POSTMODERNISM_PROQUEST_IDENTIFIER
        )
        assert isinstance(postmodernism_license_pool, LicensePool) is True
        assert postmodernism_license_pool.open_access is False
        assert postmodernism_license_pool.licenses_owned == LicensePool.UNLIMITED_ACCESS
        assert (
            postmodernism_license_pool.licenses_available
            == LicensePool.UNLIMITED_ACCESS
        )

    def test__extract_contributor_roles(
        self,
        opds2_importer_fixture: OPDS2ImporterFixture,
    ):
        _extract_contributor_roles = (
            opds2_importer_fixture.importer._extract_contributor_roles
        )

        # If there are no roles, the function returns the default
        assert _extract_contributor_roles([], Contributor.Role.AUTHOR) == [
            Contributor.Role.AUTHOR
        ]

        # If the role is unknown, the default is used
        assert _extract_contributor_roles(["invalid"], Contributor.Role.AUTHOR) == [
            Contributor.Role.AUTHOR
        ]

        # Roles are not duplicated
        assert _extract_contributor_roles(
            [Contributor.Role.AUTHOR, Contributor.Role.AUTHOR], Contributor.Role.AUTHOR
        ) == [Contributor.Role.AUTHOR]
        assert _extract_contributor_roles(
            ["invalid", "invalid"], Contributor.Role.AUTHOR
        ) == [Contributor.Role.AUTHOR]

        # Role lookup is not case-sensitive
        assert _extract_contributor_roles(["aUtHoR"], Contributor.Role.ILLUSTRATOR) == [
            Contributor.Role.AUTHOR
        ]

        # Roles can be looked up via marc codes
        assert _extract_contributor_roles(["AUT"], Contributor.Role.ILLUSTRATOR) == [
            Contributor.Role.AUTHOR
        ]

    def test_extract_next_links(
        self,
        opds2_importer_fixture: OPDS2ImporterFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ):
        extract_next_links = opds2_importer_fixture.importer.extract_next_links

        # Bad feed
        assert extract_next_links(b"garbage") == []

        # No next links
        assert extract_next_links(opds2_files_fixture.sample_data("feed.json")) == []

        # One next link
        assert extract_next_links(opds2_files_fixture.sample_data("feed2.json")) == [
            "http://bookshelf-feed-demo.us-east-1.elasticbeanstalk.com/v1/publications?page=2&limit=100"
        ]

    def test_extract_last_update_dates(
        self,
        opds2_importer_fixture: OPDS2ImporterFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ):
        extract_last_update_dates = (
            opds2_importer_fixture.importer.extract_last_update_dates
        )

        # Bad feed
        assert extract_last_update_dates(b"garbage") == []

        # Feed with last update dates
        expected_dates = [
            (
                "urn:isbn:978-3-16-148410-0",
                datetime.datetime(2015, 9, 29, 17, 0, tzinfo=datetime.timezone.utc),
            ),
            (
                "http://example.org/huckleberry-finn",
                datetime.datetime(2015, 9, 29, 17, 0, tzinfo=datetime.timezone.utc),
            ),
            (
                "urn:proquest.com/document-id/181639",
                datetime.datetime(2022, 9, 12, 21, 4, tzinfo=datetime.timezone.utc),
            ),
        ]

        assert (
            extract_last_update_dates(opds2_files_fixture.sample_data("feed.json"))
            == expected_dates
        )

        # Feed with bad publication - we still get dates from valid items in feed
        feed_dict = json.loads(opds2_files_fixture.sample_data("feed.json"))
        feed_dict["publications"].insert(0, {})
        assert extract_last_update_dates(json.dumps(feed_dict)) == expected_dates


class Opds2ApiFixture:
    def __init__(self, db: DatabaseTransactionFixture, mock_http: MagicMock):
        self.patron = db.patron()
        self.collection: Collection = db.collection(
            protocol=OPDS2API,
            settings=db.opds_settings(
                external_account_id="http://opds2.example.org/feed",
                data_source="test",
            ),
        )
        self.collection.integration_configuration.context = {
            OPDS2API.TOKEN_AUTH_CONFIG_KEY: "http://example.org/token?userName={patron_id}"
        }

        self.mock_response = MagicMock(spec=Response)
        self.mock_response.status_code = 200
        self.mock_response.text = "plaintext-auth-token"

        self.mock_http = mock_http
        self.mock_http.get_with_timeout.return_value = self.mock_response

        self.data_source = DataSource.lookup(db.session, "test", autocreate=True)

        self.pool = MagicMock(spec=LicensePool)
        self.mechanism = MagicMock(spec=LicensePoolDeliveryMechanism)
        self.pool.available_delivery_mechanisms = [self.mechanism]
        self.pool.data_source = self.data_source
        self.mechanism.resource.representation.public_url = (
            "http://example.org/11234/fulfill?authToken={authentication_token}"
        )

        self.api = OPDS2API(db.session, self.collection)

    def fulfill(self) -> Fulfillment:
        return self.api.fulfill(self.patron, "", self.pool, self.mechanism)


@pytest.fixture
def opds2_api_fixture(
    db: DatabaseTransactionFixture,
) -> Generator[Opds2ApiFixture, None, None]:
    with patch("palace.manager.core.opds2_import.HTTP") as mock_http:
        fixture = Opds2ApiFixture(db, mock_http)
        yield fixture


class TestOpds2Api:
    def test_opds2_with_authentication_tokens(
        self,
        db: DatabaseTransactionFixture,
        opds2_importer_fixture: OPDS2ImporterFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ):
        """Test the end to end workflow from importing the feed to a fulfill"""
        content = opds2_files_fixture.sample_text("auth_token_feed.json")
        (
            imported_editions,
            pools,
            works,
            failures,
        ) = opds2_importer_fixture.importer.import_from_feed(content)

        work = works[0]

        api = CirculationAPI(
            db.session,
            opds2_importer_fixture.library,
            {
                opds2_importer_fixture.collection.id: OPDS2API(
                    db.session, opds2_importer_fixture.collection
                )
            },
        )
        patron = db.patron()

        # Borrow the book from the library
        api.borrow(patron, "pin", work.license_pools[0], MagicMock(), None)

        loans = db.session.query(Loan).filter(Loan.patron == patron)
        assert loans.count() == 1

        loan = loans.first()
        assert isinstance(loan, Loan)

        epub_mechanism = None
        for mechanism in loan.license_pool.delivery_mechanisms:
            if mechanism.delivery_mechanism.content_type == "application/epub+zip":
                epub_mechanism = mechanism
                break

        assert epub_mechanism is not None

        # Fulfill (Download) the book, should redirect to an authenticated URL
        with patch.object(OPDS2API, "get_authentication_token") as mock_auth:
            mock_auth.return_value = "plaintext-token"
            fulfillment = api.fulfill(
                patron, "pin", work.license_pools[0], epub_mechanism
            )

        assert isinstance(fulfillment, RedirectFulfillment)
        assert (
            fulfillment.content_link
            == "http://example.org//getDrmFreeFile.action?documentId=1543720&mediaType=epub&authToken=plaintext-token"
        )
        assert fulfillment.content_type == "application/epub+zip"

    def test_token_fulfill(self, opds2_api_fixture: Opds2ApiFixture):
        fulfillment = opds2_api_fixture.fulfill()
        assert isinstance(fulfillment, RedirectFulfillment)

        patron_id = opds2_api_fixture.patron.identifier_to_remote_service(
            opds2_api_fixture.data_source
        )

        assert opds2_api_fixture.mock_http.get_with_timeout.call_count == 1
        assert (
            opds2_api_fixture.mock_http.get_with_timeout.call_args[0][0]
            == f"http://example.org/token?userName={patron_id}"
        )

        assert (
            fulfillment.content_link
            == "http://example.org/11234/fulfill?authToken=plaintext-auth-token"
        )

    def test_token_fulfill_alternate_template(self, opds2_api_fixture: Opds2ApiFixture):
        # Alternative templating
        opds2_api_fixture.mechanism.resource.representation.public_url = (
            "http://example.org/11234/fulfill{?authentication_token}"
        )
        fulfillment = opds2_api_fixture.fulfill()
        assert isinstance(fulfillment, RedirectFulfillment)

        assert (
            fulfillment.content_link
            == "http://example.org/11234/fulfill?authentication_token=plaintext-auth-token"
        )

    def test_token_fulfill_400_response(self, opds2_api_fixture: Opds2ApiFixture):
        # non-200 response
        opds2_api_fixture.mock_response.status_code = 400
        with pytest.raises(CannotFulfill):
            opds2_api_fixture.fulfill()

    def test_token_fulfill_no_template(self, opds2_api_fixture: Opds2ApiFixture):
        # No templating in the url
        opds2_api_fixture.mechanism.resource.representation.public_url = (
            "http://example.org/11234/fulfill"
        )
        fulfillment = opds2_api_fixture.fulfill()
        assert isinstance(fulfillment, RedirectFulfillment)
        assert (
            fulfillment.content_link
            == opds2_api_fixture.mechanism.resource.representation.public_url
        )

    def test_token_fulfill_no_endpoint_config(self, opds2_api_fixture: Opds2ApiFixture):
        # No token endpoint config
        opds2_api_fixture.api.token_auth_configuration = None
        mock = MagicMock()
        opds2_api_fixture.api.fulfill_token_auth = mock
        opds2_api_fixture.fulfill()
        # we never call the token auth function
        assert mock.call_count == 0

    def test_get_authentication_token(self, opds2_api_fixture: Opds2ApiFixture):
        token = OPDS2API.get_authentication_token(
            opds2_api_fixture.patron, opds2_api_fixture.data_source, ""
        )

        assert token == "plaintext-auth-token"
        assert opds2_api_fixture.mock_http.get_with_timeout.call_count == 1

    def test_get_authentication_token_400_response(
        self, opds2_api_fixture: Opds2ApiFixture
    ):
        opds2_api_fixture.mock_response.status_code = 400
        with pytest.raises(CannotFulfill):
            OPDS2API.get_authentication_token(
                opds2_api_fixture.patron, opds2_api_fixture.data_source, ""
            )

    def test_get_authentication_token_bad_response(
        self, opds2_api_fixture: Opds2ApiFixture
    ):
        opds2_api_fixture.mock_response.text = None
        with pytest.raises(CannotFulfill):
            OPDS2API.get_authentication_token(
                opds2_api_fixture.patron, opds2_api_fixture.data_source, ""
            )


class TestOPDS2ImportMonitor:
    @pytest.mark.parametrize(
        "content_type,exception",
        [
            ("application/json", False),
            ("application/opds+json", False),
            ("application/xml", True),
            ("foo/xyz", True),
        ],
    )
    def test__verify_media_type(
        self, db: DatabaseTransactionFixture, content_type: str, exception: bool
    ) -> None:
        collection = db.collection(
            protocol=OPDS2API,
            settings=db.opds_settings(
                external_account_id="http://opds2.example.org/feed",
            ),
        )
        monitor = OPDS2ImportMonitor(
            db.session,
            collection,
            OPDS2Importer,
        )

        ctx_manager = (
            nullcontext()
            if not exception
            else pytest.raises(
                BadResponseException, match="Bad response from http://test.com"
            )
        )

        mock_response = MockRequestsResponse(
            status_code=200, headers={"Content-Type": content_type}
        )
        with ctx_manager:
            monitor._verify_media_type("http://test.com", mock_response)
