from __future__ import annotations

import datetime
import json
from collections.abc import Generator
from functools import partial
from unittest.mock import MagicMock, call, patch

import pytest
from sqlalchemy import select

from palace.manager.api.circulation.dispatcher import CirculationApiDispatcher
from palace.manager.api.circulation.fulfillment import RedirectFulfillment
from palace.manager.celery.tasks import apply, identifiers, opds2
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.integration.license.opds.opds2 import (
    OPDS2API,
    OPDS2ImporterSettings,
)
from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.service.redis.models.set import IdentifierSet
from palace.manager.sqlalchemy.constants import (
    EditionConstants,
    IdentifierType,
    MediaTypes,
)
from palace.manager.sqlalchemy.model.contributor import Contribution, Contributor
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    DeliveryMechanismTuple,
    LicensePool,
)
from palace.manager.sqlalchemy.model.patron import Loan
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import OPDS2FilesFixture
from tests.fixtures.http import MockHttpClientFixture
from tests.fixtures.redis import RedisFixture


class OPDS2ImportFixture:
    MOBY_DICK_ISBN_IDENTIFIER = "urn:isbn:978-3-16-148410-0"
    HUCKLEBERRY_FINN_URI_IDENTIFIER = "http://example.org/huckleberry-finn"
    POSTMODERNISM_PROQUEST_IDENTIFIER = (
        "urn:librarysimplified.org/terms/id/ProQuest%20Doc%20ID/181639"
    )

    def __init__(
        self,
        db: DatabaseTransactionFixture,
        http_client: MockHttpClientFixture,
    ) -> None:
        self.db = db
        self.create_settings = partial(
            OPDS2ImporterSettings,
            external_account_id="http://opds2.example.org/feed",
            data_source="OPDS 2.0 Data Source",
        )
        self.collection = db.collection(
            protocol=OPDS2API,
            settings=self.create_settings(),
        )
        self.library = db.default_library()
        self.collection.associated_libraries.append(self.library)
        self.data_source = DataSource.lookup(
            db.session, "OPDS 2.0 Data Source", autocreate=True
        )
        self.collection.data_source = self.data_source
        self.client = http_client
        self.bibliographic_apply_queue: list[BibliographicData] = []

    def mock_bibliographic_apply(
        self,
        bibliographic: BibliographicData,
        collection_id: int | None = None,
    ) -> None:
        """
        Mock bibliographic apply

        This function mocks the apply.bibliographic_apply task, to avoid this
        task being executed asynchronously. We want to be able to test the full
        workflow, assuming that the task we are testing, and all the apply tasks
        run to completion.
        """
        assert (
            collection_id == self.collection.id
        ), "Collection ID mismatch in mocked apply"
        self.bibliographic_apply_queue.append(bibliographic)

    def process_bibliographic_apply_queue(self) -> list[Edition]:
        """
        Process the mocked bibliographic apply queue.

        This function does the same basic logic as the apply.bibliographic_apply task.
        Since we test that task separately, we can assume that it works correctly.
        """
        editions = []
        for bibliographic in self.bibliographic_apply_queue:
            edition, _ = bibliographic.edition(self.db.session)
            bibliographic.apply(
                self.db.session,
                edition,
                self.collection,
                disable_async_calculation=True,
                create_coverage_record=False,
            )
            editions.append(edition)
        self.bibliographic_apply_queue.clear()
        return editions

    def do_import(self) -> list[Edition]:
        opds2.import_collection.delay(self.collection.id).wait()
        return self.process_bibliographic_apply_queue()

    def get_pools(self) -> list[LicensePool]:
        """Get all license pools from the database."""
        return self.db.session.scalars(select(LicensePool)).unique().all()

    def get_works(self) -> list[Work]:
        """Get all works from the database."""
        return self.db.session.scalars(select(Work)).unique().all()

    @staticmethod
    def get_delivery_mechanisms_from_license_pool(
        license_pool: LicensePool,
    ) -> set[DeliveryMechanismTuple]:
        """
        Get a set of DeliveryMechanismTuples from a LicensePool.

        Makes it a little easier to compare delivery mechanisms
        """
        return {
            dm.delivery_mechanism.as_tuple for dm in license_pool.delivery_mechanisms
        }

    @staticmethod
    def get_edition_by_identifier(
        editions: list[Edition], identifier: str
    ) -> Edition | None:
        """
        Find an edition in the list by its identifier.
        """
        for edition in editions:
            if edition.primary_identifier.urn == identifier:
                return edition

        return None

    @staticmethod
    def get_license_pool_by_identifier(
        pools: list[LicensePool], identifier: str
    ) -> LicensePool | None:
        """
        Find a license pool in the list by its identifier.
        """
        for pool in pools:
            if pool.identifier.urn == identifier:
                return pool

        return None

    @staticmethod
    def get_work_by_identifier(works: list[Work], identifier: str) -> Work | None:
        """Find a license pool in the list by its identifier."""
        for work in works:
            if work.presentation_edition.primary_identifier.urn == identifier:
                return work

        return None


@pytest.fixture
def opds2_import_fixture(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
    http_client: MockHttpClientFixture,
) -> Generator[OPDS2ImportFixture]:
    fixture = OPDS2ImportFixture(db, http_client)
    with patch.object(apply, "bibliographic_apply", autospec=True) as mock_apply:
        mock_apply.delay.side_effect = fixture.mock_bibliographic_apply
        yield fixture


class TestImportAll:
    @pytest.mark.parametrize(
        "force",
        [
            pytest.param(True, id="Force import"),
            pytest.param(False, id="Do not force import"),
        ],
    )
    def test_import_all(
        self, db: DatabaseTransactionFixture, celery_fixture: CeleryFixture, force: bool
    ) -> None:
        collection1 = db.collection(protocol=OPDS2API)
        collection2 = db.collection(protocol=OPDS2API)
        decoy_collection = db.collection(protocol=OverdriveAPI)

        with patch.object(opds2, "import_collection") as mock_import_collection:
            opds2.import_all.delay(force=force).wait()

        # We queued up tasks for all OPDS2 collections, but not for Overdrive
        mock_import_collection.delay.assert_has_calls(
            [
                call(collection_id=collection1.id, force=force),
                call(collection_id=collection2.id, force=force),
            ],
            any_order=True,
        )


class TestImportCollection:
    def test_correctly_imports_valid_opds2_feed(
        self,
        db: DatabaseTransactionFixture,
        opds2_import_fixture: OPDS2ImportFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ):
        """Ensure that OPDS2Importer correctly imports valid OPDS 2.x feeds."""
        # Arrange
        content_server_feed_text = opds2_files_fixture.sample_text("feed.json")
        opds2_import_fixture.client.queue_response(
            200, content=content_server_feed_text
        )

        # Act
        opds2.import_collection.delay(opds2_import_fixture.collection.id).wait()
        imported_editions = opds2_import_fixture.process_bibliographic_apply_queue()

        # Assert
        # 1. Make sure that editions contain all required metadata
        assert isinstance(imported_editions, list)
        assert len(imported_editions) == 3

        # 1.1. Edition with open-access links (Moby-Dick)
        moby_dick_edition = opds2_import_fixture.get_edition_by_identifier(
            imported_editions, opds2_import_fixture.MOBY_DICK_ISBN_IDENTIFIER
        )
        assert isinstance(moby_dick_edition, Edition)

        assert moby_dick_edition.title == "Moby-Dick"
        assert moby_dick_edition.language == "eng"
        assert moby_dick_edition.medium == EditionConstants.AUDIO_MEDIUM
        assert moby_dick_edition.author == "Herman Melville"
        assert moby_dick_edition.duration == 100.2

        [moby_dick_author] = moby_dick_edition.author_contributors
        assert isinstance(moby_dick_author, Contributor)
        assert moby_dick_author.display_name == "Herman Melville"
        assert moby_dick_author.sort_name == "Melville, Herman"

        [moby_dick_author_contribution] = moby_dick_author.contributions
        assert isinstance(moby_dick_author_contribution, Contribution)
        assert moby_dick_author_contribution.contributor == moby_dick_author
        assert moby_dick_author_contribution.edition == moby_dick_edition
        assert moby_dick_author_contribution.role == Contributor.Role.AUTHOR

        assert moby_dick_edition.data_source == opds2_import_fixture.data_source

        assert moby_dick_edition.publisher == "Test Publisher"
        assert moby_dick_edition.published == datetime.date(2015, 9, 29)

        assert moby_dick_edition.cover_full_url == "http://example.org/cover.jpg"
        assert (
            moby_dick_edition.cover_thumbnail_url
            == "http://example.org/cover-small.jpg"
        )

        # 1.2. Edition with non open-access acquisition links (Adventures of Huckleberry Finn)
        huckleberry_finn_edition = opds2_import_fixture.get_edition_by_identifier(
            imported_editions, opds2_import_fixture.HUCKLEBERRY_FINN_URI_IDENTIFIER
        )
        assert isinstance(huckleberry_finn_edition, Edition)

        assert huckleberry_finn_edition.title == "Adventures of Huckleberry Finn"
        assert huckleberry_finn_edition.language == "eng"
        assert huckleberry_finn_edition.medium == EditionConstants.BOOK_MEDIUM
        assert huckleberry_finn_edition.author == "Samuel Langhorne Clemens, Mark Twain"

        assert len(huckleberry_finn_edition.author_contributors) == 2
        [mark_twain, samuel_clemens] = huckleberry_finn_edition.author_contributors

        assert isinstance(mark_twain, Contributor)
        assert mark_twain.display_name == "Mark Twain"
        assert mark_twain.sort_name == "Twain, Mark"

        assert len(mark_twain.contributions) == 1
        [huckleberry_finn_author_contribution] = mark_twain.contributions
        assert isinstance(huckleberry_finn_author_contribution, Contribution)
        assert huckleberry_finn_author_contribution.contributor == mark_twain
        assert huckleberry_finn_author_contribution.edition == huckleberry_finn_edition
        assert huckleberry_finn_author_contribution.role == Contributor.Role.AUTHOR

        assert isinstance(samuel_clemens, Contributor)
        assert samuel_clemens.display_name == "Samuel Langhorne Clemens"
        assert samuel_clemens.sort_name == "Clemens, Samuel Langhorne"

        assert len(samuel_clemens.contributions) == 1
        [huckleberry_finn_author_contribution] = samuel_clemens.contributions
        assert isinstance(huckleberry_finn_author_contribution, Contribution)
        assert huckleberry_finn_author_contribution.contributor == samuel_clemens
        assert huckleberry_finn_author_contribution.edition == huckleberry_finn_edition
        assert huckleberry_finn_author_contribution.role == Contributor.Role.AUTHOR

        assert huckleberry_finn_edition.data_source == opds2_import_fixture.data_source

        assert huckleberry_finn_edition.publisher == "Test Publisher"
        assert huckleberry_finn_edition.published == datetime.date(2014, 9, 28)

        assert moby_dick_edition.cover_full_url == "http://example.org/cover.jpg"

        # 2. Make sure that license pools have correct configuration
        pools = opds2_import_fixture.get_pools()
        assert isinstance(pools, list)
        assert len(pools) == 3

        # 2.1. Edition with open-access links (Moby-Dick)
        moby_dick_license_pool = opds2_import_fixture.get_license_pool_by_identifier(
            pools, opds2_import_fixture.MOBY_DICK_ISBN_IDENTIFIER
        )
        assert isinstance(moby_dick_license_pool, LicensePool)
        assert moby_dick_license_pool.open_access
        assert moby_dick_license_pool.licenses_owned == LicensePool.UNLIMITED_ACCESS
        assert moby_dick_license_pool.licenses_available == LicensePool.UNLIMITED_ACCESS
        assert moby_dick_license_pool.should_track_playtime == True

        assert opds2_import_fixture.get_delivery_mechanisms_from_license_pool(
            moby_dick_license_pool
        ) == {(MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE, DeliveryMechanism.NO_DRM)}

        # 2.2. Edition with non open-access acquisition links (Adventures of Huckleberry Finn)
        huckleberry_finn_license_pool = (
            opds2_import_fixture.get_license_pool_by_identifier(
                pools, opds2_import_fixture.HUCKLEBERRY_FINN_URI_IDENTIFIER
            )
        )
        assert isinstance(huckleberry_finn_license_pool, LicensePool)
        assert huckleberry_finn_license_pool.open_access is False
        assert (
            huckleberry_finn_license_pool.licenses_owned == LicensePool.UNLIMITED_ACCESS
        )
        assert (
            huckleberry_finn_license_pool.licenses_available
            == LicensePool.UNLIMITED_ACCESS
        )
        assert huckleberry_finn_license_pool.should_track_playtime is False

        assert opds2_import_fixture.get_delivery_mechanisms_from_license_pool(
            huckleberry_finn_license_pool
        ) == {
            (MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
            (MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.LCP_DRM),
        }

        # 2.3 Edition with non open-access acquisition links (The Politics of Postmodernism)
        postmodernism_license_pool = (
            opds2_import_fixture.get_license_pool_by_identifier(
                pools, opds2_import_fixture.POSTMODERNISM_PROQUEST_IDENTIFIER
            )
        )
        assert isinstance(postmodernism_license_pool, LicensePool)
        assert postmodernism_license_pool.open_access is False
        assert postmodernism_license_pool.licenses_owned == LicensePool.UNLIMITED_ACCESS
        assert (
            postmodernism_license_pool.licenses_available
            == LicensePool.UNLIMITED_ACCESS
        )

        assert opds2_import_fixture.get_delivery_mechanisms_from_license_pool(
            postmodernism_license_pool
        ) == {
            (MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
            (MediaTypes.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
        }

        # 3. Make sure that work objects contain all the required metadata
        works = opds2_import_fixture.get_works()
        assert isinstance(works, list)
        assert len(works) == 3

        # 3.1. Work (Moby-Dick)
        moby_dick_work = opds2_import_fixture.get_work_by_identifier(
            works, opds2_import_fixture.MOBY_DICK_ISBN_IDENTIFIER
        )
        assert isinstance(moby_dick_work, Work)
        assert moby_dick_work.presentation_edition == moby_dick_edition
        assert len(moby_dick_work.license_pools) == 1
        assert moby_dick_work.license_pools[0] == moby_dick_license_pool

        # 3.2. Work (Adventures of Huckleberry Finn)
        huckleberry_finn_work = opds2_import_fixture.get_work_by_identifier(
            works, opds2_import_fixture.HUCKLEBERRY_FINN_URI_IDENTIFIER
        )
        assert isinstance(huckleberry_finn_work, Work)
        assert huckleberry_finn_work.presentation_edition == huckleberry_finn_edition
        assert len(huckleberry_finn_work.license_pools) == 1
        assert huckleberry_finn_work.license_pools[0] == huckleberry_finn_license_pool
        assert (
            huckleberry_finn_work.summary_text
            == "Adventures of Huckleberry Finn is a novel by Mark Twain, first published in the United Kingdom in "
            "December 1884 and in the United States in February 1885."
        )

    @pytest.mark.parametrize(
        "this_identifier_type,ignore_identifier_type,identifier",
        [
            pytest.param(
                IdentifierType.ISBN,
                [IdentifierType.URI, IdentifierType.PROQUEST_ID],
                OPDS2ImportFixture.MOBY_DICK_ISBN_IDENTIFIER,
                id="Ignore URI & ProQuest ID",
            ),
            pytest.param(
                IdentifierType.URI,
                [IdentifierType.ISBN, IdentifierType.PROQUEST_ID],
                OPDS2ImportFixture.HUCKLEBERRY_FINN_URI_IDENTIFIER,
                id="Ignore ISBN & ProQuest ID",
            ),
            pytest.param(
                IdentifierType.PROQUEST_ID,
                [IdentifierType.ISBN, IdentifierType.URI],
                OPDS2ImportFixture.POSTMODERNISM_PROQUEST_IDENTIFIER,
                id="Ignore ISBN & URI",
            ),
        ],
    )
    def test_skips_publications_with_unsupported_identifier_types(
        self,
        db: DatabaseTransactionFixture,
        opds2_import_fixture: OPDS2ImportFixture,
        opds2_files_fixture: OPDS2FilesFixture,
        this_identifier_type: IdentifierType,
        ignore_identifier_type: list[IdentifierType],
        identifier: str,
    ) -> None:
        """Ensure that we import only publications having supported identifier types.
        This test imports the feed consisting of two publications,
        each having a different identifier type: ISBN and URI.
        First, it tries to import the feed marking ISBN as the only supported identifier type. Secondly, it uses URI.
        Each time it checks that CM imported only the publication having the selected identifier type.
        """
        # Arrange
        # Update the list of supported identifier types in the collection's configuration settings
        # and set the identifier type passed as a parameter as the only supported identifier type.
        OPDS2API.settings_update(
            opds2_import_fixture.collection.integration_configuration,
            opds2_import_fixture.create_settings(
                ignored_identifier_types=ignore_identifier_type,
            ),
        )

        content_server_feed_text = opds2_files_fixture.sample_text("feed.json")
        opds2_import_fixture.client.queue_response(
            200, content=content_server_feed_text
        )

        # Act
        imported_editions = opds2_import_fixture.do_import()

        # Assert

        # Ensure that that CM imported only the edition having the selected identifier type.
        assert isinstance(imported_editions, list)
        assert len(imported_editions) == 1
        assert (
            imported_editions[0].primary_identifier.type == this_identifier_type.value
        )

        # Ensure that it was parsed correctly and available by its identifier.
        edition = opds2_import_fixture.get_edition_by_identifier(
            imported_editions, identifier
        )
        assert edition is not None

    def test_auth_token_feed(
        self,
        opds2_import_fixture: OPDS2ImportFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ):
        content = opds2_files_fixture.sample_text("auth_token_feed.json")
        opds2_import_fixture.client.queue_response(200, content=content)
        opds2_import_fixture.do_import()

        # Act
        token_endpoint = (
            opds2_import_fixture.collection.integration_configuration.context.get(
                OPDS2API.TOKEN_AUTH_CONFIG_KEY
            )
        )

        # Did the token endpoint get stored correctly?
        assert token_endpoint == "http://example.org/auth?userName={patron_id}"

        # Do a second import.
        content = opds2_files_fixture.sample_text("auth_token_feed.json")
        opds2_import_fixture.client.queue_response(200, content=content)
        opds2_import_fixture.do_import()

    def test_imports_feeds_with_availability_info(
        self,
        opds2_import_fixture: OPDS2ImportFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ):
        """Ensure that OPDS2Importer correctly imports feeds with availability information."""
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

        opds2_import_fixture.client.queue_response(200, content=feed_json)
        imported_editions = opds2_import_fixture.do_import()
        pools = opds2_import_fixture.get_pools()

        # Make we have the correct number of editions
        assert isinstance(imported_editions, list)
        assert len(imported_editions) == 3

        # Make we have the correct number of license pools
        assert isinstance(pools, list)
        assert len(pools) == 3

        # Moby dick should be imported but is unavailable
        moby_dick_edition = opds2_import_fixture.get_edition_by_identifier(
            imported_editions, opds2_import_fixture.MOBY_DICK_ISBN_IDENTIFIER
        )
        assert isinstance(moby_dick_edition, Edition)
        assert moby_dick_edition.title == "Moby-Dick"

        moby_dick_license_pool = opds2_import_fixture.get_license_pool_by_identifier(
            pools, opds2_import_fixture.MOBY_DICK_ISBN_IDENTIFIER
        )
        assert isinstance(moby_dick_license_pool, LicensePool)
        assert moby_dick_license_pool.open_access
        assert moby_dick_license_pool.licenses_owned == 0
        assert moby_dick_license_pool.licenses_available == 0

        # Adventures of Huckleberry Finn is imported and is available
        huckleberry_finn_edition = opds2_import_fixture.get_edition_by_identifier(
            imported_editions, opds2_import_fixture.HUCKLEBERRY_FINN_URI_IDENTIFIER
        )
        assert isinstance(huckleberry_finn_edition, Edition)
        assert huckleberry_finn_edition.title == "Adventures of Huckleberry Finn"

        huckleberry_finn_license_pool = (
            opds2_import_fixture.get_license_pool_by_identifier(
                pools, opds2_import_fixture.HUCKLEBERRY_FINN_URI_IDENTIFIER
            )
        )
        assert isinstance(huckleberry_finn_license_pool, LicensePool)
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
        postmodernism_edition = opds2_import_fixture.get_edition_by_identifier(
            imported_editions, opds2_import_fixture.POSTMODERNISM_PROQUEST_IDENTIFIER
        )
        assert isinstance(postmodernism_edition, Edition)
        assert postmodernism_edition.title == "The Politics of Postmodernism"

        postmodernism_license_pool = (
            opds2_import_fixture.get_license_pool_by_identifier(
                pools, opds2_import_fixture.POSTMODERNISM_PROQUEST_IDENTIFIER
            )
        )
        assert isinstance(postmodernism_license_pool, LicensePool)
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

        opds2_import_fixture.client.queue_response(200, content=feed_json)
        imported_editions = opds2_import_fixture.do_import()
        pools = opds2_import_fixture.get_pools()

        # Make we have the correct number of editions
        assert isinstance(imported_editions, list)
        assert len(imported_editions) == 3

        # Make we have the correct number of license pools
        assert isinstance(pools, list)
        assert len(pools) == 3

        # Moby dick should be imported and is now available
        moby_dick_edition = opds2_import_fixture.get_edition_by_identifier(
            imported_editions, opds2_import_fixture.MOBY_DICK_ISBN_IDENTIFIER
        )
        assert isinstance(moby_dick_edition, Edition)
        assert moby_dick_edition.title == "Moby-Dick"

        moby_dick_license_pool = opds2_import_fixture.get_license_pool_by_identifier(
            pools, opds2_import_fixture.MOBY_DICK_ISBN_IDENTIFIER
        )
        assert isinstance(moby_dick_license_pool, LicensePool)
        assert moby_dick_license_pool.open_access
        assert moby_dick_license_pool.licenses_owned == LicensePool.UNLIMITED_ACCESS
        assert moby_dick_license_pool.licenses_available == LicensePool.UNLIMITED_ACCESS

        # Adventures of Huckleberry Finn is imported and is now unavailable
        huckleberry_finn_edition = opds2_import_fixture.get_edition_by_identifier(
            imported_editions, opds2_import_fixture.HUCKLEBERRY_FINN_URI_IDENTIFIER
        )
        assert isinstance(huckleberry_finn_edition, Edition)
        assert huckleberry_finn_edition.title == "Adventures of Huckleberry Finn"

        huckleberry_finn_license_pool = (
            opds2_import_fixture.get_license_pool_by_identifier(
                pools, opds2_import_fixture.HUCKLEBERRY_FINN_URI_IDENTIFIER
            )
        )
        assert isinstance(huckleberry_finn_license_pool, LicensePool) is True
        assert huckleberry_finn_license_pool.open_access is False
        assert huckleberry_finn_license_pool.licenses_owned == 0
        assert huckleberry_finn_license_pool.licenses_available == 0

        # Politics of postmodernism is still available
        postmodernism_edition = opds2_import_fixture.get_edition_by_identifier(
            imported_editions, opds2_import_fixture.POSTMODERNISM_PROQUEST_IDENTIFIER
        )
        assert isinstance(postmodernism_edition, Edition)
        assert postmodernism_edition.title == "The Politics of Postmodernism"

        postmodernism_license_pool = (
            opds2_import_fixture.get_license_pool_by_identifier(
                pools, opds2_import_fixture.POSTMODERNISM_PROQUEST_IDENTIFIER
            )
        )
        assert isinstance(postmodernism_license_pool, LicensePool) is True
        assert postmodernism_license_pool.open_access is False
        assert postmodernism_license_pool.licenses_owned == LicensePool.UNLIMITED_ACCESS
        assert (
            postmodernism_license_pool.licenses_available
            == LicensePool.UNLIMITED_ACCESS
        )

    def test_auth_token_import_to_fulfillment(
        self,
        db: DatabaseTransactionFixture,
        opds2_import_fixture: OPDS2ImportFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ):
        """Test the end to end workflow from importing the feed to a fulfillment"""

        content = opds2_files_fixture.sample_text("auth_token_feed.json")
        opds2_import_fixture.client.queue_response(200, content=content)
        opds2_import_fixture.do_import()

        work = opds2_import_fixture.get_work_by_identifier(
            opds2_import_fixture.get_works(),
            "urn:librarysimplified.org/terms/id/ProQuest%20Doc%20ID/1543720",
        )

        api = CirculationApiDispatcher(
            db.session,
            opds2_import_fixture.library,
            {
                opds2_import_fixture.collection.id: OPDS2API(
                    db.session, opds2_import_fixture.collection
                )
            },
        )
        patron = db.patron()

        # Borrow the book from the library
        api.borrow(patron, "pin", work.license_pools[0], MagicMock(), None)

        loans = db.session.scalars(select(Loan).where(Loan.patron == patron)).all()
        assert len(loans) == 1
        [loan] = loans

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

    def test_return_identifiers(
        self,
        db: DatabaseTransactionFixture,
        opds2_import_fixture: OPDS2ImportFixture,
        opds2_files_fixture: OPDS2FilesFixture,
        redis_fixture: RedisFixture,
    ):
        redis_client = redis_fixture.client

        # Arrange
        content_server_feed_text = opds2_files_fixture.sample_text("feed.json")
        opds2_import_fixture.client.queue_response(
            200, content=content_server_feed_text
        )

        # Act
        identifier_set_dict = opds2.import_collection.delay(
            opds2_import_fixture.collection.id, return_identifiers=True
        ).wait()
        assert identifier_set_dict is not None
        identifier_set = IdentifierSet(redis_client, **identifier_set_dict)
        assert identifier_set.get() == {
            IdentifierData.parse_urn(opds2_import_fixture.MOBY_DICK_ISBN_IDENTIFIER),
            IdentifierData.parse_urn(
                opds2_import_fixture.HUCKLEBERRY_FINN_URI_IDENTIFIER
            ),
            IdentifierData.parse_urn(
                opds2_import_fixture.POSTMODERNISM_PROQUEST_IDENTIFIER
            ),
        }

    def test_dont_import_already_imported_identifiers(
        self,
        db: DatabaseTransactionFixture,
        opds2_import_fixture: OPDS2ImportFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ):
        # Import feed
        content_server_feed_text = opds2_files_fixture.sample_text("feed.json")
        opds2_import_fixture.client.queue_response(
            200, content=content_server_feed_text
        )
        opds2.import_collection.delay(opds2_import_fixture.collection.id).wait()

        # There are tasks queued up for each identifier
        assert len(opds2_import_fixture.bibliographic_apply_queue) == 3
        opds2_import_fixture.process_bibliographic_apply_queue()

        # Import feed again
        content_server_feed_text = opds2_files_fixture.sample_text("feed.json")
        opds2_import_fixture.client.queue_response(
            200, content=content_server_feed_text
        )
        opds2.import_collection.delay(opds2_import_fixture.collection.id).wait()

        # This time there should be no tasks queued up for identifiers because
        # they were already imported
        assert len(opds2_import_fixture.bibliographic_apply_queue) == 0

        # Unless we force the import, then we should have tasks queued up
        content_server_feed_text = opds2_files_fixture.sample_text("feed.json")
        opds2_import_fixture.client.queue_response(
            200, content=content_server_feed_text
        )
        opds2.import_collection.delay(
            opds2_import_fixture.collection.id, force=True
        ).wait()
        assert len(opds2_import_fixture.bibliographic_apply_queue) == 3

    def test_import_multiple_pages(
        self,
        db: DatabaseTransactionFixture,
        opds2_import_fixture: OPDS2ImportFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ):
        # If a feed has a next link, we will import it, and then requeue to import the next page.
        # This will continue until there are no more pages.
        # feed2 has a next link, and feed has no next link. So we will import feed2 first,
        # then feed, and then stop because there are no more pages.
        opds2_import_fixture.client.queue_response(
            200, content=opds2_files_fixture.sample_text("feed2.json")
        )
        opds2_import_fixture.client.queue_response(
            200, content=opds2_files_fixture.sample_text("feed.json")
        )
        opds2.import_collection.delay(opds2_import_fixture.collection.id).wait()

        # There are tasks queued up for each identifier, 3 from each feed.
        # So we should have 6 tasks in total.
        assert len(opds2_import_fixture.bibliographic_apply_queue) == 6
        opds2_import_fixture.process_bibliographic_apply_queue()

    def test_import_wrong_collection(
        self,
        db: DatabaseTransactionFixture,
        opds2_import_fixture: OPDS2ImportFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ):
        collection = db.collection(protocol=OverdriveAPI)
        with pytest.raises(PalaceValueError, match="is not a OPDS2 collection"):
            opds2.import_collection.delay(collection.id).wait()


class TestImportAndReapNotFoundChord:
    @pytest.mark.parametrize(
        "force",
        [
            pytest.param(True, id="Force reaping"),
            pytest.param(False, id="Do not force reaping"),
        ],
    )
    def test_import_and_reap_not_found_chord(self, force: bool) -> None:
        """Test the import and reap not found chord."""
        # Reap the collection
        collection_id = 12  # Example collection ID
        with (
            patch.object(identifiers, "create_mark_unavailable_chord") as mock_chord,
            patch.object(opds2, "import_collection") as mock_import,
        ):
            opds2.import_and_reap_not_found_chord(
                collection_id=collection_id, force=force
            )

        mock_import.s.assert_called_once_with(
            collection_id=collection_id, force=force, return_identifiers=True
        )
        mock_chord.assert_called_once_with(collection_id, mock_import.s.return_value)
