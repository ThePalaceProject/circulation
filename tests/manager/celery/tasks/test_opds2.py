from __future__ import annotations

import datetime
import json
from functools import partial
from unittest.mock import MagicMock, call, patch

import pytest
from freezegun import freeze_time
from sqlalchemy import select

from palace.manager.api.circulation.dispatcher import CirculationApiDispatcher
from palace.manager.api.circulation.fulfillment import RedirectFulfillment
from palace.manager.celery.tasks import identifiers, opds2
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.integration.license.opds.opds2.api import OPDS2API
from palace.manager.integration.license.opds.opds2.settings import OPDS2ImporterSettings
from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.service.logging.configuration import LogLevel
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
    LicensePool,
)
from palace.manager.sqlalchemy.model.patron import Loan
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.celery import ApplyTaskFixture, CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import OPDS2FilesFixture, OPDS2WithODLFilesFixture
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
        apply_fixture: ApplyTaskFixture,
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
        self.client = http_client
        self.apply = apply_fixture

    def do_import(self) -> list[Edition]:
        opds2.import_collection.delay(self.collection.id).wait()
        self.apply.process_apply_queue()
        return self.apply.get_editions()


@pytest.fixture
def opds2_import_fixture(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
    http_client: MockHttpClientFixture,
    apply_task_fixture: ApplyTaskFixture,
) -> OPDS2ImportFixture:
    return OPDS2ImportFixture(db, http_client, apply_task_fixture)


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

    def test_import_all_with_reap_schedule_not_due(
        self, db: DatabaseTransactionFixture, celery_fixture: CeleryFixture
    ) -> None:
        """Test that import_all queues normal import when reap is not due."""
        collection = db.collection(
            protocol=OPDS2API,
            settings=db.opds2_settings(
                reap_schedule="0 0 * * 1",  # Midnight every Monday
            ),
        )

        # Set last_reap_time to 1 minute ago (so next reap is not due yet)
        one_minute_ago = utc_now() - datetime.timedelta(minutes=1)
        collection.integration_configuration.context_update(
            {OPDS2API.LAST_REAP_TIME_KEY: one_minute_ago.isoformat()}
        )

        with patch.object(opds2, "import_collection") as mock_import_collection:
            opds2.import_all.delay(force=False).wait()

        # Should queue normal import
        mock_import_collection.delay.assert_called_once_with(
            collection_id=collection.id, force=False
        )

    def test_import_all_with_reap_schedule_due(
        self, db: DatabaseTransactionFixture, celery_fixture: CeleryFixture
    ) -> None:
        """Test that import_all queues reap task when scheduled time has passed."""
        collection = db.collection(
            protocol=OPDS2API,
            settings=db.opds2_settings(
                reap_schedule="0 0 * * *",  # Daily at midnight
            ),
        )

        # Set last_reap_time to 2 days ago (so reap is due)
        two_days_ago = utc_now() - datetime.timedelta(days=2)
        collection.integration_configuration.context_update(
            {OPDS2API.LAST_REAP_TIME_KEY: two_days_ago.isoformat()}
        )

        with patch.object(opds2, "import_and_reap_not_found_chord") as mock_reap_chord:
            opds2.import_all.delay(force=False).wait()

        # Should queue reap task
        mock_reap_chord.assert_called_once_with(collection.id, False)
        mock_reap_chord.return_value.delay.assert_called_once()

        # Note: last_reap_time is updated by a callback after the reap chord completes.
        # Since we're mocking the chord, the callback doesn't run in this test.

    def test_import_all_with_reap_schedule_never_reaped(
        self, db: DatabaseTransactionFixture, celery_fixture: CeleryFixture
    ) -> None:
        """Test that import_all queues reap task when never reaped before."""
        collection = db.collection(
            protocol=OPDS2API,
            settings=db.opds2_settings(
                reap_schedule="0 0 * * 1",  # Midnight every Monday
            ),
        )

        # No last_reap_time set
        assert (
            collection.integration_configuration.context.get(
                OPDS2API.LAST_REAP_TIME_KEY
            )
            is None
        )

        with patch.object(opds2, "import_and_reap_not_found_chord") as mock_reap_chord:
            opds2.import_all.delay(force=False).wait()

        # Should queue reap task since never reaped
        mock_reap_chord.assert_called_once_with(collection.id, False)
        mock_reap_chord.return_value.delay.assert_called_once()

        # Note: last_reap_time is updated by a callback after the reap chord completes.
        # Since we're mocking the chord, the callback doesn't run in this test.

    def test_import_all_mixed_collections(
        self, db: DatabaseTransactionFixture, celery_fixture: CeleryFixture
    ) -> None:
        """Test import_all with mix of reaping and non-reaping collections."""
        # Collection with no reap schedule
        collection1 = db.collection(
            protocol=OPDS2API,
            settings=db.opds2_settings(),
        )

        # Collection with reap schedule that's due
        collection2 = db.collection(
            protocol=OPDS2API,
            settings=db.opds2_settings(
                reap_schedule="0 0 * * *",  # Daily
            ),
        )
        two_days_ago = utc_now() - datetime.timedelta(days=2)
        collection2.integration_configuration.context_update(
            {OPDS2API.LAST_REAP_TIME_KEY: two_days_ago.isoformat()}
        )

        with (
            patch.object(opds2, "import_collection") as mock_import,
            patch.object(opds2, "import_and_reap_not_found_chord") as mock_reap,
        ):
            opds2.import_all.delay(force=False).wait()

        # Collection1 should get normal import
        mock_import.delay.assert_called_once_with(
            collection_id=collection1.id, force=False
        )

        # Collection2 should get reap
        mock_reap.assert_called_once_with(collection2.id, False)
        mock_reap.return_value.delay.assert_called_once()


class TestUpdateLastReapTime:
    def test_update_last_reap_time_callback(
        self, db: DatabaseTransactionFixture, celery_fixture: CeleryFixture
    ) -> None:
        """Test that update_last_reap_time task updates the context correctly."""
        collection = db.collection(
            protocol=OPDS2API,
            settings=db.opds2_settings(
                reap_schedule="0 0 * * *",  # Daily
            ),
        )

        # No last_reap_time initially
        assert (
            collection.integration_configuration.context.get(
                OPDS2API.LAST_REAP_TIME_KEY
            )
            is None
        )

        # Call the callback task directly with True to indicate success
        update_time = utc_now()
        with freeze_time(update_time):
            opds2.update_last_reap_time.delay(True, collection_id=collection.id).wait()

        # Verify last_reap_time was updated
        db.session.refresh(collection)
        updated_time_str = collection.integration_configuration.context.get(
            OPDS2API.LAST_REAP_TIME_KEY
        )
        assert updated_time_str is not None

        updated_time = datetime.datetime.fromisoformat(updated_time_str)
        assert updated_time == update_time

    def test_update_last_reap_time_callback_failure(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test that update_last_reap_time does not update context when result is False."""
        caplog.set_level(LogLevel.warning)
        collection = db.collection(
            protocol=OPDS2API,
            settings=db.opds2_settings(
                reap_schedule="0 0 * * *",  # Daily
            ),
        )

        # Set an initial last_reap_time
        initial_time = utc_now() - datetime.timedelta(days=7)
        collection.integration_configuration.context_update(
            {OPDS2API.LAST_REAP_TIME_KEY: initial_time.isoformat()}
        )

        # Call the callback task with False to indicate failure
        opds2.update_last_reap_time.delay(False, collection_id=collection.id).wait()

        # Verify last_reap_time was NOT updated
        db.session.refresh(collection)
        updated_time_str = collection.integration_configuration.context.get(
            OPDS2API.LAST_REAP_TIME_KEY
        )
        assert updated_time_str == initial_time.isoformat()

        # Verify warning was logged
        assert "did not complete successfully" in caplog.text
        assert "last_reap_time not updated" in caplog.text
        assert f"id={collection.id}" in caplog.text


class TestImportCollection:
    def test_correctly_imports_valid_opds2_feed(
        self,
        db: DatabaseTransactionFixture,
        apply_task_fixture: ApplyTaskFixture,
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
        imported_editions = opds2_import_fixture.do_import()

        # Assert
        # 1. Make sure that editions contain all required metadata
        assert isinstance(imported_editions, list)
        assert len(imported_editions) == 3

        # 1.1. Edition with open-access links (Moby-Dick)
        moby_dick_edition = apply_task_fixture.get_edition_by_identifier(
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
        huckleberry_finn_edition = apply_task_fixture.get_edition_by_identifier(
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
        pools = apply_task_fixture.get_pools()
        assert isinstance(pools, list)
        assert len(pools) == 3

        # 2.1. Edition with open-access links (Moby-Dick)
        moby_dick_license_pool = apply_task_fixture.get_license_pool_by_identifier(
            pools, opds2_import_fixture.MOBY_DICK_ISBN_IDENTIFIER
        )
        assert isinstance(moby_dick_license_pool, LicensePool)
        assert moby_dick_license_pool.open_access
        assert moby_dick_license_pool.licenses_owned == LicensePool.UNLIMITED_ACCESS
        assert moby_dick_license_pool.licenses_available == LicensePool.UNLIMITED_ACCESS
        assert moby_dick_license_pool.should_track_playtime == True

        assert apply_task_fixture.get_delivery_mechanisms_from_license_pool(
            moby_dick_license_pool
        ) == {(MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE, DeliveryMechanism.NO_DRM)}

        # 2.2. Edition with non open-access acquisition links (Adventures of Huckleberry Finn)
        huckleberry_finn_license_pool = (
            apply_task_fixture.get_license_pool_by_identifier(
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

        assert apply_task_fixture.get_delivery_mechanisms_from_license_pool(
            huckleberry_finn_license_pool
        ) == {
            (MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
            (MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.LCP_DRM),
        }

        # 2.3 Edition with non open-access acquisition links (The Politics of Postmodernism)
        postmodernism_license_pool = apply_task_fixture.get_license_pool_by_identifier(
            pools, opds2_import_fixture.POSTMODERNISM_PROQUEST_IDENTIFIER
        )
        assert isinstance(postmodernism_license_pool, LicensePool)
        assert postmodernism_license_pool.open_access is False
        assert postmodernism_license_pool.licenses_owned == LicensePool.UNLIMITED_ACCESS
        assert (
            postmodernism_license_pool.licenses_available
            == LicensePool.UNLIMITED_ACCESS
        )

        assert apply_task_fixture.get_delivery_mechanisms_from_license_pool(
            postmodernism_license_pool
        ) == {
            (MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
            (MediaTypes.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
        }

        # 3. Make sure that work objects contain all the required metadata
        works = apply_task_fixture.get_works()
        assert isinstance(works, list)
        assert len(works) == 3

        # 3.1. Work (Moby-Dick)
        moby_dick_work = apply_task_fixture.get_work_by_identifier(
            works, opds2_import_fixture.MOBY_DICK_ISBN_IDENTIFIER
        )
        assert isinstance(moby_dick_work, Work)
        assert moby_dick_work.presentation_edition == moby_dick_edition
        assert len(moby_dick_work.license_pools) == 1
        assert moby_dick_work.license_pools[0] == moby_dick_license_pool

        # 3.2. Work (Adventures of Huckleberry Finn)
        huckleberry_finn_work = apply_task_fixture.get_work_by_identifier(
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
        apply_task_fixture: ApplyTaskFixture,
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
        edition = apply_task_fixture.get_edition_by_identifier(
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
        apply_task_fixture: ApplyTaskFixture,
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
        pools = apply_task_fixture.get_pools()

        # Make we have the correct number of editions
        assert isinstance(imported_editions, list)
        assert len(imported_editions) == 3

        # Make we have the correct number of license pools
        assert isinstance(pools, list)
        assert len(pools) == 3

        # Moby dick should be imported but is unavailable
        moby_dick_edition = apply_task_fixture.get_edition_by_identifier(
            imported_editions, opds2_import_fixture.MOBY_DICK_ISBN_IDENTIFIER
        )
        assert isinstance(moby_dick_edition, Edition)
        assert moby_dick_edition.title == "Moby-Dick"

        moby_dick_license_pool = apply_task_fixture.get_license_pool_by_identifier(
            pools, opds2_import_fixture.MOBY_DICK_ISBN_IDENTIFIER
        )
        assert isinstance(moby_dick_license_pool, LicensePool)
        assert moby_dick_license_pool.open_access
        assert moby_dick_license_pool.licenses_owned == 0
        assert moby_dick_license_pool.licenses_available == 0

        # Adventures of Huckleberry Finn is imported and is available
        huckleberry_finn_edition = apply_task_fixture.get_edition_by_identifier(
            imported_editions, opds2_import_fixture.HUCKLEBERRY_FINN_URI_IDENTIFIER
        )
        assert isinstance(huckleberry_finn_edition, Edition)
        assert huckleberry_finn_edition.title == "Adventures of Huckleberry Finn"

        huckleberry_finn_license_pool = (
            apply_task_fixture.get_license_pool_by_identifier(
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
        postmodernism_edition = apply_task_fixture.get_edition_by_identifier(
            imported_editions, opds2_import_fixture.POSTMODERNISM_PROQUEST_IDENTIFIER
        )
        assert isinstance(postmodernism_edition, Edition)
        assert postmodernism_edition.title == "The Politics of Postmodernism"

        postmodernism_license_pool = apply_task_fixture.get_license_pool_by_identifier(
            pools, opds2_import_fixture.POSTMODERNISM_PROQUEST_IDENTIFIER
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
        pools = apply_task_fixture.get_pools()

        # Make we have the correct number of editions
        assert isinstance(imported_editions, list)
        assert len(imported_editions) == 3

        # Make we have the correct number of license pools
        assert isinstance(pools, list)
        assert len(pools) == 3

        # Moby dick should be imported and is now available
        moby_dick_edition = apply_task_fixture.get_edition_by_identifier(
            imported_editions, opds2_import_fixture.MOBY_DICK_ISBN_IDENTIFIER
        )
        assert isinstance(moby_dick_edition, Edition)
        assert moby_dick_edition.title == "Moby-Dick"

        moby_dick_license_pool = apply_task_fixture.get_license_pool_by_identifier(
            pools, opds2_import_fixture.MOBY_DICK_ISBN_IDENTIFIER
        )
        assert isinstance(moby_dick_license_pool, LicensePool)
        assert moby_dick_license_pool.open_access
        assert moby_dick_license_pool.licenses_owned == LicensePool.UNLIMITED_ACCESS
        assert moby_dick_license_pool.licenses_available == LicensePool.UNLIMITED_ACCESS

        # Adventures of Huckleberry Finn is imported and is now unavailable
        huckleberry_finn_edition = apply_task_fixture.get_edition_by_identifier(
            imported_editions, opds2_import_fixture.HUCKLEBERRY_FINN_URI_IDENTIFIER
        )
        assert isinstance(huckleberry_finn_edition, Edition)
        assert huckleberry_finn_edition.title == "Adventures of Huckleberry Finn"

        huckleberry_finn_license_pool = (
            apply_task_fixture.get_license_pool_by_identifier(
                pools, opds2_import_fixture.HUCKLEBERRY_FINN_URI_IDENTIFIER
            )
        )
        assert isinstance(huckleberry_finn_license_pool, LicensePool) is True
        assert huckleberry_finn_license_pool.open_access is False
        assert huckleberry_finn_license_pool.licenses_owned == 0
        assert huckleberry_finn_license_pool.licenses_available == 0

        # Politics of postmodernism is still available
        postmodernism_edition = apply_task_fixture.get_edition_by_identifier(
            imported_editions, opds2_import_fixture.POSTMODERNISM_PROQUEST_IDENTIFIER
        )
        assert isinstance(postmodernism_edition, Edition)
        assert postmodernism_edition.title == "The Politics of Postmodernism"

        postmodernism_license_pool = apply_task_fixture.get_license_pool_by_identifier(
            pools, opds2_import_fixture.POSTMODERNISM_PROQUEST_IDENTIFIER
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
        apply_task_fixture: ApplyTaskFixture,
        opds2_import_fixture: OPDS2ImportFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ):
        """Test the end to end workflow from importing the feed to a fulfillment"""

        content = opds2_files_fixture.sample_text("auth_token_feed.json")
        opds2_import_fixture.client.queue_response(200, content=content)
        opds2_import_fixture.do_import()

        work = apply_task_fixture.get_work_by_identifier(
            apply_task_fixture.get_works(),
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
        apply_task_fixture: ApplyTaskFixture,
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
        assert len(apply_task_fixture.apply_queue) == 3
        apply_task_fixture.process_apply_queue()

        # Import feed again
        opds2_import_fixture.client.queue_response(
            200, content=content_server_feed_text
        )
        opds2.import_collection.delay(opds2_import_fixture.collection.id).wait()

        # This time there should be no tasks queued up for identifiers because
        # they were already imported
        assert len(apply_task_fixture.apply_queue) == 0

        # Unless we force the import, then we should have tasks queued up
        content_server_feed_text = opds2_files_fixture.sample_text("feed.json")
        opds2_import_fixture.client.queue_response(
            200, content=content_server_feed_text
        )
        opds2.import_collection.delay(
            opds2_import_fixture.collection.id, force=True
        ).wait()
        assert len(apply_task_fixture.apply_queue) == 3

    def test_import_multiple_pages(
        self,
        db: DatabaseTransactionFixture,
        apply_task_fixture: ApplyTaskFixture,
        opds2_import_fixture: OPDS2ImportFixture,
        opds2_files_fixture: OPDS2FilesFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        """
        Test multi-page feed imports with different scenarios:
        1. Initial import successfully processes all pages
        2. Re-import without force stops at first unchanged publication
        3. Re-import with force continues through all pages despite unchanged publications
        """
        caplog.set_level(LogLevel.info)

        # First import: Import both pages successfully
        # feed2 has a next link to feed, which has no next link.
        # So we import feed2 first, then feed, then stop.
        opds2_import_fixture.client.queue_response(
            200, content=opds2_files_fixture.sample_text("feed2.json")
        )
        opds2_import_fixture.client.queue_response(
            200, content=opds2_files_fixture.sample_text("feed.json")
        )
        opds2.import_collection.delay(opds2_import_fixture.collection.id).wait()
        apply_task_fixture.process_apply_queue()

        # There are tasks queued up for each identifier, 3 from each feed (6 total)
        assert len(apply_task_fixture.get_editions()) == 6
        apply_task_fixture.apply_queue.clear()

        # Second import without force: Should stop when finding unchanged publications
        caplog.clear()
        opds2_import_fixture.client.queue_response(
            200, content=opds2_files_fixture.sample_text("feed2.json")
        )
        # Note: We don't queue feed.json because import should stop at feed2.json
        # when it finds unchanged publications
        opds2.import_collection.delay(opds2_import_fixture.collection.id).wait()

        # Should see the log message about stopping due to unchanged publications
        assert "Found unchanged publications in feed" in caplog.text
        # Should have no new tasks queued since all publications were unchanged
        assert len(apply_task_fixture.apply_queue) == 0

        # Third import with force: Should continue through all pages despite unchanged publications
        caplog.clear()
        opds2_import_fixture.client.queue_response(
            200, content=opds2_files_fixture.sample_text("feed2.json")
        )
        opds2_import_fixture.client.queue_response(
            200, content=opds2_files_fixture.sample_text("feed.json")
        )
        opds2.import_collection.delay(
            opds2_import_fixture.collection.id, force=True
        ).wait()

        # Should NOT see the log message about stopping due to unchanged publications
        assert "Found unchanged publications in feed" not in caplog.text
        # Should see the log message about completing the import
        assert "Import complete." in caplog.text
        # Should have 6 tasks queued (3 from each page) even though publications are unchanged
        assert len(apply_task_fixture.apply_queue) == 6

    def test_import_wrong_collection(
        self,
        db: DatabaseTransactionFixture,
        opds2_import_fixture: OPDS2ImportFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ):
        collection = db.collection(protocol=OverdriveAPI)
        with pytest.raises(PalaceValueError, match="is not a OPDS2 collection"):
            opds2.import_collection.delay(collection.id).wait()

    def test_import_odl_feed(
        self,
        db: DatabaseTransactionFixture,
        apply_task_fixture: ApplyTaskFixture,
        opds2_import_fixture: OPDS2ImportFixture,
        opds2_with_odl_files_fixture: OPDS2WithODLFilesFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        """
        If we accidentally import an ODL feed, we will ignore any OPDS2+ODL publications
        in the feed, and log errors when we encounter them.
        """
        opds2_import_fixture.client.queue_response(
            200,
            content=opds2_with_odl_files_fixture.sample_text(
                "feed-audiobook-streaming.json"
            ),
        )
        opds2.import_collection.delay(opds2_import_fixture.collection.id).wait()

        # We shouldn't have imported any publications, since the items in the feed are
        # ODL publications, which are not supported by OPDS2Importer.
        apply_task_fixture.process_apply_queue()
        assert len(apply_task_fixture.get_editions()) == 0

        assert (
            "Failed to import publication: urn:ISBN:9780792766919 (Past Imperfect)"
            in caplog.text
        )


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

    def test_import_and_reap_not_found_chord_success(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ) -> None:
        collection = db.collection(
            protocol=OPDS2API,
        )

        test_lp_1 = db.licensepool(edition=None, collection=collection)
        test_lp_2 = db.licensepool(edition=None, collection=collection)

        assert (
            collection.integration_configuration.context.get(
                OPDS2API.LAST_REAP_TIME_KEY
            )
            is None
        )

        identifier_set = IdentifierSet(redis_fixture.client)
        identifier_set.add(test_lp_1.identifier)

        import_time = utc_now()
        with (
            patch.object(opds2, "opds_import_task") as mock_import,
            patch.object(identifiers, "circulation_apply") as circ_apply_task,
            freeze_time(import_time),
        ):
            mock_import.return_value = identifier_set
            opds2.import_and_reap_not_found_chord(collection.id).apply_async().wait()

        db.session.refresh(collection)
        last_reap_time_str = collection.integration_configuration.context.get(
            OPDS2API.LAST_REAP_TIME_KEY
        )
        assert last_reap_time_str is not None
        last_reap_time = datetime.datetime.fromisoformat(last_reap_time_str)
        assert last_reap_time == import_time

        # Make sure we marked the correct identifier as unavailable
        circ_apply_task.delay.assert_called_once()
        assert (
            circ_apply_task.delay.call_args.kwargs.get("collection_id") == collection.id
        )
        circulation_data: CirculationData | None = (
            circ_apply_task.delay.call_args.kwargs.get("circulation")
        )
        assert circulation_data is not None
        assert (
            circulation_data.primary_identifier_data
            == IdentifierData.from_identifier(test_lp_2.identifier)
        )

    def test_import_and_reap_not_found_chord_importer_returns_none(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """
        Test that import_and_reap_not_found_chord handles the case where the importer returns None.
        In this case the callback should be called, but the last_reap_time should not be updated.

        This simulates the case where the import fails and returns None.
        """
        caplog.set_level(LogLevel.info)

        collection = db.collection(
            protocol=OPDS2API,
        )

        assert (
            collection.integration_configuration.context.get(
                OPDS2API.LAST_REAP_TIME_KEY
            )
            is None
        )

        with patch.object(opds2, "importer_from_collection") as mock_importer:
            mock_importer.return_value.import_feed.return_value = None
            opds2.import_and_reap_not_found_chord(collection.id).apply_async().wait()

        assert "last_reap_time not updated" in caplog.text

        db.session.refresh(collection)
        assert (
            collection.integration_configuration.context.get(
                OPDS2API.LAST_REAP_TIME_KEY
            )
            is None
        )

    def test_import_and_reap_not_found_chord_importer_exception(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        collection = db.collection(
            protocol=OPDS2API,
        )

        assert (
            collection.integration_configuration.context.get(
                OPDS2API.LAST_REAP_TIME_KEY
            )
            is None
        )

        with patch.object(opds2, "importer_from_collection") as mock_importer:
            mock_importer.return_value.import_feed.side_effect = PalaceValueError(
                "OH NO!"
            )
            with pytest.raises(PalaceValueError):
                opds2.import_and_reap_not_found_chord(
                    collection.id
                ).apply_async().wait()

        db.session.refresh(collection)
        assert (
            collection.integration_configuration.context.get(
                OPDS2API.LAST_REAP_TIME_KEY
            )
            is None
        )
