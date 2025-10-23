from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest
from psycopg2._range import NumericRange

from palace.manager.api.circulation.dispatcher import CirculationApiDispatcher
from palace.manager.api.circulation.fulfillment import UrlFulfillment
from palace.manager.celery.tasks import opds1
from palace.manager.core.classifier import Classifier
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.integration.license.opds.opds1.api import OPDSAPI
from palace.manager.integration.license.opds.opds2.api import OPDS2API
from palace.manager.integration.license.opds.settings.wayfless import (
    SAMLWAYFlessFulfillmentError,
)
from palace.manager.integration.patron_auth.saml.credential import SAMLCredentialManager
from palace.manager.integration.patron_auth.saml.metadata.model import (
    SAMLAttributeStatement,
    SAMLNameID,
    SAMLNameIDFormat,
    SAMLSubject,
)
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.measurement import Measurement
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.sqlalchemy.model.resource import Representation
from palace.manager.sqlalchemy.model.work import Work
from tests.fixtures.celery import ApplyTaskFixture, CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import OPDSFilesFixture
from tests.fixtures.http import MockHttpClientFixture
from tests.fixtures.redis import RedisFixture


class Opds1ImportFixture:
    def __init__(
        self,
        db: DatabaseTransactionFixture,
        http_client: MockHttpClientFixture,
        apply_fixture: ApplyTaskFixture,
    ):
        self.db = db
        self.collection = db.collection(
            protocol=OPDSAPI,
            settings=db.opds_settings(data_source="OPDS"),
        )
        self.client = http_client
        self.apply = apply_fixture

    def import_feed(
        self,
        feed: str,
        collection: Collection | None = None,
    ) -> tuple[
        list[Edition],
        list[LicensePool],
        list[Work],
    ]:
        self.client.queue_response(200, content=feed, index=0)
        self.run_import_task(collection, apply=True)

        editions = self.apply.get_editions()
        license_pools = self.apply.get_pools()
        works = self.apply.get_works()

        return editions, license_pools, works

    def run_import_task(
        self, collection: Collection | None = None, apply: bool = False
    ) -> None:
        collection = collection if collection is not None else self.collection
        opds1.import_collection.delay(collection.id).wait()
        if apply:
            self.apply.process_apply_queue()

    def wayfless_circulation_api(
        self,
        feed: str,
        has_saml_entity_id: bool = True,
        has_saml_credential: bool = True,
    ) -> tuple[CirculationApiDispatcher, Patron, LicensePool]:
        patron = self.db.patron()

        idp_entityID = (
            "https://mycompany.com/adfs/services/trust" if has_saml_entity_id else None
        )

        saml_subject = SAMLSubject(
            idp_entityID,
            SAMLNameID(
                SAMLNameIDFormat.PERSISTENT.value, "", "", "patron@university.com"
            ),
            SAMLAttributeStatement([]),
        )
        saml_credential_manager = SAMLCredentialManager()
        if has_saml_credential:
            saml_credential_manager.create_saml_token(
                self.db.session, patron, saml_subject
            )

        collection = self.db.collection(
            "OPDS collection with a WAYFless acquisition link",
            protocol=OPDSAPI,
            settings=self.db.opds_settings(
                external_account_id="http://wayfless.example.com/feed",
                saml_wayfless_url_template="https://fsso.springer.com/saml/login?idp={idp}&targetUrl={targetUrl}",
            ),
            library=self.db.default_library(),
        )

        imported_editions, pools, works = self.import_feed(feed, collection)

        pool = pools[0]
        pool.loan_to(patron)

        return (
            CirculationApiDispatcher(
                self.db.session,
                self.db.default_library(),
                {collection.id: OPDSAPI(self.db.session, collection)},
            ),
            patron,
            pool,
        )


@pytest.fixture
def opds1_import_fixture(
    db: DatabaseTransactionFixture,
    http_client: MockHttpClientFixture,
    apply_task_fixture: ApplyTaskFixture,
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
) -> Opds1ImportFixture:
    return Opds1ImportFixture(
        db,
        http_client,
        apply_task_fixture,
    )


class TestImportCollection:

    def test_importing_bad_feed(
        self,
        opds1_import_fixture: Opds1ImportFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        opds1_import_fixture.import_feed("hot garbage 🗑️")
        assert "Failed to parse OPDS 1.x feed" in caplog.text

    def test_import(
        self,
        db: DatabaseTransactionFixture,
        opds1_import_fixture: Opds1ImportFixture,
        opds_files_fixture: OPDSFilesFixture,
    ) -> None:
        feed_data = opds_files_fixture.sample_text("content_server_mini.opds")

        imported_editions, pools, works = opds1_import_fixture.import_feed(feed_data)

        # Both editions were imported, because they were new.
        assert len(imported_editions) == 2

        # And pools and works were created
        assert len(pools) == 2
        assert len(works) == 2

        [crow, mouse] = sorted(imported_editions, key=lambda x: str(x.title))

        # Work was created for both books.
        assert crow.data_source.name == "OPDS"
        assert crow.work is not None
        assert crow.medium == Edition.BOOK_MEDIUM
        assert crow.license_pools[0].collection == opds1_import_fixture.collection

        assert mouse.work is not None
        assert mouse.medium == Edition.AUDIO_MEDIUM

        # Four links have been added to the identifier of the 'mouse'
        # edition.
        acquisition, image, thumbnail, description = sorted(
            mouse.primary_identifier.links, key=lambda x: str(x.rel)
        )

        # A Representation was imported for the summary with known
        # content.
        description_rep = description.resource.representation
        assert description_rep.content == b"This is a summary!"
        assert description_rep.media_type == Representation.TEXT_PLAIN

        # A Representation was imported for the image with a media type
        # inferred from its URL.
        image_rep = image.resource.representation
        assert image_rep.url.endswith("_9.png")
        assert image_rep.media_type == Representation.PNG_MEDIA_TYPE

        # The thumbnail was imported similarly, and its representation
        # was marked as a thumbnail of the full-sized image.
        thumbnail_rep = thumbnail.resource.representation
        assert thumbnail_rep.media_type == Representation.PNG_MEDIA_TYPE
        assert thumbnail_rep.thumbnail_of == image_rep

        # Three links were added to the identifier of the 'crow' edition.
        broken_image, working_image, acquisition = sorted(
            crow.primary_identifier.links, key=lambda x: str(x.resource.url)
        )

        # Because these images did not have a specified media type or a
        # distinctive extension, and we have not actually retrieved
        # the URLs yet, we were not able to determine their media type,
        # so they have no associated Representation.
        assert broken_image.resource.url is not None
        assert broken_image.resource.url.endswith("/broken-cover-image")
        assert working_image.resource.url is not None
        assert working_image.resource.url.endswith("/working-cover-image")
        assert broken_image.resource.representation is None
        assert working_image.resource.representation is None

        # Three measurements have been added to the 'mouse' edition.
        popularity, quality, rating = sorted(
            (x for x in mouse.primary_identifier.measurements if x.is_most_recent),
            key=lambda x: str(x.quantity_measured),
        )

        assert popularity.data_source.name == "OPDS"
        assert popularity.quantity_measured == Measurement.POPULARITY
        assert popularity.value == 0.25

        assert quality.data_source.name == "OPDS"
        assert quality.quantity_measured == Measurement.QUALITY
        assert quality.value == 0.3333

        assert rating.data_source.name == "OPDS"
        assert rating.quantity_measured == Measurement.RATING
        assert rating.value == 0.6

        seven, children, courtship, fantasy, pz, magic, new_york = sorted(
            mouse.primary_identifier.classifications, key=lambda x: str(x.subject.name)
        )

        pz_s = pz.subject
        assert pz_s.name == "Juvenile Fiction"
        assert pz_s.identifier == "PZ"

        new_york_s = new_york.subject
        assert new_york_s.name == "New York (N.Y.) -- Fiction"
        assert new_york_s.identifier == "sh2008108377"

        assert seven.subject.identifier == "7"
        assert seven.weight == 100
        assert seven.subject.type == Subject.AGE_RANGE

        # The pools have presentation editions.
        assert {x.presentation_edition.title for x in pools} == {
            "The Green Mouse",
            "Johnny Crow's Party",
        }

        def sort_key(x: LicensePool) -> str:
            assert x.presentation_edition.title is not None
            return x.presentation_edition.title

        [crow_pool, mouse_pool] = sorted(pools, key=sort_key)

        assert crow_pool.collection == opds1_import_fixture.collection
        assert mouse_pool.collection == opds1_import_fixture.collection
        assert crow_pool.work is not None
        assert mouse_pool.work is not None

        # The pools are all open access
        for pool in pools:
            assert pool.open_access is True
            assert pool.licenses_owned == LicensePool.UNLIMITED_ACCESS
            assert pool.licenses_available == LicensePool.UNLIMITED_ACCESS

        # Test the works quality calculation
        # First we update the measurements datasource to be the metadata wrangler,
        # so we can more easily test the quality calculation, since we know the scaling
        # is 1 for the wrangler.
        popularity.data_source = DataSource.lookup(
            db.session, DataSource.METADATA_WRANGLER
        )
        quality.data_source = DataSource.lookup(
            db.session, DataSource.METADATA_WRANGLER
        )
        rating.data_source = DataSource.lookup(db.session, DataSource.METADATA_WRANGLER)
        work = mouse_pool.work
        work.calculate_presentation()
        assert work.quality is not None
        assert round(work.quality, 4) == 0.4142
        assert work.audience == Classifier.AUDIENCE_CHILDREN
        assert work.target_age == NumericRange(7, 7, "[]")

        # The information used to create the first LicensePool said
        # that the licensing authority is Project Gutenberg, so that's used
        # as the DataSource for the first LicensePool. The information used
        # to create the second LicensePool didn't include a data source,
        # so the source of the OPDS feed "OPDS" was used.
        assert {pool.data_source.name for pool in pools} == {
            DataSource.GUTENBERG,
            "OPDS",
        }

        # Bonus: make sure that delivery mechanisms are set appropriately.
        [mech] = mouse_pool.delivery_mechanisms
        assert (
            mech.delivery_mechanism.content_type
            == Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE
        )
        assert mech.delivery_mechanism.drm_scheme == DeliveryMechanism.NO_DRM
        assert mech.resource.url == "http://www.gutenberg.org/ebooks/10441.epub.images"

        # If we import the same file again, no tasks are queued because we've already
        # imported everything in the feed.
        opds1_import_fixture.client.queue_response(200, content=feed_data)
        opds1_import_fixture.run_import_task()
        assert len(opds1_import_fixture.apply.apply_queue) == 0

    def test_import_with_unrecognized_distributor_creates_distributor(
        self,
        db: DatabaseTransactionFixture,
        opds1_import_fixture: Opds1ImportFixture,
        opds_files_fixture: OPDSFilesFixture,
    ):
        """We get a book from a previously unknown data source, with a license
        that comes from a second previously unknown data source. The
        book is imported and both DataSources are created.
        """
        feed_data = opds_files_fixture.sample_text("unrecognized_distributor.opds")

        collection = db.collection(
            protocol=OPDSAPI, settings=db.opds_settings(data_source="some new source")
        )

        imported_editions, pools, works = opds1_import_fixture.import_feed(
            feed_data, collection
        )

        # We imported an Edition because there was bibliographic.
        [edition] = imported_editions
        assert edition.data_source.name == "some new source"

        # We imported a LicensePool because there was an open-access
        # link, even though the ultimate source of the link was one
        # we'd never seen before.
        [pool] = pools
        assert pool.data_source.name == "Unknown Source"

        # From an Edition and a LicensePool we created a Work.
        assert len(works) == 1

    def test_import_updates_bibliographic(
        self,
        db: DatabaseTransactionFixture,
        opds1_import_fixture: Opds1ImportFixture,
        opds_files_fixture: OPDSFilesFixture,
    ):
        feed = opds_files_fixture.sample_text("metadata_wrangler_overdrive.opds")

        edition, is_new = db.edition(
            DataSource.OVERDRIVE, Identifier.OVERDRIVE_ID, with_license_pool=True
        )
        [old_license_pool] = edition.license_pools
        old_license_pool.calculate_work()
        work = old_license_pool.work

        feed = feed.replace("{OVERDRIVE ID}", edition.primary_identifier.identifier)

        collection = db.collection(
            protocol=OPDSAPI,
            settings=db.opds_settings(data_source=DataSource.OVERDRIVE),
        )

        (
            imported_editions,
            imported_pools,
            imported_works,
        ) = opds1_import_fixture.import_feed(feed, collection)

        # The edition we created has had its bibliographic updated.
        [new_edition] = imported_editions
        assert edition == new_edition
        assert new_edition.title == "The Green Mouse"
        assert new_edition.data_source.name == DataSource.OVERDRIVE

        # But the license pools have not changed.
        assert edition.license_pools == [old_license_pool]
        assert work.license_pools == [old_license_pool]

    def test_import_from_license_source(
        self,
        opds1_import_fixture: Opds1ImportFixture,
        opds_files_fixture: OPDSFilesFixture,
    ):
        # Instead of importing this data as though it came from the
        # metadata wrangler, let's import it as though it came from the
        # open-access content server.
        feed_data = opds_files_fixture.sample_text("content_server_mini.opds")

        (
            imported_editions,
            imported_pools,
            imported_works,
        ) = opds1_import_fixture.import_feed(feed_data)

        # Two works have been created, because the content server
        # actually tells you how to get copies of these books.
        [crow, mouse] = sorted(imported_works, key=lambda x: x.title)

        # Each work has one license pool.
        [crow_pool] = crow.license_pools
        [mouse_pool] = mouse.license_pools

        # The OPDS importer sets the data source of the license pool
        # to Project Gutenberg, since that's the authority that grants
        # access to the book.
        assert mouse_pool.data_source.name == DataSource.GUTENBERG

        # But the license pool's presentation edition has a data
        # source associated with the collection.
        assert mouse_pool.presentation_edition.data_source.name == "OPDS"

        # Since the 'mouse' book came with an open-access link, the license
        # pool delivery mechanism has been marked as open access.
        assert mouse_pool.open_access is True
        assert (
            mouse_pool.delivery_mechanisms[0].rights_status.uri
            == RightsStatus.PUBLIC_DOMAIN_USA
        )

        # The 'mouse' work was marked presentation-ready immediately.
        assert mouse_pool.work.presentation_ready == True

        # The OPDS feed didn't actually say where the 'crow' book
        # comes from, but we did tell the importer to use the open access
        # content server as the data source, so both a Work and a LicensePool
        # were created, and their data source is the datasoruce of the collection
        assert crow_pool.data_source.name == "OPDS"

    def test_import_book_that_offers_no_license(
        self,
        opds1_import_fixture: Opds1ImportFixture,
        opds_files_fixture: OPDSFilesFixture,
    ):
        feed = opds_files_fixture.sample_text("book_without_license.opds")
        (
            imported_editions,
            imported_pools,
            imported_works,
        ) = opds1_import_fixture.import_feed(feed)

        # We got an Edition for this book, but no LicensePool and no Work.
        [edition] = imported_editions
        assert edition.title == "Howards End"
        assert imported_pools == []
        assert imported_works == []

        # We were able to figure out the medium of the Edition
        # based on its <dcterms:format> tag.
        assert edition.medium == Edition.AUDIO_MEDIUM

    def test_import_multiple_pages(
        self,
        opds1_import_fixture: Opds1ImportFixture,
        opds_files_fixture: OPDSFilesFixture,
    ):
        # Queue up two feed pages, the first one has a next link, and the second one doesn't
        # We should import both pages.
        opds1_import_fixture.client.queue_response(
            200, content=opds_files_fixture.sample_text("content_server.opds")
        )
        opds1_import_fixture.client.queue_response(
            200, content=opds_files_fixture.sample_text("book_with_license.opds")
        )
        opds1_import_fixture.run_import_task(apply=True)

        editions = opds1_import_fixture.apply.get_editions()

        # There are 77 editions, 76 in the first feed, and 1 in the second feed.
        assert len(editions) == 77

        # Make sure the identifier from the second feed is present
        edition = opds1_import_fixture.apply.get_edition_by_identifier(
            editions, "https://unglue.it/api/id/work/7775/"
        )
        assert edition is not None
        assert edition.title == "Warbreaker"

    def test_import_open_access_audiobook(
        self,
        opds1_import_fixture: Opds1ImportFixture,
        opds_files_fixture: OPDSFilesFixture,
    ):
        feed = opds_files_fixture.sample_text("audiobooks.opds")
        download_manifest_url = "https://api.archivelab.org/books/kniga_zitij_svjatyh_na_mesjac_avgust_eu_0811_librivox/opds_audio_manifest"

        (
            imported_editions,
            imported_pools,
            imported_works,
        ) = opds1_import_fixture.import_feed(feed)

        assert len(imported_editions) == 1

        [august] = imported_editions
        assert august.title == "Zhitiia Sviatykh, v. 12 - August"

        [august_pool] = imported_pools
        assert august_pool.open_access == True

        [lpdm] = august_pool.delivery_mechanisms
        mechanism = lpdm.delivery_mechanism
        assert mechanism.content_type == Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE
        assert mechanism.drm_scheme == DeliveryMechanism.NO_DRM

    def test_wayfless_url(
        self,
        opds1_import_fixture: Opds1ImportFixture,
        opds_files_fixture: OPDSFilesFixture,
    ):
        circulation, patron, pool = opds1_import_fixture.wayfless_circulation_api(
            opds_files_fixture.sample_text("wayfless.opds")
        )
        fulfilment = circulation.fulfill(
            patron, "test", pool, pool.delivery_mechanisms[0]
        )
        assert isinstance(fulfilment, UrlFulfillment)
        assert (
            fulfilment.content_link
            == "https://fsso.springer.com/saml/login?idp=https%3A%2F%2Fmycompany.com%2Fadfs%2Fservices%2Ftrust&"
            "targetUrl=http%3A%2F%2Fwww.gutenberg.org%2Febooks%2F10441.epub.images"
        )

    def test_wayfless_url_no_saml_credential(
        self,
        opds1_import_fixture: Opds1ImportFixture,
        opds_files_fixture: OPDSFilesFixture,
    ):
        circulation, patron, pool = opds1_import_fixture.wayfless_circulation_api(
            opds_files_fixture.sample_text("wayfless.opds"), has_saml_credential=False
        )
        with pytest.raises(
            SAMLWAYFlessFulfillmentError,
            match="There are no existing SAML credentials for patron",
        ):
            circulation.fulfill(patron, "test", pool, pool.delivery_mechanisms[0])

    def test_wayfless_url_no_saml_entity_id(
        self,
        opds1_import_fixture: Opds1ImportFixture,
        opds_files_fixture: OPDSFilesFixture,
    ):
        circulation, patron, pool = opds1_import_fixture.wayfless_circulation_api(
            opds_files_fixture.sample_text("wayfless.opds"), has_saml_entity_id=False
        )
        with pytest.raises(
            SAMLWAYFlessFulfillmentError,
            match="^SAML subject.*does not contain an IdP's entityID$",
        ):
            circulation.fulfill(patron, "test", pool, pool.delivery_mechanisms[0])

    @patch.object(IdentifierData, "parse_urn")
    def test_parse_identifier(
        self,
        mock_parse_urn: MagicMock,
        opds1_import_fixture: Opds1ImportFixture,
        opds_files_fixture: OPDSFilesFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # Normal case, we just call out to Identifier.parse_urn
        expected_identifier = IdentifierData(
            type=Identifier.URI, identifier="https://example.com/12345"
        )
        mock_parse_urn.return_value = expected_identifier

        opds1_import_fixture.client.queue_response(
            200, content=opds_files_fixture.sample_text("book_with_license.opds")
        )
        opds1_import_fixture.run_import_task(apply=True)

        [edition] = opds1_import_fixture.apply.get_editions()

        # Our mock identifier was used to create the edition's primary identifier.
        mock_parse_urn.assert_called_once_with("https://unglue.it/api/id/work/7775/")
        assert (
            IdentifierData.from_identifier(edition.primary_identifier)
            == expected_identifier
        )

        # In the case of an exception, we log the relevant info.
        mock_parse_urn.reset_mock()
        mock_parse_urn.side_effect = ValueError("My god, it's full of stars")
        opds1_import_fixture.client.queue_response(
            200, content=opds_files_fixture.sample_text("book_with_license.opds")
        )
        opds1_import_fixture.run_import_task(apply=True)
        assert (
            "https://unglue.it/api/id/work/7775/ (Warbreaker) - "
            "Could not extract an identifier from the publication: My god, it's full of stars"
            in caplog.text
        )
        assert "Traceback" in caplog.text


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
        collection1 = db.collection(protocol=OPDSAPI)
        collection2 = db.collection(protocol=OPDSAPI)
        decoy_collection = db.collection(protocol=OPDS2API)

        with patch.object(opds1, "import_collection") as mock_import_collection:
            opds1.import_all.delay(force=force).wait()

        mock_import_collection.s.assert_called_once_with(
            force=force,
        )
        mock_import_collection.s.return_value.delay.assert_has_calls(
            [
                call(collection_id=collection1.id),
                call(collection_id=collection2.id),
            ],
            any_order=True,
        )
