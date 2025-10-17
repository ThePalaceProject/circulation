from __future__ import annotations

from dataclasses import dataclass

import pytest

from palace.manager.core.coverage import CoverageFailure
from palace.manager.data_layer.policy.presentation import PresentationCalculationPolicy
from palace.manager.integration.license.overdrive.coverage import (
    OverdriveBibliographicCoverageProvider,
)
from palace.manager.scripts.coverage_provider import RunCollectionCoverageProviderScript
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism
from palace.manager.sqlalchemy.model.resource import Representation
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.overdrive import OverdriveAPIFixture
from tests.mocks.overdrive import MockOverdriveAPI


@dataclass
class OverdriveBibliographicCoverageProviderFixture:
    overdrive: OverdriveAPIFixture
    provider: OverdriveBibliographicCoverageProvider
    api: MockOverdriveAPI


@pytest.fixture
def overdrive_biblio_provider_fixture(
    db: DatabaseTransactionFixture,
    overdrive_api_fixture: OverdriveAPIFixture,
) -> OverdriveBibliographicCoverageProviderFixture:
    overdrive = overdrive_api_fixture
    api = overdrive_api_fixture.api
    provider = OverdriveBibliographicCoverageProvider(
        overdrive_api_fixture.collection, api=api
    )
    return OverdriveBibliographicCoverageProviderFixture(
        overdrive,
        provider,
        api,
    )


class TestOverdriveBibliographicCoverageProvider:
    """Test the code that looks up bibliographic information from Overdrive."""

    def test_script_instantiation(
        self,
        overdrive_biblio_provider_fixture: OverdriveBibliographicCoverageProviderFixture,
    ):
        """Test that RunCoverageProviderScript can instantiate
        the coverage provider.
        """

        fixture = overdrive_biblio_provider_fixture
        db = fixture.overdrive.db

        script = RunCollectionCoverageProviderScript(
            OverdriveBibliographicCoverageProvider,
            db.session,
            api=fixture.api,
        )
        [provider] = script.providers
        assert isinstance(provider, OverdriveBibliographicCoverageProvider)
        assert provider.api is fixture.api
        assert fixture.overdrive.collection == provider.collection

    def test_invalid_or_unrecognized_guid(
        self,
        overdrive_biblio_provider_fixture: OverdriveBibliographicCoverageProviderFixture,
        db: DatabaseTransactionFixture,
    ):
        """A bad or malformed GUID can't get coverage."""
        fixture = overdrive_biblio_provider_fixture
        http = fixture.overdrive.mock_http

        identifier = db.identifier()
        identifier.identifier = "bad guid"

        error = '{"errorCode": "InvalidGuid", "message": "An invalid guid was given.", "token": "7aebce0e-2e88-41b3-b6d3-82bf15f8e1a2"}'
        http.queue_response(200, content=error)

        failure = fixture.provider.process_item(identifier)
        assert isinstance(failure, CoverageFailure)
        assert failure.transient is False
        assert failure.exception == "Invalid Overdrive ID: bad guid"

        # This is for when the GUID is well-formed but doesn't
        # correspond to any real Overdrive book.
        error = '{"errorCode": "NotFound", "message": "Not found in Overdrive collection.", "token": "7aebce0e-2e88-41b3-b6d3-82bf15f8e1a2"}'
        http.queue_response(200, content=error)

        failure = fixture.provider.process_item(identifier)
        assert isinstance(failure, CoverageFailure)
        assert failure.transient is False
        assert failure.exception == "ID not recognized by Overdrive: bad guid"

    def test_process_item_creates_presentation_ready_work(
        self,
        overdrive_biblio_provider_fixture: OverdriveBibliographicCoverageProviderFixture,
        db: DatabaseTransactionFixture,
    ):
        """Test the normal workflow where we ask Overdrive for data,
        Overdrive provides it, and we create a presentation-ready work.
        """
        fixture = overdrive_biblio_provider_fixture
        http = fixture.overdrive.mock_http

        # Here's the book mentioned in overdrive_metadata.json.
        identifier = db.identifier(identifier_type=Identifier.OVERDRIVE_ID)
        identifier.identifier = "3896665d-9d81-4cac-bd43-ffc5066de1f5"

        # This book has no LicensePool.
        assert identifier.licensed_through == []

        # Run it through the OverdriveBibliographicCoverageProvider
        raw, info = fixture.overdrive.sample_json("overdrive_metadata.json")
        http.queue_response(200, content=raw)

        [result] = fixture.provider.process_batch([identifier])

        assert result == identifier

        assert fixture.work_policy_recalc_fixture.is_queued(
            identifier.work.id,
            PresentationCalculationPolicy.recalculate_everything(),
        )
        # A LicensePool was created, not because we know anything
        # about how we've licensed this book, but to have a place to
        # store the information about what formats the book is
        # available in.
        [pool] = identifier.licensed_through
        assert pool.licenses_owned == 0
        assert {
            (
                x.delivery_mechanism.content_type,
                x.delivery_mechanism.drm_scheme,
                x.available,
            )
            for x in pool.delivery_mechanisms
        } == {
            (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM, False),
            (Representation.PDF_MEDIA_TYPE, DeliveryMechanism.NO_DRM, False),
            (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM, True),
            (
                DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
                DeliveryMechanism.STREAMING_DRM,
                True,
            ),
        }

        # A Work was created and made presentation ready.
        assert pool.work.title == "Agile Documentation"
        assert pool.work.presentation_ready is True
