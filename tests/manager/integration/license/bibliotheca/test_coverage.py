from __future__ import annotations

from typing import cast

from palace.manager.integration.license.bibliotheca.coverage import (
    BibliothecaBibliographicCoverageProvider,
)
from palace.manager.scripts.coverage_provider import RunCollectionCoverageProviderScript
from palace.manager.sqlalchemy.model.identifier import Identifier
from tests.manager.integration.license.bibliotheca.conftest import (
    BibliothecaAPITestFixture,
)
from tests.mocks.bibliotheca import MockBibliothecaAPI


class TestBibliographicCoverageProvider:
    """Test the code that looks up bibliographic information from Bibliotheca."""

    def test_script_instantiation(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        """Test that RunCollectionCoverageProviderScript can instantiate
        this coverage provider.
        """
        script = RunCollectionCoverageProviderScript(
            BibliothecaBibliographicCoverageProvider,
            bibliotheca_fixture.db.session,
            api_class=MockBibliothecaAPI,
        )
        [provider] = script.providers
        assert isinstance(provider, BibliothecaBibliographicCoverageProvider)
        assert isinstance(provider.api, MockBibliothecaAPI)

    def test_process_item_creates_presentation_ready_work(
        self, bibliotheca_fixture: BibliothecaAPITestFixture
    ):
        db = bibliotheca_fixture.db
        # Test the normal workflow where we ask Bibliotheca for data,
        # Bibliotheca provides it, and we create a presentation-ready work.
        identifier = db.identifier(identifier_type=Identifier.BIBLIOTHECA_ID)
        identifier.identifier = "ddf4gr9"

        # This book has no LicensePools.
        assert [] == identifier.licensed_through

        # Run it through the BibliothecaBibliographicCoverageProvider
        provider = BibliothecaBibliographicCoverageProvider(
            bibliotheca_fixture.collection, api_class=MockBibliothecaAPI
        )
        api = cast(MockBibliothecaAPI, provider.api)
        data = bibliotheca_fixture.files.sample_data("item_metadata_single.xml")

        # We can't use bibliotheca_fixture.api because that's not the same object
        # as the one created by the coverage provider.
        api.queue_response(200, content=data)
        [result] = provider.process_batch([identifier])
        assert identifier == result
        # A LicensePool was created and populated with format and availability
        # information.
        [pool] = identifier.licensed_through
        assert 1 == pool.licenses_owned
        assert 1 == pool.licenses_available
        [lpdm] = pool.delivery_mechanisms
        assert (
            "application/epub+zip (application/vnd.adobe.adept+xml)"
            == lpdm.delivery_mechanism.name
        )

        # A Work was created and made presentation ready.
        assert "The Incense Game" == pool.work.title
        assert True == pool.work.presentation_ready
