import functools
import urllib.error
import urllib.parse
import urllib.request
from unittest.mock import create_autospec

import pytest
from pymarc import Record

from api.marc import LibraryAnnotator
from core.config import Configuration
from core.marc import MarcExporterLibrarySettings
from core.model import ConfigurationSetting, create
from core.model.discovery_service_registration import DiscoveryServiceRegistration
from tests.fixtures.database import (
    DatabaseTransactionFixture,
    IntegrationConfigurationFixture,
)


class LibraryAnnotatorFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.library = db.default_library()

        # Mock class to verify that the correct methods
        # are called by annotate_work_record.
        self.mock_annotator = LibraryAnnotator(self.library)
        self.mock_add_marc_organization_code = create_autospec(
            spec=self.mock_annotator.add_marc_organization_code
        )
        self.mock_annotator.add_marc_organization_code = (
            self.mock_add_marc_organization_code
        )
        self.mock_add_summary = create_autospec(spec=self.mock_annotator.add_summary)
        self.mock_annotator.add_summary = self.mock_add_summary
        self.mock_add_simplified_genres = create_autospec(
            spec=self.mock_annotator.add_simplified_genres
        )
        self.mock_annotator.add_simplified_genres = self.mock_add_simplified_genres
        self.mock_add_web_client_urls = create_autospec(
            spec=self.mock_annotator.add_web_client_urls
        )
        self.mock_annotator.add_web_client_urls = self.mock_add_web_client_urls
        self.mock_add_distributor = create_autospec(
            spec=self.mock_annotator.add_distributor
        )
        self.mock_annotator.add_distributor = self.mock_add_distributor
        self.mock_add_formats = create_autospec(spec=self.mock_annotator.add_formats)
        self.mock_annotator.add_formats = self.mock_add_formats

        self.record = Record()
        self.work = db.work(with_license_pool=True)
        self.pool = self.work.license_pools[0]
        self.edition = self.pool.presentation_edition
        self.identifier = self.pool.identifier

        self.mock_annotate_work_record = functools.partial(
            self.mock_annotator.annotate_work_record,
            work=self.work,
            active_license_pool=self.pool,
            edition=self.edition,
            identifier=self.identifier,
            record=self.record,
        )

        # The URL for a work is constructed as:
        # - <cm-base>/<lib-short-name>/works/<qualified-identifier>
        work_link_template = "{cm_base}/{lib}/works/{qid}"
        # It is then encoded and the web client URL is constructed in this form:
        # - <web-client-base>/book/<encoded-work-url>
        client_url_template = "{client_base}/book/{work_link}"

        qualified_identifier = urllib.parse.quote(
            self.identifier.type + "/" + self.identifier.identifier, safe=""
        )
        cm_base_url = "http://test-circulation-manager"

        expected_work_link = work_link_template.format(
            cm_base=cm_base_url, lib=self.library.short_name, qid=qualified_identifier
        )
        encoded_work_link = urllib.parse.quote(expected_work_link, safe="")

        self.client_base_1 = "http://web_catalog"
        self.client_base_2 = "http://another_web_catalog"
        self.expected_client_url_1 = client_url_template.format(
            client_base=self.client_base_1, work_link=encoded_work_link
        )
        self.expected_client_url_2 = client_url_template.format(
            client_base=self.client_base_2, work_link=encoded_work_link
        )

        # A few checks to ensure that our setup is useful.
        assert self.library.short_name is not None
        assert len(self.library.short_name) > 0
        assert self.client_base_1 != self.client_base_2
        assert self.expected_client_url_1 != self.expected_client_url_2
        assert self.expected_client_url_1.startswith(self.client_base_1)
        assert self.expected_client_url_2.startswith(self.client_base_2)

        ConfigurationSetting.sitewide(
            db.session, Configuration.BASE_URL_KEY
        ).value = cm_base_url

        self.annotator = LibraryAnnotator(self.library)

        self.add_web_client_urls = functools.partial(
            self.annotator.add_web_client_urls,
            record=self.record,
            library=self.library,
            identifier=self.identifier,
        )


@pytest.fixture
def library_annotator_fixture(
    db: DatabaseTransactionFixture,
) -> LibraryAnnotatorFixture:
    return LibraryAnnotatorFixture(db)


class TestLibraryAnnotator:
    @pytest.mark.parametrize(
        "settings",
        [
            pytest.param(MarcExporterLibrarySettings(), id="defaults"),
            pytest.param(
                MarcExporterLibrarySettings(include_summary=False), id="summary_false"
            ),
            pytest.param(
                MarcExporterLibrarySettings(include_genres=False), id="genres_false"
            ),
            pytest.param(
                MarcExporterLibrarySettings(
                    include_summary=False, include_genres=False
                ),
                id="summary_and_genres_false",
            ),
        ],
    )
    def test_annotate_work_record_default_settings(
        self,
        library_annotator_fixture: LibraryAnnotatorFixture,
        settings: MarcExporterLibrarySettings,
    ) -> None:
        library_annotator_fixture.mock_annotate_work_record(settings=settings)

        # If there are no settings, or the settings are false, the only methods called will be add_web_client_urls
        # and the parent class methods.
        library_annotator_fixture.mock_add_marc_organization_code.assert_not_called()
        library_annotator_fixture.mock_add_summary.assert_not_called()
        library_annotator_fixture.mock_add_simplified_genres.assert_not_called()
        library_annotator_fixture.mock_add_web_client_urls.assert_called_once_with(
            library_annotator_fixture.record,
            library_annotator_fixture.library,
            library_annotator_fixture.identifier,
            settings,
        )
        library_annotator_fixture.mock_add_distributor.assert_called_once_with(
            library_annotator_fixture.record, library_annotator_fixture.pool
        )
        library_annotator_fixture.mock_add_formats.assert_called_once_with(
            library_annotator_fixture.record, library_annotator_fixture.pool
        )

    def test_annotate_work_record_settings(
        self, library_annotator_fixture: LibraryAnnotatorFixture
    ) -> None:
        # Once the include settings are true and the marc organization code is set,
        # all methods are called.
        settings = MarcExporterLibrarySettings(
            include_summary=True,
            include_genres=True,
            organization_code="marc org",
            web_client_url="http://web_catalog",
        )

        library_annotator_fixture.mock_annotate_work_record(settings=settings)

        library_annotator_fixture.mock_add_marc_organization_code.assert_called_once_with(
            library_annotator_fixture.record, settings.organization_code
        )

        library_annotator_fixture.mock_add_summary.assert_called_once_with(
            library_annotator_fixture.record, library_annotator_fixture.work
        )

        library_annotator_fixture.mock_add_simplified_genres.assert_called_once_with(
            library_annotator_fixture.record, library_annotator_fixture.work
        )

        library_annotator_fixture.mock_add_web_client_urls.assert_called_once_with(
            library_annotator_fixture.record,
            library_annotator_fixture.library,
            library_annotator_fixture.identifier,
            settings,
        )

        library_annotator_fixture.mock_add_distributor.assert_called_once_with(
            library_annotator_fixture.record, library_annotator_fixture.pool
        )

        library_annotator_fixture.mock_add_formats.assert_called_once_with(
            library_annotator_fixture.record, library_annotator_fixture.pool
        )

    def test_add_web_client_urls_none(
        self, library_annotator_fixture: LibraryAnnotatorFixture
    ):
        settings = MarcExporterLibrarySettings()

        # If no web catalog URLs are set for the library, nothing will be changed.
        library_annotator_fixture.add_web_client_urls(exporter_settings=settings)
        assert [] == library_annotator_fixture.record.get_fields("856")

    def test_add_web_client_urls_from_library_registry(
        self,
        db: DatabaseTransactionFixture,
        create_integration_configuration: IntegrationConfigurationFixture,
        library_annotator_fixture: LibraryAnnotatorFixture,
    ):
        settings = MarcExporterLibrarySettings()

        # Add a URL from a library registry.
        registry = create_integration_configuration.discovery_service()
        create(
            db.session,
            DiscoveryServiceRegistration,
            library=db.default_library(),
            integration=registry,
            web_client=library_annotator_fixture.client_base_1,
        )

        library_annotator_fixture.add_web_client_urls(exporter_settings=settings)
        [field] = library_annotator_fixture.record.get_fields("856")
        assert field.indicators == ["4", "0"]
        assert (
            field.get_subfields("u")[0]
            == library_annotator_fixture.expected_client_url_1
        )

    def test_add_web_client_urls_from_configuration(
        self, library_annotator_fixture: LibraryAnnotatorFixture
    ):
        # Add a manually configured URL on a MARC export integration.
        settings = MarcExporterLibrarySettings(
            web_client_url=library_annotator_fixture.client_base_2
        )
        library_annotator_fixture.add_web_client_urls(exporter_settings=settings)
        [field] = library_annotator_fixture.record.get_fields("856")
        assert field.indicators == ["4", "0"]
        assert (
            field.get_subfields("u")[0]
            == library_annotator_fixture.expected_client_url_2
        )

    def test_add_web_client_urls_from_both(
        self,
        db: DatabaseTransactionFixture,
        create_integration_configuration: IntegrationConfigurationFixture,
        library_annotator_fixture: LibraryAnnotatorFixture,
    ):
        # Add a URL from a library registry.
        registry = create_integration_configuration.discovery_service()
        create(
            db.session,
            DiscoveryServiceRegistration,
            library=db.default_library(),
            integration=registry,
            web_client=library_annotator_fixture.client_base_1,
        )

        # Add a manually configured URL on a MARC export integration.
        settings = MarcExporterLibrarySettings(
            web_client_url=library_annotator_fixture.client_base_2
        )

        library_annotator_fixture.add_web_client_urls(exporter_settings=settings)

        fields = library_annotator_fixture.record.get_fields("856")
        assert len(fields) == 2

        # The manually configured URL should be first.
        [field_1, field_2] = fields
        assert field_1.indicators == ["4", "0"]
        assert (
            field_1.get_subfields("u")[0]
            == library_annotator_fixture.expected_client_url_2
        )

        assert field_2.indicators == ["4", "0"]
        assert (
            field_2.get_subfields("u")[0]
            == library_annotator_fixture.expected_client_url_1
        )
