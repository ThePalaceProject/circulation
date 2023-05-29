import urllib.error
import urllib.parse
import urllib.request

from pymarc import Record

from api.marc import LibraryAnnotator
from api.registration.registry import Registration
from core.config import Configuration
from core.marc import MARCExporter
from core.model import ConfigurationSetting, ExternalIntegration
from tests.fixtures.database import DatabaseTransactionFixture


class TestLibraryAnnotator:
    def test_annotate_work_record(self, db: DatabaseTransactionFixture):
        # Mock class to verify that the correct methods
        # are called by annotate_work_record.
        class MockAnnotator(LibraryAnnotator):
            called_with = dict()

            def add_marc_organization_code(self, record, marc_org):
                self.called_with["add_marc_organization_code"] = [record, marc_org]

            def add_summary(self, record, work):
                self.called_with["add_summary"] = [record, work]

            def add_simplified_genres(self, record, work):
                self.called_with["add_simplified_genres"] = [record, work]

            def add_web_client_urls(self, record, library, identifier, integration):
                self.called_with["add_web_client_urls"] = [
                    record,
                    library,
                    identifier,
                    integration,
                ]

            # Also check that the parent class annotate_work_record is called.
            def add_distributor(self, record, pool):
                self.called_with["add_distributor"] = [record, pool]

            def add_formats(self, record, pool):
                self.called_with["add_formats"] = [record, pool]

        annotator = MockAnnotator(db.default_library())
        record = Record()
        work = db.work(with_license_pool=True)
        pool = work.license_pools[0]
        edition = pool.presentation_edition
        identifier = pool.identifier

        integration = db.external_integration(
            ExternalIntegration.MARC_EXPORT,
            ExternalIntegration.CATALOG_GOAL,
            libraries=[db.default_library()],
        )

        annotator.annotate_work_record(
            work, pool, edition, identifier, record, integration
        )

        # If there are no settings, the only methods called will be add_web_client_urls
        # and the parent class methods.
        assert "add_marc_organization_code" not in annotator.called_with
        assert "add_summary" not in annotator.called_with
        assert "add_simplified_genres" not in annotator.called_with
        assert [
            record,
            db.default_library(),
            identifier,
            integration,
        ] == annotator.called_with.get("add_web_client_urls")
        assert [record, pool] == annotator.called_with.get("add_distributor")
        assert [record, pool] == annotator.called_with.get("add_formats")

        # If settings are false, the methods still won't be called.
        ConfigurationSetting.for_library_and_externalintegration(
            db.session, MARCExporter.INCLUDE_SUMMARY, db.default_library(), integration
        ).value = "false"

        ConfigurationSetting.for_library_and_externalintegration(
            db.session,
            MARCExporter.INCLUDE_SIMPLIFIED_GENRES,
            db.default_library(),
            integration,
        ).value = "false"

        annotator = MockAnnotator(db.default_library())
        annotator.annotate_work_record(
            work, pool, edition, identifier, record, integration
        )

        assert "add_marc_organization_code" not in annotator.called_with
        assert "add_summary" not in annotator.called_with
        assert "add_simplified_genres" not in annotator.called_with
        assert [
            record,
            db.default_library(),
            identifier,
            integration,
        ] == annotator.called_with.get("add_web_client_urls")
        assert [record, pool] == annotator.called_with.get("add_distributor")
        assert [record, pool] == annotator.called_with.get("add_formats")

        # Once the include settings are true and the marc organization code is set,
        # all methods are called.
        ConfigurationSetting.for_library_and_externalintegration(
            db.session, MARCExporter.INCLUDE_SUMMARY, db.default_library(), integration
        ).value = "true"

        ConfigurationSetting.for_library_and_externalintegration(
            db.session,
            MARCExporter.INCLUDE_SIMPLIFIED_GENRES,
            db.default_library(),
            integration,
        ).value = "true"

        ConfigurationSetting.for_library_and_externalintegration(
            db.session,
            MARCExporter.MARC_ORGANIZATION_CODE,
            db.default_library(),
            integration,
        ).value = "marc org"

        annotator = MockAnnotator(db.default_library())
        annotator.annotate_work_record(
            work, pool, edition, identifier, record, integration
        )

        assert [record, "marc org"] == annotator.called_with.get(
            "add_marc_organization_code"
        )
        assert [record, work] == annotator.called_with.get("add_summary")
        assert [record, work] == annotator.called_with.get("add_simplified_genres")
        assert [
            record,
            db.default_library(),
            identifier,
            integration,
        ] == annotator.called_with.get("add_web_client_urls")
        assert [record, pool] == annotator.called_with.get("add_distributor")
        assert [record, pool] == annotator.called_with.get("add_formats")

    def test_add_web_client_urls(self, db: DatabaseTransactionFixture):
        # Web client URLs can come from either the MARC export integration or
        # a library registry integration.

        identifier = db.identifier(foreign_id="identifier")
        lib_short_name = db.default_library().short_name

        # The URL for a work is constructed as:
        # - <cm-base>/<lib-short-name>/works/<qualified-identifier>
        work_link_template = "{cm_base}/{lib}/works/{qid}"
        # It is then encoded and the web client URL is constructed in this form:
        # - <web-client-base>/book/<encoded-work-url>
        client_url_template = "{client_base}/book/{work_link}"

        qualified_identifier = urllib.parse.quote(
            identifier.type + "/" + identifier.identifier, safe=""
        )
        cm_base_url = "http://test-circulation-manager"

        expected_work_link = work_link_template.format(
            cm_base=cm_base_url, lib=lib_short_name, qid=qualified_identifier
        )
        encoded_work_link = urllib.parse.quote(expected_work_link, safe="")

        client_base_1 = "http://web_catalog"
        client_base_2 = "http://another_web_catalog"
        expected_client_url_1 = client_url_template.format(
            client_base=client_base_1, work_link=encoded_work_link
        )
        expected_client_url_2 = client_url_template.format(
            client_base=client_base_2, work_link=encoded_work_link
        )

        # A few checks to ensure that our setup is useful.
        assert lib_short_name is not None
        assert len(lib_short_name) > 0
        assert client_base_1 != client_base_2
        assert expected_client_url_1 != expected_client_url_2
        assert expected_client_url_1.startswith(client_base_1)
        assert expected_client_url_2.startswith(client_base_2)

        ConfigurationSetting.sitewide(
            db.session, Configuration.BASE_URL_KEY
        ).value = cm_base_url

        annotator = LibraryAnnotator(db.default_library())

        # If no web catalog URLs are set for the library, nothing will be changed.
        record = Record()
        annotator.add_web_client_urls(record, db.default_library(), identifier)
        assert [] == record.get_fields("856")

        # Add a URL from a library registry.
        registry = db.external_integration(
            ExternalIntegration.OPDS_REGISTRATION,
            ExternalIntegration.DISCOVERY_GOAL,
            libraries=[db.default_library()],
        )
        ConfigurationSetting.for_library_and_externalintegration(
            db.session,
            Registration.LIBRARY_REGISTRATION_WEB_CLIENT,
            db.default_library(),
            registry,
        ).value = client_base_1

        record = Record()
        annotator.add_web_client_urls(record, db.default_library(), identifier)
        [field] = record.get_fields("856")
        assert ["4", "0"] == field.indicators
        assert expected_client_url_1 == field.get_subfields("u")[0]

        # Add a manually configured URL on a MARC export integration.
        integration = db.external_integration(
            ExternalIntegration.MARC_EXPORT,
            ExternalIntegration.CATALOG_GOAL,
            libraries=[db.default_library()],
        )

        ConfigurationSetting.for_library_and_externalintegration(
            db.session, MARCExporter.WEB_CLIENT_URL, db.default_library(), integration
        ).value = client_base_2

        record = Record()
        annotator.add_web_client_urls(
            record, db.default_library(), identifier, integration
        )
        [field1, field2] = record.get_fields("856")
        assert ["4", "0"] == field1.indicators
        assert expected_client_url_2 == field1.get_subfields("u")[0]

        assert ["4", "0"] == field2.indicators
        assert expected_client_url_1 == field2.get_subfields("u")[0]
