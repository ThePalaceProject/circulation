import datetime
import json
import uuid
from functools import partial
from typing import Any

from palace.manager.integration.license.opds.odl.api import OPDS2WithODLApi
from palace.manager.integration.license.opds.odl.importer import (
    importer_from_collection,
)
from palace.manager.opds.odl.info import Checkouts, LicenseInfo, LicenseStatus
from palace.manager.opds.odl.terms import Terms
from palace.manager.opds.opds2 import PublicationFeedNoValidation
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import OPDS2FilesFixture
from tests.fixtures.http import MockHttpClientFixture
from tests.fixtures.services import ServicesFixture


class TestOpdsImporter:
    def test_fetch_license_info(
        self,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
        http_client: MockHttpClientFixture,
    ) -> None:
        """Ensure that OPDS2WithODLImporter correctly retrieves license data from an OPDS2 feed."""

        def license_info_dict() -> dict[str, Any]:
            return LicenseInfo(
                identifier=str(uuid.uuid4()),
                status=LicenseStatus.available,
                checkouts=Checkouts(
                    available=10,
                ),
            ).model_dump(mode="json", exclude_none=True)

        collection = db.collection(
            protocol=OPDS2WithODLApi,
            settings=db.opds2_odl_settings(data_source="test collection"),
        )
        registry = services_fixture.services.integration_registry().license_providers()
        importer = importer_from_collection(collection, registry)

        fetch = partial(
            importer._fetch_license_document,
            "http://example.org/feed",
        )

        # Bad status code
        http_client.queue_response(400, content=b"Bad Request")
        assert fetch() is None
        assert len(http_client.requests) == 1
        assert http_client.requests.pop() == "http://example.org/feed"

        # 200 status - parses response body
        expiry = utc_now() + datetime.timedelta(days=1)
        license_helper = LicenseInfo(
            identifier=str(uuid.uuid4()),
            status=LicenseStatus.available,
            checkouts=Checkouts(
                available=10,
                left=4,
            ),
            terms=Terms(
                concurrency=11,
                expires=expiry,
            ),
        )
        http_client.queue_response(200, content=license_helper.model_dump_json())
        parsed = fetch()
        assert parsed.checkouts.available == 10
        assert parsed.checkouts.left == 4
        assert parsed.terms.concurrency == 11
        assert parsed.terms.expires == expiry
        assert parsed.status == LicenseStatus.available
        assert parsed.identifier == license_helper.identifier

        # 201 status - parses response body
        http_client.queue_response(201, content=license_helper.model_dump_json())
        parsed = fetch()
        assert parsed.checkouts.available == 10
        assert parsed.checkouts.left == 4
        assert parsed.terms.concurrency == 11
        assert parsed.terms.expires == expiry
        assert parsed.status == LicenseStatus.available
        assert parsed.identifier == license_helper.identifier

        # Bad data
        http_client.queue_response(201, content="{}")
        assert fetch() is None

        # No identifier
        license_dict = license_info_dict()
        license_dict.pop("identifier")
        http_client.queue_response(201, content=json.dumps(license_dict))
        assert fetch() is None

        # No status
        license_dict = license_info_dict()
        license_dict.pop("status")
        http_client.queue_response(201, content=json.dumps(license_dict))
        assert fetch() is None

        # Bad status
        license_dict = license_info_dict()
        license_dict["status"] = "bad"
        http_client.queue_response(201, content=json.dumps(license_dict))
        assert fetch() is None

        # No available
        license_dict = license_info_dict()
        license_dict["checkouts"].pop("available")
        http_client.queue_response(201, content=json.dumps(license_dict))
        assert fetch() is None

        # Format str
        license_dict = license_info_dict()
        license_dict["format"] = "single format"
        http_client.queue_response(201, content=json.dumps(license_dict))
        parsed = fetch()
        assert parsed is not None
        assert parsed.formats == ("single format",)

        # Format list
        license_dict = license_info_dict()
        license_dict["format"] = ["format1", "format2"]
        http_client.queue_response(201, content=json.dumps(license_dict))
        parsed = fetch()
        assert parsed is not None
        assert parsed.formats == ("format1", "format2")

    def test__extract_publications_from_feed(
        self,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
        http_client: MockHttpClientFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ) -> None:
        collection = db.collection(
            protocol=OPDS2WithODLApi,
            settings=db.opds2_odl_settings(data_source="test collection"),
        )
        registry = services_fixture.services.integration_registry().license_providers()
        importer = importer_from_collection(collection, registry)

        opds2_feed = json.loads(opds2_files_fixture.sample_text("feed.json"))
        opds2_feed["publications"] = [opds2_feed["publications"][0], {}]
        feed = PublicationFeedNoValidation.model_validate(opds2_feed)
        successful, failed = importer._extract_publications_from_feed(feed)

        # Only the first publication is valid, so it is the one returned
        assert len(successful) == 1
        [(identifier, bibliographic)] = list(successful.items())

        assert identifier.type == Identifier.ISBN
        assert identifier.identifier == "978-3-16-148410-0"
        assert bibliographic.primary_identifier_data == identifier

        # The second publication is invalid, so it is in the failed list
        assert len(failed) == 1
        [failed_publication] = failed
        assert failed_publication.error_message == "Error validating publication"
        assert failed_publication.identifier is None
        assert failed_publication.title is None
        assert failed_publication.publication_data == "{}"
