import json

import pytest
from pydantic import ValidationError

from palace.manager.integration.license.opds.opds2.importer import OPDS2Importer
from palace.manager.opds.opds2 import PublicationFeedNoValidation
from tests.fixtures.files import OPDS2FilesFixture


class TestOPDS2Importer:
    def test__get_publication(
        self,
        opds2_files_fixture: OPDS2FilesFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # Normally _get_publication just turns a publications dict into a Publication model
        opds2_feed = json.loads(opds2_files_fixture.sample_text("feed.json"))
        publication_dict = opds2_feed["publications"][0]
        publication = OPDS2Importer._get_publication(publication_dict)
        assert publication.metadata.identifier == "urn:isbn:978-3-16-148410-0"

        # However if there is a validation error, it adds a helpful log message
        # before raising the validation error
        with pytest.raises(
            ValidationError, match="3 validation errors for Publication"
        ):
            OPDS2Importer._get_publication({})

        assert "3 validation errors for Publication" in caplog.text

    def test_next_page(self, opds2_files_fixture: OPDS2FilesFixture) -> None:
        # No next links
        feed = PublicationFeedNoValidation.model_validate_json(
            opds2_files_fixture.sample_data("feed.json")
        )
        assert OPDS2Importer.next_page(feed) is None

        # Feed has next link
        feed = PublicationFeedNoValidation.model_validate_json(
            opds2_files_fixture.sample_data("feed2.json")
        )
        assert (
            OPDS2Importer.next_page(feed)
            == "http://bookshelf-feed-demo.us-east-1.elasticbeanstalk.com/v1/publications?page=2&limit=100"
        )
