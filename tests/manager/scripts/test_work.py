from __future__ import annotations

from io import StringIO
from unittest.mock import patch

import pytest

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.scripts.work import (
    ReclassifyWorksForUncheckedSubjectsScript,
    WorkProcessingScript,
)
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from tests.fixtures.database import DatabaseTransactionFixture


class TestWorkProcessingScript:
    def test_make_query(self, db: DatabaseTransactionFixture):
        # Create two Gutenberg works and one Overdrive work
        g1 = db.work(with_license_pool=True, with_open_access_download=True)
        g2 = db.work(with_license_pool=True, with_open_access_download=True)

        overdrive_edition = db.edition(
            data_source_name=DataSource.OVERDRIVE,
            identifier_type=Identifier.OVERDRIVE_ID,
            with_license_pool=True,
        )[0]
        overdrive_work = db.work(presentation_edition=overdrive_edition)

        ugi_edition = db.edition(
            data_source_name=DataSource.UNGLUE_IT,
            identifier_type=Identifier.URI,
            with_license_pool=True,
        )[0]
        unglue_it = db.work(presentation_edition=ugi_edition)

        se_edition = db.edition(
            data_source_name=DataSource.STANDARD_EBOOKS,
            identifier_type=Identifier.URI,
            with_license_pool=True,
        )[0]
        standard_ebooks = db.work(presentation_edition=se_edition)

        everything = WorkProcessingScript.make_query(db.session, None, None, None)
        assert {g1, g2, overdrive_work, unglue_it, standard_ebooks} == set(
            everything.all()
        )

        all_gutenberg = WorkProcessingScript.make_query(
            db.session, Identifier.GUTENBERG_ID, [], None
        )
        assert {g1, g2} == set(all_gutenberg.all())

        one_gutenberg = WorkProcessingScript.make_query(
            db.session, Identifier.GUTENBERG_ID, [g1.license_pools[0].identifier], None
        )
        assert [g1] == one_gutenberg.all()

        one_standard_ebook = WorkProcessingScript.make_query(
            db.session, Identifier.URI, [], DataSource.STANDARD_EBOOKS
        )
        assert [standard_ebooks] == one_standard_ebook.all()

    def test_init_no_arguments(self, db: DatabaseTransactionFixture):
        # Create some works to ensure the query isn't empty
        db.work(with_license_pool=True)
        db.work(with_license_pool=True)

        # Initialize with no command-line arguments
        script = WorkProcessingScript(_db=db.session, cmd_args=[], stdin=StringIO())

        # Check that the script is properly initialized
        assert script.identifier_type is None
        assert script.data_source is None
        assert script.identifiers == []
        assert script.batch_size == 10  # default value
        assert script.force is False  # default value
        assert script.query is not None

    def test_init_with_identifier_type(self, db: DatabaseTransactionFixture):
        # Create a Gutenberg work
        work = db.work(with_license_pool=True, with_open_access_download=True)

        # Initialize with identifier type but no specific identifiers
        script = WorkProcessingScript(
            _db=db.session,
            cmd_args=["--identifier-type", Identifier.GUTENBERG_ID],
            stdin=StringIO(),
        )

        # Check that identifier_type is set
        assert script.identifier_type == Identifier.GUTENBERG_ID
        assert script.data_source is None
        assert script.identifiers == []
        # Query should be filtered to Gutenberg works
        assert work in script.query.all()

    def test_init_with_identifier_strings_no_type_raises_error(
        self, db: DatabaseTransactionFixture
    ):
        # Create an identifier
        identifier = db.identifier()

        # Providing identifier strings without identifier type should raise an error
        with pytest.raises(PalaceValueError) as excinfo:
            WorkProcessingScript(
                _db=db.session, cmd_args=[str(identifier.id)], stdin=StringIO()
            )

        assert "No identifier type specified" in str(excinfo.value)
        assert '--identifier-type="Database ID"' in str(excinfo.value)

    def test_init_with_identifier_type_and_strings(
        self, db: DatabaseTransactionFixture
    ):
        # Create a work with a specific identifier
        work = db.work(with_license_pool=True)
        identifier = work.license_pools[0].identifier

        # Initialize with both identifier type and identifier strings
        script = WorkProcessingScript(
            _db=db.session,
            cmd_args=[
                "--identifier-type",
                "Database ID",
                str(identifier.id),
            ],
            stdin=StringIO(),
        )

        # Check that identifiers are populated
        assert script.identifier_type == "Database ID"
        assert len(script.identifiers) == 1
        assert script.identifiers[0] == identifier
        # The query is created with the identifiers, so it should be filtered
        # We can't easily check the exact query result without implementing paginate_query
        # but we can verify the query was constructed
        assert script.query is not None

    def test_init_with_custom_batch_size_and_force(
        self, db: DatabaseTransactionFixture
    ):
        # Initialize with custom batch_size and force parameters
        script = WorkProcessingScript(
            _db=db.session, cmd_args=[], batch_size=50, force=True, stdin=StringIO()
        )

        # Check that custom parameters are set
        assert script.batch_size == 50
        assert script.force is True

    def test_init_with_data_source(self, db: DatabaseTransactionFixture):
        # Create an Overdrive work
        overdrive_edition = db.edition(
            data_source_name=DataSource.OVERDRIVE,
            identifier_type=Identifier.OVERDRIVE_ID,
            with_license_pool=True,
        )[0]
        overdrive_work = db.work(presentation_edition=overdrive_edition)

        # Initialize with identifier type and data source
        script = WorkProcessingScript(
            _db=db.session,
            cmd_args=[
                "--identifier-type",
                Identifier.OVERDRIVE_ID,
                "--identifier-data-source",
                DataSource.OVERDRIVE,
            ],
            stdin=StringIO(),
        )

        # Check that data_source is set
        assert script.identifier_type == Identifier.OVERDRIVE_ID
        assert script.data_source == DataSource.OVERDRIVE
        # Query should be filtered to Overdrive works
        assert overdrive_work in script.query.all()


class TestReclassifyWorksForUncheckedSubjectsScript:
    def test_run(self, db: DatabaseTransactionFixture):
        """Make sure the underlying celery task is triggered."""

        with patch("palace.manager.scripts.work.classify_unchecked_subjects") as task:

            ReclassifyWorksForUncheckedSubjectsScript().run()
            assert task.delay.call_count == 1
