"""Tests for BulkUpdateAudienceScript."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import patch

import pytest

from palace.manager.core.classifier import Classifier
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.scripts.classification import (
    BulkUpdateAudienceScript,
    CSVRow,
)
from palace.manager.sqlalchemy.model.classification import Classification, Subject
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from tests.fixtures.database import DatabaseTransactionFixture


class TestBulkUpdateAudienceScript:
    def test_arg_parser(self, db: DatabaseTransactionFixture) -> None:
        """Test that the argument parser is configured correctly."""
        parser = BulkUpdateAudienceScript.arg_parser(db.session)

        # Test with required argument
        parsed = parser.parse_args(["test.csv"])
        assert parsed.csv_file == Path("test.csv")
        assert parsed.batch_size == 50
        assert parsed.dry_run is False

        # Test with optional arguments
        parsed = parser.parse_args(["test.csv", "--batch-size", "100", "--dry-run"])
        assert parsed.csv_file == Path("test.csv")
        assert parsed.batch_size == 100
        assert parsed.dry_run is True

    @pytest.mark.parametrize(
        "encoding",
        [
            pytest.param("utf-8", id="without_bom"),
            pytest.param("utf-8-sig", id="with_bom"),
        ],
    )
    def test_parse_csv_valid(
        self, db: DatabaseTransactionFixture, tmp_path: Path, encoding: str
    ) -> None:
        """Test parsing a valid CSV file.

        The utf-8-sig encoding includes a BOM (byte order mark) at the start of
        the file, which is common in CSV files exported from Excel on Windows.

        We include an emoji to make sure that character encoding is handled
        correctly.
        """
        csv_content = """identifier_type,identifier,audience
ISBN,9780674368279,Young Adult
Overdrive ID,abc-123-def🔥,All Ages
"""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(csv_content, encoding=encoding)

        script = BulkUpdateAudienceScript(db.session)
        rows = script._parse_csv(csv_path)

        assert len(rows) == 2
        assert rows[0] == CSVRow(
            identifier_type="ISBN",
            identifier="9780674368279",
            audience="Young Adult",
            row_number=2,
        )
        assert rows[1] == CSVRow(
            identifier_type="Overdrive ID",
            identifier="abc-123-def🔥",
            audience="All Ages",
            row_number=3,
        )

    def test_parse_csv_missing_columns(
        self, db: DatabaseTransactionFixture, tmp_path: Path
    ) -> None:
        """Test that missing required columns raise an error."""
        csv_content = """identifier_type,identifier
ISBN,9780674368279
"""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(csv_content)

        script = BulkUpdateAudienceScript(db.session)
        with pytest.raises(
            PalaceValueError, match="missing required columns: audience"
        ):
            script._parse_csv(csv_path)

    def test_parse_csv_empty_file(
        self, db: DatabaseTransactionFixture, tmp_path: Path
    ) -> None:
        """Test that an empty CSV file raises an error."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("")

        script = BulkUpdateAudienceScript(db.session)
        with pytest.raises(PalaceValueError, match="CSV file is empty"):
            script._parse_csv(csv_path)

    def test_parse_csv_invalid_audience(
        self,
        db: DatabaseTransactionFixture,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test that invalid audience values are skipped with warnings."""
        csv_content = """identifier_type,identifier,audience
ISBN,9780674368279,InvalidAudience
ISBN,9780674368280,Children
"""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(csv_content)

        script = BulkUpdateAudienceScript(db.session)
        with caplog.at_level(logging.WARNING):
            rows = script._parse_csv(csv_path)

        # Only the valid row should be included
        assert len(rows) == 1
        assert rows[0].identifier == "9780674368280"
        assert "Invalid audience 'InvalidAudience'" in caplog.text

    def test_parse_csv_case_insensitive_audience(
        self, db: DatabaseTransactionFixture, tmp_path: Path
    ) -> None:
        """Test that audience matching is case-insensitive."""
        csv_content = """identifier_type,identifier,audience
ISBN,9780674368279,children
ISBN,9780674368280,YOUNG ADULT
ISBN,9780674368281,Adult
ISBN,9780674368282,all ages
"""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(csv_content)

        script = BulkUpdateAudienceScript(db.session)
        rows = script._parse_csv(csv_path)

        assert len(rows) == 4
        # Verify audiences are normalized to canonical form
        assert rows[0].audience == "Children"
        assert rows[1].audience == "Young Adult"
        assert rows[2].audience == "Adult"
        assert rows[3].audience == "All Ages"

    def test_parse_csv_blank_audience_skipped(
        self, db: DatabaseTransactionFixture, tmp_path: Path
    ) -> None:
        """Test that rows with blank audience are silently skipped."""
        csv_content = """identifier_type,identifier,audience
ISBN,9780674368279,
ISBN,9780674368280,Young Adult
ISBN,9780674368281,
"""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(csv_content)

        script = BulkUpdateAudienceScript(db.session)
        rows = script._parse_csv(csv_path)

        # Only the row with a non-blank audience should be included
        assert len(rows) == 1
        assert rows[0].identifier == "9780674368280"
        assert rows[0].audience == "Young Adult"

    def test_parse_csv_missing_identifier(
        self,
        db: DatabaseTransactionFixture,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test that rows with missing identifiers are skipped."""
        csv_content = """identifier_type,identifier,audience
ISBN,,Young Adult
,9780674368279,Young Adult
ISBN,9780674368280,Children
"""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(csv_content)

        script = BulkUpdateAudienceScript(db.session)
        with caplog.at_level(logging.WARNING):
            rows = script._parse_csv(csv_path)

        # Only the valid row should be included
        assert len(rows) == 1
        assert rows[0].identifier == "9780674368280"
        assert "Missing identifier_type or identifier" in caplog.text

    def test_process_single_row_identifier_not_found(
        self, db: DatabaseTransactionFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that missing identifiers are handled gracefully."""
        row = CSVRow(
            identifier_type=Identifier.ISBN,
            identifier="9999999999999",
            audience="Young Adult",
            row_number=2,
        )

        script = BulkUpdateAudienceScript(db.session)
        with caplog.at_level(logging.WARNING):
            result = script._process_single_row(row, dry_run=False)

        assert result is False
        assert "Identifier not found" in caplog.text

    def test_process_single_row_no_work(
        self, db: DatabaseTransactionFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that identifiers without works are handled gracefully."""
        # Create an identifier without a work
        identifier = db.identifier(identifier_type=Identifier.ISBN)

        row = CSVRow(
            identifier_type=Identifier.ISBN,
            identifier=identifier.identifier,
            audience="Young Adult",
            row_number=2,
        )

        script = BulkUpdateAudienceScript(db.session)
        with caplog.at_level(logging.WARNING):
            result = script._process_single_row(row, dry_run=False)

        assert result is False
        assert "No work found for identifier" in caplog.text

    def test_process_single_row_dry_run(
        self, db: DatabaseTransactionFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test dry run mode doesn't make changes."""
        work = db.work(audience=Classifier.AUDIENCE_ADULT, with_license_pool=True)
        identifier = work.presentation_edition.primary_identifier

        row = CSVRow(
            identifier_type=identifier.type,
            identifier=identifier.identifier,
            audience="Young Adult",
            row_number=2,
        )

        script = BulkUpdateAudienceScript(db.session)
        with caplog.at_level(logging.INFO):
            result = script._process_single_row(row, dry_run=True)

        assert result is True
        assert "Would update" in caplog.text
        # Verify the work audience hasn't changed
        assert work.audience == Classifier.AUDIENCE_ADULT

    def test_process_single_row_success(
        self, db: DatabaseTransactionFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test successful audience update."""
        work = db.work(audience=Classifier.AUDIENCE_ADULT, with_license_pool=True)
        identifier = work.presentation_edition.primary_identifier

        row = CSVRow(
            identifier_type=identifier.type,
            identifier=identifier.identifier,
            audience="Young Adult",
            row_number=2,
        )

        script = BulkUpdateAudienceScript(db.session)

        # Mock calculate_presentation to avoid side effects
        with caplog.at_level(logging.INFO), patch.object(
            work, "calculate_presentation"
        ):
            result = script._process_single_row(row, dry_run=False)

        assert result is True
        assert "Updated" in caplog.text

        # Verify a staff classification was created
        staff_data_source = DataSource.lookup(db.session, DataSource.LIBRARY_STAFF)
        classifications = (
            db.session.query(Classification)
            .join(Subject)
            .filter(
                Classification.identifier == identifier,
                Classification.data_source == staff_data_source,
                Subject.type == Subject.FREEFORM_AUDIENCE,
            )
            .all()
        )
        assert len(classifications) == 1
        assert classifications[0].subject.identifier == "Young Adult"

    def test_update_audience_deletes_existing_classifications(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """Test that existing FREEFORM_AUDIENCE staff classifications are deleted."""
        work = db.work(audience=Classifier.AUDIENCE_ADULT, with_license_pool=True)
        identifier = work.presentation_edition.primary_identifier
        staff_data_source = DataSource.lookup(db.session, DataSource.LIBRARY_STAFF)

        # Create an existing staff classification
        identifier.classify(
            data_source=staff_data_source,
            subject_type=Subject.FREEFORM_AUDIENCE,
            subject_identifier="Adult",
            weight=1000,
        )
        db.session.commit()

        # Verify it exists
        initial_count = (
            db.session.query(Classification)
            .join(Subject)
            .filter(
                Classification.identifier == identifier,
                Classification.data_source == staff_data_source,
                Subject.type == Subject.FREEFORM_AUDIENCE,
            )
            .count()
        )
        assert initial_count == 1

        script = BulkUpdateAudienceScript(db.session)

        # Mock calculate_presentation to avoid side effects
        with patch.object(work, "calculate_presentation"):
            script._update_audience(work, "Young Adult")

        # Verify the old classification was deleted and new one created
        classifications = (
            db.session.query(Classification)
            .join(Subject)
            .filter(
                Classification.identifier == identifier,
                Classification.data_source == staff_data_source,
                Subject.type == Subject.FREEFORM_AUDIENCE,
            )
            .all()
        )
        assert len(classifications) == 1
        assert classifications[0].subject.identifier == "Young Adult"

    def test_update_audience_already_correct(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """Test that no changes are made when classification already matches."""
        work = db.work(audience=Classifier.AUDIENCE_ADULT, with_license_pool=True)
        identifier = work.presentation_edition.primary_identifier
        staff_data_source = DataSource.lookup(db.session, DataSource.LIBRARY_STAFF)

        # Create an existing staff classification with the target audience
        identifier.classify(
            data_source=staff_data_source,
            subject_type=Subject.FREEFORM_AUDIENCE,
            subject_identifier="Young Adult",
            weight=1000,
        )
        db.session.commit()

        script = BulkUpdateAudienceScript(db.session)

        # Should return False since classification already matches
        result = script._update_audience(work, "Young Adult")

        assert result is False

    def test_process_single_row_no_presentation_edition(
        self, db: DatabaseTransactionFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test handling of work without a presentation edition."""
        work = db.work(audience=Classifier.AUDIENCE_ADULT, with_license_pool=True)
        identifier = work.presentation_edition.primary_identifier

        # Remove the presentation edition from the work
        work.presentation_edition = None

        row = CSVRow(
            identifier_type=identifier.type,
            identifier=identifier.identifier,
            audience="Young Adult",
            row_number=2,
        )

        script = BulkUpdateAudienceScript(db.session)
        with caplog.at_level(logging.ERROR):
            result = script._process_single_row(row, dry_run=False)

        assert result is False
        assert "No presentation edition for work" in caplog.text

    def test_process_single_row_already_correct_classification(
        self, db: DatabaseTransactionFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test logging when work already has the correct classification."""
        work = db.work(audience=Classifier.AUDIENCE_ADULT, with_license_pool=True)
        identifier = work.presentation_edition.primary_identifier
        staff_data_source = DataSource.lookup(db.session, DataSource.LIBRARY_STAFF)

        # Create an existing staff classification with the target audience
        identifier.classify(
            data_source=staff_data_source,
            subject_type=Subject.FREEFORM_AUDIENCE,
            subject_identifier="Adult",
            weight=1000,
        )
        db.session.commit()

        row = CSVRow(
            identifier_type=identifier.type,
            identifier=identifier.identifier,
            audience="Adult",
            row_number=2,
        )

        script = BulkUpdateAudienceScript(db.session)
        with caplog.at_level(logging.INFO):
            result = script._process_single_row(row, dry_run=False)

        assert result is True
        assert "Already correct" in caplog.text

    def test_process_rows_with_skipped_rows(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """Test that skipped rows are counted correctly in processing results."""
        # Create one valid work and one identifier without a work
        work = db.work(audience=Classifier.AUDIENCE_ADULT, with_license_pool=True)
        valid_identifier = work.presentation_edition.primary_identifier
        orphan_identifier = db.identifier(identifier_type=Identifier.ISBN)

        rows = [
            CSVRow(
                identifier_type=orphan_identifier.type,
                identifier=orphan_identifier.identifier,
                audience="Young Adult",
                row_number=2,
            ),
            CSVRow(
                identifier_type=valid_identifier.type,
                identifier=valid_identifier.identifier,
                audience="Young Adult",
                row_number=3,
            ),
        ]

        script = BulkUpdateAudienceScript(db.session)

        with patch.object(work, "calculate_presentation"):
            result = script._process_rows(rows, batch_size=50, dry_run=False)

        assert result.total == 2
        assert result.updated == 1
        assert result.skipped == 1
        assert result.errors == 0

    def test_process_rows_batching(
        self, db: DatabaseTransactionFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that commits happen at batch boundaries."""
        # Create 5 works
        works = []
        rows = []
        for i in range(5):
            work = db.work(audience=Classifier.AUDIENCE_ADULT, with_license_pool=True)
            works.append(work)
            identifier = work.presentation_edition.primary_identifier
            rows.append(
                CSVRow(
                    identifier_type=identifier.type,
                    identifier=identifier.identifier,
                    audience="Young Adult",
                    row_number=i + 2,
                )
            )

        script = BulkUpdateAudienceScript(db.session)

        # Mock calculate_presentation for all works
        with (
            caplog.at_level(logging.INFO),
            patch("palace.manager.sqlalchemy.model.work.Work.calculate_presentation"),
        ):
            # Use batch_size=2 to trigger progress logs
            result = script._process_rows(rows, batch_size=2, dry_run=False)

        assert result.total == 5
        assert result.updated == 5
        assert result.skipped == 0
        assert result.errors == 0

        # Check progress was logged
        assert "Progress: 2/5" in caplog.text
        assert "Progress: 4/5" in caplog.text

    def test_do_run_file_not_found(
        self, db: DatabaseTransactionFixture, tmp_path: Path
    ) -> None:
        """Test that missing CSV file raises an error."""
        csv_path = tmp_path / "nonexistent.csv"

        script = BulkUpdateAudienceScript(db.session, cmd_args=[str(csv_path)])

        with pytest.raises(PalaceValueError, match="CSV file not found"):
            script.do_run()

    def test_do_run_empty_csv(
        self,
        db: DatabaseTransactionFixture,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test handling of CSV with only headers (no data rows)."""
        csv_content = """identifier_type,identifier,audience
"""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(csv_content)

        script = BulkUpdateAudienceScript(db.session, cmd_args=[str(csv_path)])
        with caplog.at_level(logging.WARNING):
            script.do_run()

        assert "No valid rows found" in caplog.text

    def test_do_run_full_integration(
        self,
        db: DatabaseTransactionFixture,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test complete script execution with valid data."""
        # Create works with different audiences
        work1 = db.work(audience=Classifier.AUDIENCE_ADULT, with_license_pool=True)
        work2 = db.work(audience=Classifier.AUDIENCE_CHILDREN, with_license_pool=True)
        id1 = work1.presentation_edition.primary_identifier
        id2 = work2.presentation_edition.primary_identifier

        csv_content = f"""identifier_type,identifier,audience
{id1.type},{id1.identifier},Young Adult
{id2.type},{id2.identifier},All Ages
"""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(csv_content)

        script = BulkUpdateAudienceScript(
            db.session, cmd_args=[str(csv_path), "--batch-size", "1"]
        )

        # Mock calculate_presentation
        with (
            caplog.at_level(logging.INFO),
            patch("palace.manager.sqlalchemy.model.work.Work.calculate_presentation"),
        ):
            script.do_run()

        assert "Completed: total=2, updated=2, skipped=0, errors=0" in caplog.text

    def test_do_run_dry_run_mode(
        self,
        db: DatabaseTransactionFixture,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test dry run mode through do_run."""
        work = db.work(audience=Classifier.AUDIENCE_ADULT, with_license_pool=True)
        identifier = work.presentation_edition.primary_identifier

        csv_content = f"""identifier_type,identifier,audience
{identifier.type},{identifier.identifier},Young Adult
"""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(csv_content)

        script = BulkUpdateAudienceScript(
            db.session, cmd_args=[str(csv_path), "--dry-run"]
        )
        with caplog.at_level(logging.INFO):
            script.do_run()

        assert "dry_run=True" in caplog.text
        assert "Would update" in caplog.text
        # Verify no changes were made
        db.session.refresh(work)
        assert work.audience == Classifier.AUDIENCE_ADULT

    def test_process_rows_error_handling(
        self, db: DatabaseTransactionFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that errors in processing individual rows are handled."""
        work = db.work(audience=Classifier.AUDIENCE_ADULT, with_license_pool=True)
        identifier = work.presentation_edition.primary_identifier

        row = CSVRow(
            identifier_type=identifier.type,
            identifier=identifier.identifier,
            audience="Young Adult",
            row_number=2,
        )

        script = BulkUpdateAudienceScript(db.session)

        # Patch _process_single_row to raise an exception
        with patch.object(
            script, "_process_single_row", side_effect=Exception("Test error")
        ):
            result = script._process_rows([row], batch_size=50, dry_run=False)

        assert result.errors == 1
        assert result.updated == 0
        assert "Error processing - Test error" in caplog.text
