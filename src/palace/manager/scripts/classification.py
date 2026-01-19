"""Script to bulk update audience classifications from a CSV file."""

from __future__ import annotations

import argparse
import csv
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

from sqlalchemy.orm import Session

from palace.manager.core.classifier import Classifier
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.policy.presentation import PresentationCalculationPolicy
from palace.manager.scripts.base import Script
from palace.manager.sqlalchemy.model.classification import Classification, Subject
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier

if TYPE_CHECKING:
    from palace.manager.sqlalchemy.model.work import Work


class CSVRow(NamedTuple):
    """Represents a row from the input CSV file."""

    identifier_type: str
    identifier: str
    old_audience: str
    new_audience: str
    row_number: int


class ProcessingResult(NamedTuple):
    """Summary of script execution."""

    total: int
    updated: int
    skipped: int
    errors: int


class BulkUpdateAudienceScript(Script):
    """Update audience classifications for works in bulk from a CSV file.

    The CSV file must have columns: identifier_type, identifier, old_audience, new_audience

    The script will:
    - Load each identifier from the CSV
    - Verify the work's current audience matches old_audience
    - Delete existing FREEFORM_AUDIENCE staff classifications
    - Create a new classification with the new audience
    - Recalculate the work's presentation with classification updates
    """

    # Weight for staff-created classifications, matching WorkController.STAFF_WEIGHT
    STAFF_WEIGHT = 1000

    # Valid audience values from Classifier
    VALID_AUDIENCES = Classifier.AUDIENCES

    def __init__(
        self,
        _db: Session | None = None,
        cmd_args: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the script.

        :param _db: Database session (optional, for testing).
        :param cmd_args: Command line arguments (optional, for testing).
        """
        super().__init__(_db, **kwargs)
        self._cmd_args = cmd_args

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        """Create the argument parser for this script.

        :param _db: Database session (unused but required by base class).
        :return: Configured argument parser.
        """
        parser = argparse.ArgumentParser(
            description="Bulk update audience classifications from a CSV file."
        )
        parser.add_argument(
            "csv_file",
            type=Path,
            help="Path to CSV file with columns: identifier_type, identifier, old_audience, new_audience",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=50,
            help="Number of items to process before committing (default: 50)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Parse and validate the CSV without making changes",
        )
        return parser

    def do_run(self) -> None:
        """Execute the script."""
        parsed = self.parse_command_line(self._db, self._cmd_args)
        csv_path: Path = parsed.csv_file
        batch_size: int = parsed.batch_size
        dry_run: bool = parsed.dry_run

        if not csv_path.exists():
            raise PalaceValueError(f"CSV file not found: {csv_path}")

        rows = self._parse_csv(csv_path)
        if not rows:
            self.log.warning("No valid rows found in CSV file")
            return

        self.log.info(
            f"Processing {len(rows)} rows (batch_size={batch_size}, dry_run={dry_run})"
        )

        result = self._process_rows(rows, batch_size, dry_run)

        self.log.info(
            f"Completed: total={result.total}, updated={result.updated}, "
            f"skipped={result.skipped}, errors={result.errors}"
        )

    def _parse_csv(self, csv_path: Path) -> list[CSVRow]:
        """Parse the CSV file and validate its contents.

        :param csv_path: Path to the CSV file.
        :return: List of validated CSVRow objects.
        :raises PalaceValueError: If the CSV is missing required columns.
        """
        required_columns = {
            "identifier_type",
            "identifier",
            "old_audience",
            "new_audience",
        }
        rows: list[CSVRow] = []

        with csv_path.open(newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)

            if reader.fieldnames is None:
                raise PalaceValueError("CSV file is empty or has no headers")

            missing_columns = required_columns - set(reader.fieldnames)
            if missing_columns:
                raise PalaceValueError(
                    f"CSV file missing required columns: {', '.join(sorted(missing_columns))}"
                )

            for row_num, row in enumerate(
                reader, start=2
            ):  # Start at 2 (header is row 1)
                identifier_type = row["identifier_type"].strip()
                identifier = row["identifier"].strip()
                old_audience = row["old_audience"].strip()
                new_audience = row["new_audience"].strip()

                # Validate audiences
                if old_audience not in self.VALID_AUDIENCES:
                    self.log.warning(
                        f"Row {row_num}: Invalid old_audience '{old_audience}'. "
                        f"Valid values: {', '.join(sorted(self.VALID_AUDIENCES))}"
                    )
                    continue

                if new_audience not in self.VALID_AUDIENCES:
                    self.log.warning(
                        f"Row {row_num}: Invalid new_audience '{new_audience}'. "
                        f"Valid values: {', '.join(sorted(self.VALID_AUDIENCES))}"
                    )
                    continue

                if not identifier_type or not identifier:
                    self.log.warning(
                        f"Row {row_num}: Missing identifier_type or identifier"
                    )
                    continue

                rows.append(
                    CSVRow(
                        identifier_type=identifier_type,
                        identifier=identifier,
                        old_audience=old_audience,
                        new_audience=new_audience,
                        row_number=row_num,
                    )
                )

        return rows

    def _process_rows(
        self, rows: list[CSVRow], batch_size: int, dry_run: bool
    ) -> ProcessingResult:
        """Process all rows from the CSV file.

        :param rows: List of CSVRow objects to process.
        :param batch_size: Number of rows to process before committing.
        :param dry_run: If True, validate without making changes.
        :return: Summary of processing results.
        """
        total = len(rows)
        updated = 0
        skipped = 0
        errors = 0

        for i, row in enumerate(rows, start=1):
            try:
                if self._process_single_row(row, dry_run):
                    updated += 1
                else:
                    skipped += 1
            except Exception as e:
                self.log.error(f"Row {row.row_number}: Error processing - {e}")
                errors += 1

            # Commit at batch boundaries
            if i % batch_size == 0:
                if not dry_run:
                    self._db.commit()
                self.log.info(
                    f"Progress: {i}/{total} processed "
                    f"(updated={updated}, skipped={skipped}, errors={errors})"
                )

        # Final commit for remaining rows
        if not dry_run:
            self._db.commit()

        return ProcessingResult(
            total=total, updated=updated, skipped=skipped, errors=errors
        )

    def _process_single_row(self, row: CSVRow, dry_run: bool) -> bool:
        """Process a single CSV row.

        :param row: The CSV row to process.
        :param dry_run: If True, validate without making changes.
        :return: True if the work was updated, False if skipped.
        """
        # Look up the identifier
        identifier, _ = Identifier.for_foreign_id(
            self._db, row.identifier_type, row.identifier, autocreate=False
        )

        if identifier is None:
            self.log.warning(
                f"Row {row.row_number}: Identifier not found: "
                f"{row.identifier_type}/{row.identifier}"
            )
            return False

        # Get the work associated with this identifier
        work: Work | None = identifier.work
        if work is None:
            self.log.warning(
                f"Row {row.row_number}: No work found for identifier: "
                f"{row.identifier_type}/{row.identifier}"
            )
            return False

        # Validate current audience matches expected old_audience
        if work.audience != row.old_audience:
            self.log.warning(
                f"Row {row.row_number}: Audience mismatch for "
                f"{row.identifier_type}/{row.identifier}. "
                f"Expected '{row.old_audience}', found '{work.audience}'"
            )
            return False

        # Verify work has a presentation edition
        if work.presentation_edition is None:
            self.log.error(
                f"Row {row.row_number}: No presentation edition for work: "
                f"{row.identifier_type}/{row.identifier}"
            )
            return False

        if dry_run:
            self.log.info(
                f"Row {row.row_number}: Would update "
                f"{row.identifier_type}/{row.identifier} "
                f"from '{row.old_audience}' to '{row.new_audience}'"
            )
            return True

        # Update the audience classification
        changed = self._update_audience(work, row.new_audience)

        if changed:
            self.log.info(
                f"Row {row.row_number}: Updated {row.identifier_type}/{row.identifier} "
                f"from '{row.old_audience}' to '{row.new_audience}'"
            )
        else:
            self.log.info(
                f"Row {row.row_number}: Already correct {row.identifier_type}/{row.identifier} "
                f"(audience: '{row.new_audience}')"
            )
        return True

    def _update_audience(self, work: Work, new_audience: str) -> bool:
        """Update the audience classification for a work.

        This mirrors the logic in WorkController.edit_classifications.

        :param work: The work to update.
        :param new_audience: The new audience value.
        :return: True if a change was made, False if already correct.
        """
        staff_data_source = DataSource.lookup(
            self._db, DataSource.LIBRARY_STAFF, autocreate=True
        )

        # presentation_edition is validated in _process_single_row before calling this method
        presentation_edition = work.presentation_edition
        assert presentation_edition is not None
        primary_identifier = presentation_edition.primary_identifier

        # Get existing staff FREEFORM_AUDIENCE classifications for this identifier
        existing_audience_classifications = (
            self._db.query(Classification)
            .join(Subject)
            .filter(
                Classification.identifier == primary_identifier,
                Classification.data_source == staff_data_source,
                Subject.type == Subject.FREEFORM_AUDIENCE,
            )
            .all()
        )

        # Check if we already have the correct classification
        if len(existing_audience_classifications) == 1:
            existing = existing_audience_classifications[0]
            if existing.subject.identifier == new_audience:
                # Already has the correct classification, nothing to do
                return False

        # Delete existing FREEFORM_AUDIENCE staff classifications
        for classification in existing_audience_classifications:
            self._db.delete(classification)

        # Create a new classification with the new audience
        primary_identifier.classify(
            data_source=staff_data_source,
            subject_type=Subject.FREEFORM_AUDIENCE,
            subject_identifier=new_audience,
            weight=self.STAFF_WEIGHT,
        )

        # Recalculate presentation with classification updates
        policy = PresentationCalculationPolicy(
            classify=True,
            update_search_index=True,
        )
        work.calculate_presentation(policy=policy)
        return True
