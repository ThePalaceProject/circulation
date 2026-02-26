import argparse
import csv
from collections.abc import Sequence
from enum import Enum, auto
from typing import cast

from sqlalchemy import select
from sqlalchemy.orm import Session

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.scripts.base import Script, _normalize_cmd_args
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.library import Library


class SuppressResult(Enum):
    NEWLY_SUPPRESSED = auto()
    ALREADY_SUPPRESSED = auto()
    NOT_FOUND = auto()


class SuppressWorkForLibraryScript(Script):
    """Suppress works from a library by identifier"""

    BY_DATABASE_ID = "Database ID"

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        library_name_list = sorted(
            str(l.short_name) for l in _db.scalars(select(Library))
        )
        library_names = '"' + '", "'.join(library_name_list) + '"'
        parser.add_argument(
            "-l",
            "--library",
            help=f"Short name of the library. Libraries on this system: {library_names}.",
            required=True,
            metavar="SHORT_NAME",
        )
        parser.add_argument(
            "-t",
            "--identifier-type",
            help="Identifier type (default: ISBN). "
            f'To name identifiers by their database ID, use --identifier-type="{cls.BY_DATABASE_ID}".',
            default="ISBN",
        )

        id_group = parser.add_mutually_exclusive_group(required=True)
        id_group.add_argument(
            "-i",
            "--identifier",
            help="The identifier to suppress.",
        )
        id_group.add_argument(
            "-f",
            "--file",
            help='Path to a CSV file with "identifier" and optional "identifier_type" columns.',
            metavar="FILE_PATH",
        )

        parser.add_argument(
            "--dry-run",
            help="Report what would be suppressed without making any changes.",
            action="store_true",
        )
        return parser

    @classmethod
    def parse_command_line(
        cls,
        _db: Session,
        cmd_args: Sequence[str | None] | None = None,
    ) -> argparse.Namespace:
        parser = cls.arg_parser(_db)
        return parser.parse_known_args(_normalize_cmd_args(cmd_args))[0]

    def load_library(self, library_short_name: str) -> Library:
        library_short_name = library_short_name.strip()
        library = cast(
            Library | None,
            self._db.scalars(
                select(Library).where(Library.short_name == library_short_name)
            ).one_or_none(),
        )
        if library is None:
            raise PalaceValueError(f"Unknown library: {library_short_name}")
        return library

    def load_identifier(self, identifier_type: str, identifier: str) -> Identifier:
        query = select(Identifier)
        identifier_type = identifier_type.strip()
        identifier = identifier.strip()
        if identifier_type == self.BY_DATABASE_ID:
            query = query.where(Identifier.id == int(identifier))
        else:
            query = query.where(Identifier.type == identifier_type).where(
                Identifier.identifier == identifier
            )

        identifier_obj = cast(
            Identifier | None, self._db.scalars(query).unique().one_or_none()
        )
        if identifier_obj is None:
            raise PalaceValueError(
                f"Unknown identifier: {identifier_type}/{identifier}"
            )

        return identifier_obj

    def load_identifiers_from_file(
        self, file_path: str, default_identifier_type: str
    ) -> list[tuple[str, str]]:
        """Load (identifier_type, identifier) pairs from a CSV file.

        The CSV must have an "identifier" column. The "identifier_type" column
        is optional; rows missing a type value fall back to default_identifier_type.
        """
        identifiers: list[tuple[str, str]] = []
        try:
            with open(file_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames or "identifier" not in reader.fieldnames:
                    raise PalaceValueError(
                        f'CSV file must contain an "identifier" column. '
                        f"Found columns: {reader.fieldnames}"
                    )
                has_type_column = "identifier_type" in reader.fieldnames
                for row in reader:
                    identifier = row["identifier"].strip()
                    if not identifier:
                        continue
                    if has_type_column and row["identifier_type"].strip():
                        id_type = row["identifier_type"].strip()
                    else:
                        id_type = default_identifier_type
                    identifiers.append((id_type, identifier))
        except FileNotFoundError:
            raise PalaceValueError(f"CSV file not found: {file_path}")
        return identifiers

    def suppress_work(
        self,
        library: Library,
        identifier: Identifier,
        dry_run: bool = False,
    ) -> SuppressResult:
        work = identifier.work
        if not work:
            self.log.warning(f"No work found for {identifier}")
            return SuppressResult.NOT_FOUND

        if library in work.suppressed_for:
            return SuppressResult.ALREADY_SUPPRESSED

        if not dry_run:
            work.suppressed_for.append(library)

        self.log.info(
            f"{'[DRY RUN] Would suppress' if dry_run else 'Suppressing'} "
            f"{identifier.type}/{identifier.identifier} (work id: {work.id}) "
            f"for {library.short_name}."
        )
        return SuppressResult.NEWLY_SUPPRESSED

    def do_run(self, cmd_args: list[str] | None = None) -> None:
        parsed = self.parse_command_line(self._db, cmd_args=cmd_args)
        library = self.load_library(parsed.library)
        dry_run: bool = parsed.dry_run

        if parsed.file:
            pairs = self.load_identifiers_from_file(parsed.file, parsed.identifier_type)
        else:
            pairs = [(parsed.identifier_type, parsed.identifier)]

        results: dict[tuple[str, str], SuppressResult] = {}
        try:
            for id_type, id_value in pairs:
                try:
                    identifier = self.load_identifier(id_type, id_value)
                    result = self.suppress_work(library, identifier, dry_run=dry_run)
                except PalaceValueError:
                    result = SuppressResult.NOT_FOUND
                results[(id_type, id_value)] = result

            if not dry_run:
                self._db.commit()
        except Exception:
            self._db.rollback()
            raise

        self._print_results(results, dry_run)

    def _print_results(
        self,
        results: dict[tuple[str, str], SuppressResult],
        dry_run: bool,
    ) -> None:
        newly_suppressed = [
            k for k, v in results.items() if v == SuppressResult.NEWLY_SUPPRESSED
        ]
        already_suppressed = [
            k for k, v in results.items() if v == SuppressResult.ALREADY_SUPPRESSED
        ]
        not_found = [k for k, v in results.items() if v == SuppressResult.NOT_FOUND]

        prefix = "[DRY RUN] " if dry_run else ""
        suppress_label = "Would suppress" if dry_run else "Newly suppressed"

        col = 20
        print(f"\n{prefix}Suppression Results Summary:")
        print(f"  {suppress_label + ':':<{col}} {len(newly_suppressed)}")
        print(f"  {'Already suppressed:':<{col}} {len(already_suppressed)}")
        print(f"  {'Not found:':<{col}} {len(not_found)}")

        print(f"\n{prefix}Details:")
        status_map = {
            SuppressResult.NEWLY_SUPPRESSED: (
                "WOULD SUPPRESS" if dry_run else "SUPPRESSED"
            ),
            SuppressResult.ALREADY_SUPPRESSED: "ALREADY SUPPRESSED",
            SuppressResult.NOT_FOUND: "NOT FOUND",
        }
        for (id_type, id_value), result in results.items():
            status = status_map[result]
            print(f"  [{status}] {id_type}/{id_value}")
