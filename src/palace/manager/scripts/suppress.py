import argparse
import csv
from collections.abc import Sequence
from datetime import datetime, timezone
from enum import Enum, auto
from typing import NamedTuple, cast

from sqlalchemy import select
from sqlalchemy.orm import Session

from palace.util.exceptions import PalaceValueError

from palace.manager.scripts.base import Script, _normalize_cmd_args
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.work import Work


class SuppressResult(Enum):
    NEWLY_SUPPRESSED = auto()
    ALREADY_SUPPRESSED = auto()
    NOT_FOUND = auto()
    AMBIGUOUS = auto()


class SuppressOutcome(NamedTuple):
    result: SuppressResult
    # The title and work id of every work described by this outcome,
    # formatted as "<title> (work id: <id>)" and joined with "; " when
    # there's more than one -- e.g. for AMBIGUOUS, or when
    # --suppress-ambiguous affects more than one work at once.
    title: str | None = None


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
        parser.add_argument(
            "--suppress-ambiguous",
            help="If an identifier resolves to more than one distinct work via "
            "identifier equivalency (e.g. the same ISBN licensed through more "
            "than one collection), suppress it in all of them instead of "
            "skipping it.",
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
                    id_type_val = row.get("identifier_type") or ""
                    if has_type_column and id_type_val.strip():
                        id_type = id_type_val.strip()
                    else:
                        id_type = default_identifier_type
                    identifiers.append((id_type, identifier))
        except FileNotFoundError:
            raise PalaceValueError(f"CSV file not found: {file_path}")
        return identifiers

    def load_works(self, identifier: Identifier, library: Library) -> list[Work]:
        """Find the Work(s) reachable from an identifier for a library.

        An identifier that owns a LicensePool directly is an exact match
        and is returned on its own, with no need to consult equivalencies,
        regardless of which library that LicensePool's collection belongs
        to (this matches the identifier resolution the script has always
        done, before this class started consulting equivalencies at all).

        Only when there's no direct match do we look past LicensePools
        whose own identifier matches, to also include LicensePools
        reachable through identifier equivalency -- e.g. an ISBN a
        librarian has on hand is often linked to a vendor's LicensePool
        via metadata equivalency rather than being that LicensePool's own
        identifier. This uses the same strict, high-confidence equivalency
        policy that `Work.from_identifiers` applies by default everywhere
        else in the codebase, so it won't walk into loosely-related works.

        This equivalency fallback is scoped to `library`'s own collections.
        Otherwise, a title licensed to two different libraries through two
        different collections (e.g. one via OverDrive, one via Bibliotheca)
        would look ambiguous when suppressing for either library alone,
        even though only one of the two candidate works is actually
        licensed to that library.
        """
        direct_work = identifier.work
        if direct_work is not None:
            return [direct_work]

        collection_ids = [c.id for c in library.active_collections]
        if not collection_ids:
            return []

        base_query = (
            self._db.query(Work)
            .join(Work.license_pools)
            .join(LicensePool.identifier)
            .filter(LicensePool.collection_id.in_(collection_ids))
        )
        query = Work.from_identifiers(self._db, [identifier], base_query=base_query)
        if query is None:
            return []
        return cast(list[Work], query.distinct().all())

    @staticmethod
    def _describe_works(works: list[Work]) -> str:
        """Format a list of works for operator-facing output, e.g. for an
        ambiguous match or when suppressing more than one work at once."""
        return "; ".join(f"{w.title or '[no title]'} (work id: {w.id})" for w in works)

    def suppress_work(
        self,
        library: Library,
        identifier: Identifier,
        dry_run: bool = False,
        suppress_ambiguous: bool = False,
    ) -> SuppressOutcome:
        """Suppress the work(s) resolved from an identifier for a library.

        :param library: The library for which the resolved work should be suppressed.
        :param identifier: The identifier used to resolve the work, either
            directly (it owns a LicensePool) or through identifier equivalency.
        :param dry_run: If true, report the outcome without changing suppression.
        :param suppress_ambiguous: If the identifier resolves to more than one
            distinct work via equivalency, suppress it for the library in
            every candidate work instead of refusing to guess which one is
            meant.
        :return: The result of the suppression attempt, and the resolved
            work's title(s) when available.
        """
        works = self.load_works(identifier, library)
        if not works:
            self.log.warning(f"No work found for {identifier}")
            return SuppressOutcome(SuppressResult.NOT_FOUND)

        if len(works) == 1:
            work = works[0]
            description = self._describe_works(works)

            if library in work.suppressed_for:
                return SuppressOutcome(SuppressResult.ALREADY_SUPPRESSED, description)

            if not dry_run:
                # Suppression is scoped to exactly this one library. Resolving
                # the work via identifier equivalency only changes *which work*
                # we find -- it never changes *which libraries* it's suppressed
                # for, since only the `library` argument passed in is ever
                # appended to `suppressed_for`.
                work.suppressed_for.append(library)

            self.log.info(
                f"{'[DRY RUN] Would suppress' if dry_run else 'Suppressing'} "
                f"{identifier.type}/{identifier.identifier} (work id: {work.id}) "
                f"for {library.short_name}."
            )
            return SuppressOutcome(SuppressResult.NEWLY_SUPPRESSED, description)

        # More than one distinct Work is reachable from this identifier via
        # equivalency (see load_works) -- e.g. the same ISBN licensed
        # through more than one of this library's collections, each with
        # its own permanent Work.
        if all(library in work.suppressed_for for work in works):
            # Every candidate already reflects the desired state, so there's
            # nothing to decide or change -- this isn't really ambiguous in
            # practice, just a no-op re-run (e.g. after an earlier
            # --suppress-ambiguous run already covered every candidate).
            return SuppressOutcome(
                SuppressResult.ALREADY_SUPPRESSED, self._describe_works(works)
            )

        if not suppress_ambiguous:
            # Without --suppress-ambiguous, refuse to guess which one(s) the
            # operator meant.
            titles = self._describe_works(works)
            self.log.warning(
                f"{identifier.type}/{identifier.identifier} resolves to "
                f"{len(works)} different works via identifier equivalency; "
                "skipping rather than guessing which one to suppress. Pass "
                "--suppress-ambiguous to suppress all of them for this library."
            )
            return SuppressOutcome(SuppressResult.AMBIGUOUS, titles)

        # At least one candidate isn't yet suppressed (the all-suppressed
        # case was handled above), so this always changes at least one work.
        changed_works: list[Work] = []
        for work in works:
            if library in work.suppressed_for:
                continue
            if not dry_run:
                work.suppressed_for.append(library)
            changed_works.append(work)

        self.log.info(
            f"{'[DRY RUN] Would suppress' if dry_run else 'Suppressing'} "
            f"{identifier.type}/{identifier.identifier} in {len(changed_works)} "
            f"of {len(works)} ambiguous work(s) for {library.short_name}."
        )
        return SuppressOutcome(
            SuppressResult.NEWLY_SUPPRESSED, self._describe_works(changed_works)
        )

    def do_run(self, cmd_args: list[str] | None = None) -> None:
        parsed = self.parse_command_line(self._db, cmd_args=cmd_args)
        library = self.load_library(parsed.library)
        dry_run: bool = parsed.dry_run
        suppress_ambiguous: bool = parsed.suppress_ambiguous
        started_at = datetime.now(tz=timezone.utc)

        if parsed.file:
            pairs = self.load_identifiers_from_file(parsed.file, parsed.identifier_type)
        else:
            pairs = [(parsed.identifier_type, parsed.identifier)]

        seen: set[tuple[str, str]] = set()
        unique_pairs: list[tuple[str, str]] = []
        for pair in pairs:
            if pair not in seen:
                seen.add(pair)
                unique_pairs.append(pair)
        if len(unique_pairs) < len(pairs):
            self.log.warning(
                f"Removed {len(pairs) - len(unique_pairs)} duplicate identifier(s) from input."
            )
        pairs = unique_pairs

        results: dict[tuple[str, str], SuppressOutcome] = {}
        try:
            for id_type, id_value in pairs:
                try:
                    identifier = self.load_identifier(id_type, id_value)
                    outcome = self.suppress_work(
                        library,
                        identifier,
                        dry_run=dry_run,
                        suppress_ambiguous=suppress_ambiguous,
                    )
                except PalaceValueError:
                    outcome = SuppressOutcome(SuppressResult.NOT_FOUND)
                results[(id_type, id_value)] = outcome

            if not dry_run:
                self._db.commit()
        except Exception:
            self._db.rollback()
            raise

        duration = datetime.now(tz=timezone.utc) - started_at
        self._print_results(
            results, dry_run, library, started_at, duration.total_seconds()
        )

    def _print_results(
        self,
        results: dict[tuple[str, str], SuppressOutcome],
        dry_run: bool,
        library: Library,
        started_at: datetime,
        duration_seconds: float,
    ) -> None:
        newly_suppressed = [
            k for k, v in results.items() if v.result == SuppressResult.NEWLY_SUPPRESSED
        ]
        already_suppressed = [
            k
            for k, v in results.items()
            if v.result == SuppressResult.ALREADY_SUPPRESSED
        ]
        not_found = [
            k for k, v in results.items() if v.result == SuppressResult.NOT_FOUND
        ]
        ambiguous = [
            k for k, v in results.items() if v.result == SuppressResult.AMBIGUOUS
        ]

        prefix = "[DRY RUN] " if dry_run else ""
        suppress_label = "Would suppress" if dry_run else "Newly suppressed"

        summary_rows = [
            ("Library:", f"{library.name} ({library.short_name})"),
            ("Started at:", started_at.strftime("%Y-%m-%d %H:%M:%S UTC")),
            ("Duration:", f"{duration_seconds:.2f}s"),
            (suppress_label + ":", len(newly_suppressed)),
            ("Already suppressed:", len(already_suppressed)),
            ("Not found:", len(not_found)),
            ("Ambiguous:", len(ambiguous)),
        ]
        col = max(len(label) for label, _ in summary_rows)
        print(f"\n{prefix}Suppression Results Summary:")
        for label, value in summary_rows:
            print(f"  {label:<{col}} {value}")

        print(f"\n{prefix}Details:")
        status_map = {
            SuppressResult.NEWLY_SUPPRESSED: (
                "WOULD SUPPRESS" if dry_run else "SUPPRESSED"
            ),
            SuppressResult.ALREADY_SUPPRESSED: "ALREADY SUPPRESSED",
            SuppressResult.NOT_FOUND: "NOT FOUND",
            SuppressResult.AMBIGUOUS: "AMBIGUOUS",
        }
        for (id_type, id_value), outcome in results.items():
            status = status_map[outcome.result]
            title_suffix = f" -- {outcome.title}" if outcome.title else ""
            print(f"  [{status}] {id_type}/{id_value}{title_suffix}")
