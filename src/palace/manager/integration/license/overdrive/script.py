from __future__ import annotations

import argparse
import csv
import datetime
import logging
from typing import Any

from sqlalchemy import and_, not_, or_
from sqlalchemy.orm import Session

from palace.util.datetime_helpers import strptime_utc
from palace.util.exceptions import PalaceValueError

from palace.manager.celery.tasks import overdrive
from palace.manager.integration.license.overdrive.advantage import (
    OverdriveAdvantageAccount,
)
from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.scripts.base import Script
from palace.manager.scripts.input import InputScript
from palace.manager.sqlalchemy.model.collection import Collection


class GenerateOverdriveAdvantageAccountList(InputScript):
    """Generates a CSV containing the following fields:
    circulation manager
    collection
    client_key
    external_account_id
    library_token
    advantage_name
    advantage_id
    advantage_token
    already_configured
    """

    def __init__(self, _db: Session | None = None, *args: Any, **kwargs: Any) -> None:
        super().__init__(_db, *args, **kwargs)
        self._data: list[list[str | bool]] = list()

    def _create_overdrive_api(self, collection: Collection) -> OverdriveAPI:
        return OverdriveAPI(_db=self._db, collection=collection)

    def do_run(self, **kwargs: Any) -> None:
        parsed = GenerateOverdriveAdvantageAccountList.parse_command_line(
            _db=self._db, **kwargs
        )
        query = Collection.by_protocol(self._db, protocol=OverdriveAPI.label())
        for collection in query.filter(Collection.parent_id == None):
            api = self._create_overdrive_api(collection=collection)
            client_key = api.client_key()
            client_secret = api.client_secret()
            library_id = api.library_id()

            try:
                library_token = api.collection_token
                advantage_accounts = api.get_advantage_accounts()

                for aa in advantage_accounts:
                    existing_child_collections = query.filter(
                        Collection.parent_id == collection.id
                    )
                    already_configured_aa_libraries = [
                        OverdriveAPI.child_settings_load(
                            e.integration_configuration
                        ).external_account_id
                        for e in existing_child_collections
                    ]
                    self._data.append(
                        [
                            collection.name,
                            library_id,
                            client_key,
                            client_secret,
                            library_token,
                            aa.name,
                            aa.library_id,
                            aa.token,
                            aa.library_id in already_configured_aa_libraries,
                        ]
                    )
            except Exception as e:
                logging.error(
                    f"Could not connect to collection {collection.name}: reason: {str(e)}."
                )

        file_path = parsed.output_file_path[0]
        circ_manager_name = parsed.circulation_manager_name[0]
        self.write_csv(output_file_path=file_path, circ_manager_name=circ_manager_name)

    def write_csv(self, output_file_path: str, circ_manager_name: str) -> None:
        with open(output_file_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(
                [
                    "cm",
                    "collection",
                    "overdrive_library_id",
                    "client_key",
                    "client_secret",
                    "library_token",
                    "advantage_name",
                    "advantage_id",
                    "advantage_token",
                    "already_configured",
                ]
            )
            for i in self._data:
                i.insert(0, circ_manager_name)
                writer.writerow(i)

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--output-file-path",
            help="The path of an output file",
            metavar="o",
            nargs=1,
        )

        parser.add_argument(
            "--circulation-manager-name",
            help="The name of the circulation-manager",
            metavar="c",
            nargs=1,
            required=True,
        )

        parser.add_argument(
            "--file-format",
            help="The file format of the output file",
            metavar="f",
            nargs=1,
            default="csv",
        )

        return parser


class OverdriveAdvantageAccountListScript(Script):
    def run(self) -> None:
        """Explain every Overdrive collection and, for each one, all of its
        Advantage collections.
        """
        collections = Collection.by_protocol(self._db, OverdriveAPI.label())
        for collection in collections:
            self.explain_main_collection(collection)
            print()

    def explain_main_collection(self, collection: Collection) -> None:
        """Explain an Overdrive collection and all of its Advantage
        collections.
        """
        api = OverdriveAPI(self._db, collection)
        print("Main Overdrive collection: %s" % collection.name)
        print("\n".join(collection.explain()))
        print("A few of the titles in the main collection:")
        for i, book in enumerate(api.all_ids()):
            print("", book["title"])
            if i > 10:
                break
        advantage_accounts = list(api.get_advantage_accounts())
        print("%d associated Overdrive Advantage account(s)." % len(advantage_accounts))
        for advantage_collection in advantage_accounts:
            self.explain_advantage_collection(advantage_collection)
            print()

    def explain_advantage_collection(
        self, collection: OverdriveAdvantageAccount
    ) -> None:
        """Explain a single Overdrive Advantage collection."""
        parent_collection, child = collection.to_collection(self._db)
        print(" Overdrive Advantage collection: %s" % child.name)
        print(" " + ("\n ".join(child.explain())))
        print(" A few of the titles in this Advantage collection:")
        child_api = OverdriveAPI(self._db, child)
        for i, book in enumerate(child_api.all_ids()):
            print(" ", book["title"])
            if i > 10:
                break


class ImportCollection(Script):
    """A convenient script for manually kicking off an OverDrive collection import"""

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--collection-name",
            type=str,
            help="Collection Name",
        ),
        parser.add_argument(
            "--import-all",
            action="store_true",
            help="Import all identifiers rather not just recently changed ones.",
        ),
        return parser

    def do_run(self, cmd_args: list[str] | None = None) -> None:
        parsed = self.parse_command_line(self._db, cmd_args=cmd_args)
        collection_name = parsed.collection_name

        collection = Collection.by_name(self._db, collection_name)
        if not collection:
            raise PalaceValueError(f'No collection found named "{collection_name}".')

        overdrive.import_collection.delay(
            collection_id=collection.id,
            import_all=parsed.import_all,
        )


class ImportCollectionGroup(Script):
    """A convenient script for manually kicking off an OverDrive main collection import followed by
    an import for each of the child OverDrive Advantage collections in parallel."""

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--collection-name",
            type=str,
            help="Collection Name",
        ),
        parser.add_argument(
            "--import-all",
            action="store_true",
            help="Import all identifiers rather not just recently changed ones.",
        ),
        return parser

    def do_run(self, cmd_args: list[str] | None = None) -> None:
        parsed = self.parse_command_line(self._db, cmd_args=cmd_args)
        collection_name = parsed.collection_name

        collection = Collection.by_name(self._db, collection_name)
        if not collection:
            raise PalaceValueError(f'No collection found named "{collection_name}".')

        if collection.parent:
            raise PalaceValueError(
                f'This collection, "{collection_name}", is an advantage collection. The main collection'
                f'associated with this advantage collection is "{collection.parent.name}".'
            )

        overdrive.import_collection_group.delay(
            collection_id=collection.id,
            import_all=parsed.import_all,
        )


class BackfillCollection(Script):
    """A convenient script for manually kicking off an OverDrive backfill.

    The backfill re-fetches metadata for titles the old importer damaged. It is driven
    by the state of our own database rather than by what Overdrive reports as changed,
    because those titles are wrong for reasons Overdrive will never report.
    """

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description="Manually kick off the Overdrive backfill Celery task."
        )
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument(
            "--collection-name",
            type=str,
            help="Name of the Overdrive collection to back-fill.",
        )
        group.add_argument(
            "--all-collections",
            action="store_true",
            help=(
                "Back-fill every Overdrive collection. Each title costs one blocking "
                "Overdrive request, so prefer one collection at a time unless the "
                "queues have room."
            ),
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=overdrive.BACKFILL_BATCH_SIZE,
            help=(
                "Titles to process per batch "
                f"(default: {overdrive.BACKFILL_BATCH_SIZE}). Ignored with "
                "--all-collections."
            ),
        )
        parser.add_argument(
            "--max-titles",
            type=int,
            default=None,
            help=(
                "Stop after this many titles. Caps the whole run, not each batch. "
                "With --all-collections it caps each collection separately. Use it "
                "to measure the real per-title cost on a sample before committing a "
                "whole collection."
            ),
        )
        parser.add_argument(
            "--import-since",
            type=str,
            default=None,
            metavar="YYYY-MM-DD",
            help=(
                "Only repair titles imported at or after this date -- that is, "
                "titles whose record we last wrote then. The damage came from a "
                "known release, so dating from that release isolates what it "
                "touched. Titles with no presentation edition are excluded when "
                "this is given, because they have no such date."
            ),
        )
        parser.add_argument(
            "--include-suspected-magazines",
            action="store_true",
            help=(
                "Repair suspected magazines too. They are skipped by default. "
                "THE HEURISTIC: a title is suspected of being a magazine when its "
                "series name is identical to its own title, which is how a magazine "
                "issue is shaped. This is a guess about the shape of our data, not a "
                "field Overdrive gives us -- on one production copy it matched 8,233 "
                "of the 8,242 titles that had nothing to open, and 20 of the 626 "
                "that did. They are skipped because we have no magazine support: "
                "Overdrive serves them under a format id we do not recognise, so "
                "repairing one spends a blocking Overdrive request and rewrites both "
                "of its hashes to leave it exactly as damaged as it was."
            ),
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help=(
                "Report how many titles would be processed and exit. Queues nothing "
                "and makes no Overdrive requests. Each title in a real run costs one "
                "blocking Overdrive request, so this is the cheap way to size a run "
                "first."
            ),
        )
        return parser

    def do_run(self, cmd_args: list[str] | None = None) -> None:
        parsed = self.parse_command_line(self._db, cmd_args=cmd_args)

        imported_since = (
            strptime_utc(parsed.import_since, "%Y-%m-%d")
            if parsed.import_since
            else None
        )
        if parsed.max_titles is not None and parsed.max_titles < 1:
            raise PalaceValueError("--max-titles must be at least 1.")
        # A zero batch size would select nothing, log that the backfill is complete
        # and repair none of the titles a dry run just reported. A negative one is
        # not a valid SQL limit at all.
        if parsed.batch_size < 1:
            raise PalaceValueError("--batch-size must be at least 1.")

        collections = (
            self._overdrive_collections()
            if parsed.all_collections
            else [self._overdrive_collection(parsed.collection_name)]
        )

        if parsed.dry_run:
            self._report_dry_run(
                collections,
                imported_since,
                parsed.max_titles,
                parsed.include_suspected_magazines,
            )
            return

        if parsed.all_collections:
            overdrive.backfill_all_collections.delay(
                max_titles=parsed.max_titles,
                imported_since=imported_since,
                include_suspected_magazines=parsed.include_suspected_magazines,
            )
            self.log.info(
                'The "backfill_all_collections" task has been queued for execution. '
                "See the celery logs for details."
            )
            return

        collection = collections[0]
        overdrive.backfill_collection.delay(
            collection_id=collection.id,
            batch_size=parsed.batch_size,
            max_titles=parsed.max_titles,
            imported_since=imported_since,
            include_suspected_magazines=parsed.include_suspected_magazines,
        )
        self.log.info(
            f'The "backfill_collection" task has been queued for collection '
            f"'{collection.name}'. See the celery logs for details."
        )

    def _overdrive_collection(self, collection_name: str) -> Collection:
        """Look up one Overdrive collection by name, or raise."""
        collection = Collection.by_name(self._db, collection_name)
        if not collection:
            raise PalaceValueError(f'No collection found named "{collection_name}".')

        if collection.protocol != OverdriveAPI.label():
            raise PalaceValueError(
                f'Collection "{collection_name}" is not an Overdrive collection '
                f'(protocol: "{collection.protocol}").'
            )
        return collection

    def _overdrive_collections(self) -> list[Collection]:
        """Every Overdrive collection, parent and Advantage alike."""
        registry = self.services.integration_registry().license_providers()
        return list(
            self._db.scalars(
                Collection.select_by_protocol(OverdriveAPI, registry=registry)
            ).all()
        )

    def _report_dry_run(
        self,
        collections: list[Collection],
        imported_since: datetime.datetime | None,
        max_titles: int | None,
        include_suspected_magazines: bool,
    ) -> None:
        """Report what a real run would process, without queuing or fetching anything.

        Counts through the same query the task uses, so the estimate cannot drift from
        the run it is estimating.

        The suspected-magazine count is reported whether or not they are being
        excluded, because it is the number that decides whether a run is worth making
        at all: a collection that is mostly magazines would spend nearly all of its
        blocking Overdrive requests, and nearly all of its rewritten hashes, on titles
        it cannot make openable.
        """
        clauses = overdrive._backfill_clauses()
        found_only_by_the_others = and_(
            or_(clauses.unsearchable, clauses.incomplete),
            not_(clauses.unopenable),
        )
        total = 0

        for collection in collections:
            damaged = overdrive.count_backfill_candidates(
                self._db,
                collection.id,
                imported_since,
                include_suspected_magazines=True,
            )
            not_magazines = overdrive.count_backfill_candidates(
                self._db, collection.id, imported_since
            )
            suspected_magazines = damaged - not_magazines

            candidates = damaged if include_suspected_magazines else not_magazines
            # max_titles caps each collection separately, matching how
            # backfill_all_collections forwards it.
            to_process = (
                candidates if max_titles is None else min(candidates, max_titles)
            )
            total += to_process

            # Report the symptoms apart as well as together. "Unopenable" is expected
            # to cover nearly everything; the other two are worth keeping only if they
            # find titles it misses, and this is how you find out.
            unopenable = overdrive.count_backfill_candidates(
                self._db,
                collection.id,
                imported_since,
                clause=clauses.unopenable,
                include_suspected_magazines=include_suspected_magazines,
            )
            only_others = overdrive.count_backfill_candidates(
                self._db,
                collection.id,
                imported_since,
                clause=found_only_by_the_others,
                include_suspected_magazines=include_suspected_magazines,
            )

            disposition = (
                "included; pass no flag to skip them"
                if include_suspected_magazines
                else "skipped by default"
            )
            self.log.info(
                f"[dry run] '{collection.name}' (id={collection.id}): "
                f"{damaged} damaged title(s), of which {suspected_magazines} "
                f"suspected magazine(s) ({disposition}). "
                f"{to_process} would be processed "
                f"({unopenable} unopenable; {only_others} found only by the other "
                f"two symptoms)."
            )

        # The per-collection numbers above count pools. A title held by more than one
        # collection -- a parent and its Advantage children, or two libraries on this
        # manager -- appears in each of them, but a run repairs it once and the other
        # collections then stop selecting it, because every damage symptom is keyed on
        # the title rather than the collection. So summing those numbers overstates
        # the job. Count the distinct titles for the figure that matters: how many
        # blocking Overdrive requests the run will actually make.
        titles = overdrive.count_backfill_titles(
            self._db,
            [collection.id for collection in collections],
            imported_since,
            include_suspected_magazines=include_suspected_magazines,
        )
        shared = total - titles
        shared_note = (
            f" That is {shared} fewer than the per-collection totals above, because"
            f" a title in more than one collection is repaired once."
            if shared > 0
            else ""
        )
        capped_note = (
            f" A cap of {max_titles} per collection applies on top of this."
            if max_titles is not None
            else ""
        )
        self.log.info(
            f"[dry run] {titles} distinct title(s) across "
            f"{len(collections)} collection(s), one blocking Overdrive request each."
            f"{shared_note}{capped_note} Nothing has been queued."
        )
