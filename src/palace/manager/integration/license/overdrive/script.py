from __future__ import annotations

import argparse
import csv
import logging
from typing import Any

from sqlalchemy.orm import Session

from palace.manager.celery.tasks import overdrive
from palace.manager.core.exceptions import PalaceValueError
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
