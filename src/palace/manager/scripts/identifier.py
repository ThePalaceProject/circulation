import argparse
import sys
from collections.abc import Sequence

from sqlalchemy.orm import Session

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.policy.presentation import (
    PresentationCalculationPolicy,
)
from palace.manager.scripts.input import IdentifierInputScript, SupportsReadlines
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier


class AddClassificationScript(IdentifierInputScript):
    name = "Add a classification to an identifier"

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        parser = IdentifierInputScript.arg_parser(_db)
        parser.add_argument(
            "--subject-type",
            help="The type of the subject to add to each identifier.",
            required=True,
        )
        parser.add_argument(
            "--subject-identifier",
            help="The identifier of the subject to add to each identifier.",
        )
        parser.add_argument(
            "--subject-name", help="The name of the subject to add to each identifier."
        )
        parser.add_argument(
            "--data-source",
            help="The data source to use when classifying.",
            default=DataSource.MANUAL,
        )
        parser.add_argument(
            "--weight",
            help="The weight to use when classifying.",
            type=int,
            default=1000,
        )
        parser.add_argument(
            "--create-subject",
            help="Add the subject to the database if it doesn't already exist",
            action="store_const",
            const=True,
        )
        return parser

    def __init__(
        self,
        _db: Session | None = None,
        cmd_args: Sequence[str | None] | None = None,
        stdin: SupportsReadlines = sys.stdin,
    ) -> None:
        super().__init__(_db=_db)
        args = self.parse_command_line(self._db, cmd_args=cmd_args, stdin=stdin)
        self.identifier_type = args.identifier_type
        self.identifiers: list[Identifier] = args.identifiers
        subject_type = args.subject_type
        subject_identifier = args.subject_identifier
        subject_name = args.subject_name
        if not subject_name and not subject_identifier:
            raise PalaceValueError(
                "Either subject-name or subject-identifier must be provided."
            )
        data_source = DataSource.lookup(self._db, args.data_source)
        if data_source is None:
            raise PalaceValueError(f"Unknown data source: {args.data_source}")
        self.data_source = data_source
        self.weight = args.weight
        self.subject, ignore = Subject.lookup(
            self._db,
            subject_type,
            subject_identifier,
            subject_name,
            autocreate=args.create_subject,
        )

    def do_run(self) -> None:
        policy = PresentationCalculationPolicy(
            choose_edition=False,
            set_edition_metadata=False,
            classify=True,
            choose_summary=False,
            calculate_quality=False,
            choose_cover=False,
            update_search_index=True,
            verbose=True,
        )
        if self.subject:
            for identifier in self.identifiers:
                identifier.classify(
                    self.data_source,
                    self.subject.type,
                    self.subject.identifier,
                    self.subject.name,
                    self.weight,
                )
                work = identifier.work
                if work:
                    work.calculate_presentation(policy=policy)
        else:
            self.log.warning("Could not locate subject, doing nothing.")
