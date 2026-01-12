import argparse
from collections.abc import Sequence
from typing import cast

from sqlalchemy import select
from sqlalchemy.orm import Session

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.scripts.base import Script, _normalize_cmd_args
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.library import Library


class SuppressWorkForLibraryScript(Script):
    """Suppress works from a library by identifier"""

    BY_DATABASE_ID = "Database ID"

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        library_name_list = sorted(str(l.short_name) for l in _db.query(Library))
        library_names = '"' + '", "'.join(library_name_list) + '"'
        parser.add_argument(
            "-l",
            "--library",
            help="Short name of the library. Libraries on this system: %s."
            % library_names,
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
        parser.add_argument(
            "-i",
            "--identifier",
            help="The identifier to suppress.",
            required=True,
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
        if not library:
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
        if not identifier_obj:
            raise PalaceValueError(
                f"Unknown identifier: {identifier_type}/{identifier}"
            )

        return identifier_obj

    def do_run(self, cmd_args: list[str] | None = None) -> None:
        parsed = self.parse_command_line(self._db, cmd_args=cmd_args)

        library = self.load_library(parsed.library)
        identifier = self.load_identifier(parsed.identifier_type, parsed.identifier)

        self.suppress_work(library, identifier)

    def suppress_work(self, library: Library, identifier: Identifier) -> None:
        work = identifier.work
        if not work:
            self.log.warning(f"No work found for {identifier}")
            return

        work.suppressed_for.append(library)
        self.log.info(
            f"Suppressed {identifier.type}/{identifier.identifier} (work id: {work.id}) for {library.short_name}."
        )

        self._db.commit()
