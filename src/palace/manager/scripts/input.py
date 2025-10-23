import argparse
import logging
import os
import sys

from sqlalchemy.exc import MultipleResultsFound, NoResultFound

from palace.manager.scripts.base import Script
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.sqlalchemy.util import get_one


class InputScript(Script):
    @classmethod
    def read_stdin_lines(self, stdin):
        """Read lines from a (possibly mocked, possibly empty) standard input."""
        if stdin is not sys.stdin or not os.isatty(0):
            # A file has been redirected into standard input. Grab its
            # lines.
            lines = [x.strip() for x in stdin.readlines()]
        else:
            lines = []
        return lines


class IdentifierInputScript(InputScript):
    """A script that takes identifiers as command line inputs."""

    DATABASE_ID = "Database ID"

    @classmethod
    def parse_command_line(
        cls, _db=None, cmd_args=None, stdin=sys.stdin, *args, **kwargs
    ):
        parser = cls.arg_parser()
        parsed = parser.parse_args(cmd_args)
        stdin = cls.read_stdin_lines(stdin)
        return cls.look_up_identifiers(_db, parsed, stdin, *args, **kwargs)

    @classmethod
    def look_up_identifiers(
        cls, _db, parsed, stdin_identifier_strings, *args, **kwargs
    ):
        """Turn identifiers as specified on the command line into
        real database Identifier objects.
        """
        data_source = None
        if parsed.identifier_data_source:
            data_source = DataSource.lookup(_db, parsed.identifier_data_source)
        if _db and parsed.identifier_type:
            # We can also call parse_identifier_list.
            identifier_strings = parsed.identifier_strings
            if stdin_identifier_strings:
                identifier_strings = identifier_strings + stdin_identifier_strings
            parsed.identifiers = cls.parse_identifier_list(
                _db,
                parsed.identifier_type,
                data_source,
                identifier_strings,
                *args,
                **kwargs,
            )
        else:
            # The script can call parse_identifier_list later if it
            # wants to.
            parsed.identifiers = None
        return parsed

    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--identifier-type",
            help='Process identifiers of this type. If IDENTIFIER is not specified, all identifiers of this type will be processed. To name identifiers by their database ID, use --identifier-type="Database ID"',
        )
        parser.add_argument(
            "--identifier-data-source",
            help="Process only identifiers which have a LicensePool associated with this DataSource",
        )
        parser.add_argument(
            "identifier_strings",
            help="A specific identifier to process.",
            metavar="IDENTIFIER",
            nargs="*",
        )
        return parser

    @classmethod
    def parse_identifier_list(
        cls, _db, identifier_type, data_source, arguments, autocreate=False
    ):
        """Turn a list of identifiers into a list of Identifier objects.

        The list of arguments is probably derived from a command-line
        parser such as the one defined in
        IdentifierInputScript.arg_parser().

        This makes it easy to identify specific identifiers on the
        command line. Examples:

        1 2

        a b c
        """
        identifiers = []

        if not identifier_type:
            raise ValueError(
                "No identifier type specified! Use '--identifier-type=\"Database ID\"' to name identifiers by database ID."
            )

        if len(arguments) == 0:
            if data_source:
                identifiers = (
                    _db.query(Identifier)
                    .join(Identifier.licensed_through)
                    .filter(
                        Identifier.type == identifier_type,
                        LicensePool.data_source == data_source,
                    )
                    .all()
                )
            return identifiers

        for arg in arguments:
            if identifier_type == cls.DATABASE_ID:
                try:
                    arg = int(arg)
                except ValueError as e:
                    # We'll print out a warning later.
                    arg = None
                if arg:
                    identifier = get_one(_db, Identifier, id=arg)
            else:
                identifier, ignore = Identifier.for_foreign_id(
                    _db, identifier_type, arg, autocreate=autocreate
                )
            if not identifier:
                logging.warning("Could not load identifier %s/%s", identifier_type, arg)
            if identifier:
                identifiers.append(identifier)
        return identifiers


class LibraryInputScript(InputScript):
    """A script that operates on one or more Libraries."""

    @classmethod
    def parse_command_line(cls, _db=None, cmd_args=None, *args, **kwargs):
        parser = cls.arg_parser(_db)
        parsed = parser.parse_args(cmd_args)
        return cls.look_up_libraries(_db, parsed, *args, **kwargs)

    @classmethod
    def arg_parser(cls, _db, multiple_libraries=True):
        parser = argparse.ArgumentParser()
        library_names = sorted(l.short_name for l in _db.query(Library))
        library_names = '"' + '", "'.join(library_names) + '"'
        parser.add_argument(
            "libraries",
            help="Name of a specific library to process. Libraries on this system: %s"
            % library_names,
            metavar="SHORT_NAME",
            nargs="*" if multiple_libraries else 1,
        )
        return parser

    @classmethod
    def look_up_libraries(cls, _db, parsed, *args, **kwargs):
        """Turn library names as specified on the command line into real
        Library objects.
        """
        if _db:
            library_strings = parsed.libraries
            if library_strings:
                parsed.libraries = cls.parse_library_list(
                    _db, library_strings, *args, **kwargs
                )
            else:
                # No libraries are specified. We will be processing
                # every library.
                parsed.libraries = _db.query(Library).all()
        else:
            # Database is not active yet. The script can call
            # parse_library_list later if it wants to.
            parsed.libraries = None
        return parsed

    @classmethod
    def parse_library_list(cls, _db, arguments):
        """Turn a list of library short names into a list of Library objects.

        The list of arguments is probably derived from a command-line
        parser such as the one defined in
        LibraryInputScript.arg_parser().
        """
        if len(arguments) == 0:
            return []
        libraries = []
        for arg in arguments:
            if not arg:
                continue
            for field in (Library.short_name, Library.name):
                try:
                    library = _db.query(Library).filter(field == arg).one()
                except NoResultFound:
                    continue
                except MultipleResultsFound:
                    continue
                if library:
                    libraries.append(library)
                    break
            else:
                logging.warning("Could not find library %s", arg)
        return libraries

    def do_run(self, *args, **kwargs):
        parsed = self.parse_command_line(self._db, *args, **kwargs)
        self.process_libraries(parsed.libraries)

    def process_libraries(self, libraries):
        for library in libraries:
            self.process_library(library)

    def process_library(self, library):
        raise NotImplementedError()


class PatronInputScript(LibraryInputScript):
    """A script that operates on one or more Patrons."""

    @classmethod
    def parse_command_line(
        cls, _db=None, cmd_args=None, stdin=sys.stdin, *args, **kwargs
    ):
        parser = cls.arg_parser(_db)
        parsed = parser.parse_args(cmd_args)
        if stdin:
            stdin = cls.read_stdin_lines(stdin)
        parsed = super().look_up_libraries(_db, parsed, *args, **kwargs)
        return cls.look_up_patrons(_db, parsed, stdin, *args, **kwargs)

    @classmethod
    def arg_parser(cls, _db):
        parser = super().arg_parser(_db, multiple_libraries=False)
        parser.add_argument(
            "identifiers",
            help="A specific patron identifier to process.",
            metavar="IDENTIFIER",
            nargs="+",
        )
        return parser

    @classmethod
    def look_up_patrons(cls, _db, parsed, stdin_patron_strings, *args, **kwargs):
        """Turn patron identifiers as specified on the command line into real
        Patron objects.
        """
        if _db:
            patron_strings = parsed.identifiers
            library = parsed.libraries[0]
            if stdin_patron_strings:
                patron_strings = patron_strings + stdin_patron_strings
            parsed.patrons = cls.parse_patron_list(
                _db, library, patron_strings, *args, **kwargs
            )
        else:
            # Database is not active yet. The script can call
            # parse_patron_list later if it wants to.
            parsed.patrons = None
        return parsed

    @classmethod
    def parse_patron_list(cls, _db, library, arguments):
        """Turn a list of patron identifiers into a list of Patron objects.

        The list of arguments is probably derived from a command-line
        parser such as the one defined in
        PatronInputScript.arg_parser().
        """
        if len(arguments) == 0:
            return []
        patrons = []
        for arg in arguments:
            if not arg:
                continue
            for field in (
                Patron.authorization_identifier,
                Patron.username,
                Patron.external_identifier,
            ):
                try:
                    patron = (
                        _db.query(Patron)
                        .filter(field == arg)
                        .filter(Patron.library_id == library.id)
                        .one()
                    )
                except NoResultFound:
                    continue
                except MultipleResultsFound:
                    continue
                if patron:
                    patrons.append(patron)
                    break
            else:
                logging.warning("Could not find patron %s", arg)
        return patrons

    def do_run(self, *args, **kwargs):
        parsed = self.parse_command_line(self._db, *args, **kwargs)
        self.process_patrons(parsed.patrons)

    def process_patrons(self, patrons):
        for patron in patrons:
            self.process_patron(patron)

    def process_patron(self, patron):
        raise NotImplementedError()


class CollectionInputScript(Script):
    """A script that takes collection names as command line inputs."""

    @classmethod
    def parse_command_line(cls, _db=None, cmd_args=None, *args, **kwargs):
        parser = cls.arg_parser()
        parsed = parser.parse_args(cmd_args)
        return cls.look_up_collections(_db, parsed, *args, **kwargs)

    @classmethod
    def look_up_collections(cls, _db, parsed, *args, **kwargs):
        """Turn collection names as specified on the command line into
        real database Collection objects.
        """
        parsed.collections = []
        for name in parsed.collection_names:
            collection = Collection.by_name(_db, name)

            if not collection:
                raise ValueError("Unknown collection: %s" % name)
            parsed.collections.append(collection)
        return parsed

    @classmethod
    def arg_parser(cls) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--collection",
            help="Collection to use",
            dest="collection_names",
            metavar="NAME",
            action="append",
            default=[],
        )
        return parser


class CollectionArgumentsScript(CollectionInputScript):
    @classmethod
    def arg_parser(cls) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "collection_names",
            help="One or more collection names.",
            metavar="COLLECTION",
            nargs="*",
        )
        return parser
