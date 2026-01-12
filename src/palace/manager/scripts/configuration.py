import argparse
import sys
import uuid
from collections.abc import Sequence
from typing import TextIO

from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.orm.session import Session

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.scripts.base import Script, _normalize_cmd_args
from palace.manager.service.integration_registry.license_providers import (
    LicenseProvidersRegistry,
)
from palace.manager.sqlalchemy.listeners import site_configuration_has_changed
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.lane import Lane
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.util import create, get_one


class ConfigurationSettingScript(Script):
    @classmethod
    def _parse_setting(cls, setting: str) -> tuple[str, str]:
        """Parse a command-line setting option into a key-value pair."""
        if "=" not in setting:
            raise PalaceValueError(
                f'Incorrect format for setting: "{setting}". Should be "key=value"'
            )
        key, value = setting.split("=", 1)
        return key, value

    @classmethod
    def add_setting_argument(cls, parser: argparse.ArgumentParser, help: str) -> None:
        """Modify an ArgumentParser to indicate that the script takes
        command-line settings.
        """
        parser.add_argument("--setting", help=help, action="append")


class ConfigureLibraryScript(ConfigurationSettingScript):
    """Create a library or change its settings."""

    name = "Change a library's settings"

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--name",
            help="Official name of the library",
        )
        parser.add_argument(
            "--short-name",
            help="Short name of the library",
        )
        cls.add_setting_argument(
            parser,
            'Set a per-library setting, such as terms-of-service. Format: --setting="terms-of-service=https://example.library/tos"',
        )
        return parser

    def apply_settings(self, settings: Sequence[str] | None, library: Library) -> None:
        """Treat `settings` as a list of command-line argument settings,
        and apply each one to `obj`.
        """
        if not settings:
            return
        for setting in settings:
            key, value = self._parse_setting(setting)
            library.settings_dict[key] = value
        flag_modified(library, "settings_dict")

    def do_run(
        self,
        _db: Session | None = None,
        cmd_args: Sequence[str | None] | None = None,
        output: TextIO = sys.stdout,
    ) -> None:
        _db = _db or self._db
        args = self.parse_command_line(_db, cmd_args=cmd_args)
        if not args.short_name:
            raise PalaceValueError("You must identify the library by its short name.")

        # Are we talking about an existing library?
        libraries = _db.query(Library).all()

        if libraries:
            # Currently there can only be one library, and one already exists.
            [library] = libraries
            if args.short_name and library.short_name != args.short_name:
                raise PalaceValueError(f"Could not locate library '{args.short_name}'")
        else:
            # No existing library. Make one.
            public_key, private_key = Library.generate_keypair()
            library, ignore = create(
                _db,
                Library,
                uuid=str(uuid.uuid4()),
                short_name=args.short_name,
                public_key=public_key,
                private_key=private_key,
            )

        if args.name:
            library.name = args.name
        if args.short_name:
            library.short_name = args.short_name
        self.apply_settings(args.setting, library)
        site_configuration_has_changed(_db)
        _db.commit()
        output.write("Configuration settings stored.\n")
        output.write("\n".join(library.explain()))
        output.write("\n")


class ConfigureCollectionScript(ConfigurationSettingScript):
    """Create a collection or change its settings."""

    name = "Change a collection's settings"

    @classmethod
    def parse_command_line(
        cls,
        _db: Session,
        cmd_args: Sequence[str | None] | None = None,
    ) -> argparse.Namespace:
        parser = cls.arg_parser(_db)
        return parser.parse_known_args(_normalize_cmd_args(cmd_args))[0]

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        registry = LicenseProvidersRegistry()
        protocols = [protocol for protocol, _ in registry]

        parser = argparse.ArgumentParser()
        parser.add_argument("--name", help="Name of the collection", required=True)
        parser.add_argument(
            "--protocol",
            help='Protocol to use to get the licenses. Possible values: "%s"'
            % ('", "'.join(protocols)),
        )
        parser.add_argument(
            "--external-account-id",
            help='The ID of this collection according to the license source. Sometimes called a "library ID".',
        )
        parser.add_argument(
            "--url",
            help="Run the acquisition protocol against this URL.",
        )
        parser.add_argument(
            "--username",
            help='Use this username to authenticate with the license protocol. Sometimes called a "key".',
        )
        parser.add_argument(
            "--password",
            help='Use this password to authenticate with the license protocol. Sometimes called a "secret".',
        )
        cls.add_setting_argument(
            parser,
            'Set a protocol-specific setting on the collection, such as Overdrive\'s "website_id". Format: --setting="website_id=89"',
        )
        library_names = cls._library_names(_db)
        if library_names:
            parser.add_argument(
                "--library",
                help="Associate this collection with the given library. Possible libraries: %s"
                % library_names,
                action="append",
            )

        return parser

    @classmethod
    def _library_names(cls, _db: Session) -> str:
        """Return a string that lists known library names."""
        library_names = [
            x.short_name for x in _db.query(Library).order_by(Library.short_name)
        ]
        if library_names:
            return f'"{"\", \"".join(library_names)}"'
        return ""

    def do_run(
        self,
        _db: Session | None = None,
        cmd_args: Sequence[str | None] | None = None,
        output: TextIO = sys.stdout,
    ) -> None:
        _db = _db or self._db
        args = self.parse_command_line(_db, cmd_args=cmd_args)

        # Find or create the collection
        protocol = None
        name = args.name
        protocol = args.protocol
        collection = Collection.by_name(_db, name)
        if not collection:
            if protocol:
                collection, is_new = Collection.by_name_and_protocol(
                    _db, name, protocol
                )
            else:
                # We didn't find a Collection, and we don't have a protocol,
                # so we can't create a new Collection.
                raise PalaceValueError(
                    f'No collection called "{name}". You can create it, but you must specify a protocol.'
                )
        config = collection.integration_configuration
        settings = config.settings_dict.copy()
        if protocol:
            config.protocol = protocol
        if args.external_account_id:
            settings["external_account_id"] = args.external_account_id
        if args.url:
            settings["url"] = args.url
        if args.username:
            settings["username"] = args.username
        if args.password:
            settings["password"] = args.password
        if args.setting:
            for setting in args.setting:
                key, value = self._parse_setting(setting)
                settings[key] = value
        config.settings_dict = settings

        if hasattr(args, "library"):
            for name in args.library:
                library = get_one(_db, Library, short_name=name)
                if not library:
                    library_names = self._library_names(_db)
                    message = f'No such library: "{name}".'
                    if library_names:
                        message += f" I only know about: {library_names}"
                    raise PalaceValueError(message)
                if collection not in library.associated_collections:
                    collection.associated_libraries.append(library)
        site_configuration_has_changed(_db)
        _db.commit()
        output.write("Configuration settings stored.\n")
        output.write("\n".join(collection.explain()))
        output.write("\n")


class ConfigureLaneScript(ConfigurationSettingScript):
    """Create a lane or change its settings."""

    name = "Change a lane's settings"

    @classmethod
    def parse_command_line(
        cls,
        _db: Session,
        cmd_args: Sequence[str | None] | None = None,
    ) -> argparse.Namespace:
        parser = cls.arg_parser(_db)
        return parser.parse_known_args(_normalize_cmd_args(cmd_args))[0]

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--id",
            help="ID of the lane, if editing an existing lane.",
        )
        parser.add_argument(
            "--library-short-name",
            help="Short name of the library for this lane. Possible values: %s"
            % cls._library_names(_db),
        )
        parser.add_argument(
            "--parent-id",
            help="The ID of this lane's parent lane",
        )
        parser.add_argument(
            "--priority",
            help="The lane's priority",
        )
        parser.add_argument(
            "--display-name",
            help="The lane name that will be displayed to patrons.",
        )
        return parser

    @classmethod
    def _library_names(cls, _db: Session) -> str:
        """Return a string that lists known library names."""
        library_names = [
            x.short_name for x in _db.query(Library).order_by(Library.short_name)
        ]
        if library_names:
            return f'"{"\", \"".join(library_names)}"'
        return ""

    def do_run(
        self,
        _db: Session | None = None,
        cmd_args: Sequence[str | None] | None = None,
        output: TextIO = sys.stdout,
    ) -> None:
        _db = _db or self._db
        args = self.parse_command_line(_db, cmd_args=cmd_args)

        # Find or create the lane
        id = args.id
        lane = get_one(_db, Lane, id=id)
        if not lane:
            if args.library_short_name and args.display_name:
                library = get_one(_db, Library, short_name=args.library_short_name)
                if not library:
                    raise PalaceValueError(
                        f'No such library: "{args.library_short_name}".'
                    )
                lane, is_new = create(
                    _db, Lane, library=library, display_name=args.display_name
                )
            else:
                raise PalaceValueError(
                    "Library short name and lane display name are required to create a new lane."
                )
        if lane is None:
            raise PalaceValueError("Unable to locate or create a lane.")

        if args.parent_id:
            lane.parent_id = args.parent_id
        if args.priority:
            lane.priority = args.priority
        if args.display_name:
            lane.display_name = args.display_name
        site_configuration_has_changed(_db)
        _db.commit()
        output.write("Lane settings stored.\n")
        output.write("\n".join(lane.explain()))
        output.write("\n")
