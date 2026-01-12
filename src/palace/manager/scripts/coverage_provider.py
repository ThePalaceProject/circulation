import argparse
import random
import sys
from collections.abc import Callable, Sequence
from typing import Any

from sqlalchemy.orm import Session

from palace.manager.core.coverage import (
    BaseCoverageProvider,
    CollectionCoverageProvider,
    CoverageProviderProgress,
    IdentifierCoverageProvider,
)
from palace.manager.scripts.base import Script, _normalize_cmd_args
from palace.manager.scripts.input import IdentifierInputScript, SupportsReadlines
from palace.manager.sqlalchemy.model.identifier import Identifier


class RunCoverageProvidersScript(Script):
    """Alternate between multiple coverage providers."""

    def __init__(
        self,
        providers: Sequence[
            BaseCoverageProvider | Callable[[Session], BaseCoverageProvider]
        ],
        _db: Session | None = None,
    ) -> None:
        super().__init__(_db=_db)
        self.providers = []
        for i in providers:
            if callable(i):
                i = i(self._db)
            self.providers.append(i)

    def do_run(self) -> list[CoverageProviderProgress | None]:
        providers = list(self.providers)
        if not providers:
            self.log.info("No CoverageProviders to run.")

        progress: list[CoverageProviderProgress | None] = []
        while providers:
            random.shuffle(providers)
            for provider in providers:
                self.log.debug("Running %s", provider.service_name)

                try:
                    provider_progress = provider.run_once_and_update_timestamp()
                    progress.append(provider_progress)
                except Exception as e:
                    self.log.error(
                        "Error in %r, moving on to next CoverageProvider.",
                        provider,
                        exc_info=e,
                    )

                self.log.debug("Completed %s", provider.service_name)
                providers.remove(provider)
        return progress


class RunCollectionCoverageProviderScript(RunCoverageProvidersScript):
    """Run the same CoverageProvider code for all Collections that
    get their licenses from the appropriate place.
    """

    def __init__(
        self,
        provider_class: type[CollectionCoverageProvider] | None,
        _db: Session | None = None,
        providers: Sequence[BaseCoverageProvider] | None = None,
        **kwargs: Any,
    ) -> None:
        _db = _db or self._db
        providers = list(providers or [])
        if provider_class:
            providers += self.get_providers(_db, provider_class, **kwargs)
        super().__init__(providers, _db=_db)

    def get_providers(
        self,
        _db: Session,
        provider_class: type[CollectionCoverageProvider],
        **kwargs: Any,
    ) -> list[BaseCoverageProvider]:
        return list(provider_class.all(_db, **kwargs))


class RunCoverageProviderScript(IdentifierInputScript):
    """Run a single coverage provider."""

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        parser = IdentifierInputScript.arg_parser(_db)
        parser.add_argument(
            "--cutoff-time",
            help="Update existing coverage records if they were originally created after this time.",
        )
        return parser

    @classmethod
    def parse_command_line(
        cls,
        _db: Session,
        cmd_args: Sequence[str | None] | None = None,
        stdin: SupportsReadlines = sys.stdin,
        *args: Any,
        **kwargs: Any,
    ) -> argparse.Namespace:
        parser = cls.arg_parser(_db)
        parsed = parser.parse_args(_normalize_cmd_args(cmd_args))
        stdin_lines = cls.read_stdin_lines(stdin)
        parsed = cls.look_up_identifiers(_db, parsed, stdin_lines, *args, **kwargs)
        if parsed.cutoff_time:
            parsed.cutoff_time = cls.parse_time(parsed.cutoff_time)
        return parsed

    def __init__(
        self,
        provider: (
            IdentifierCoverageProvider | Callable[..., IdentifierCoverageProvider]
        ),
        _db: Session | None = None,
        cmd_args: Sequence[str | None] | None = None,
        *provider_args: Any,
        **provider_kwargs: Any,
    ) -> None:
        super().__init__(_db)
        parsed_args = self.parse_command_line(self._db, cmd_args)
        if parsed_args.identifier_type:
            self.identifier_type = parsed_args.identifier_type
            self.identifier_types = [self.identifier_type]
        else:
            self.identifier_type = None
            self.identifier_types = []

        if parsed_args.identifiers:
            self.identifiers: list[Identifier] = parsed_args.identifiers
        else:
            self.identifiers = []

        if callable(provider):
            kwargs = self.extract_additional_command_line_arguments()
            kwargs.update(provider_kwargs)

            provider = provider(
                self._db, *provider_args, cutoff_time=parsed_args.cutoff_time, **kwargs
            )
        self.provider = provider
        self.name = self.provider.service_name

    def extract_additional_command_line_arguments(self) -> dict[str, list[Identifier]]:
        """A hook method for subclasses.

        Turns command-line arguments into additional keyword arguments
        to the CoverageProvider constructor.

        By default, pass in a value used only by CoverageProvider
        """
        return {
            "input_identifiers": self.identifiers,
        }

    def do_run(self) -> None:
        if self.identifiers:
            self.provider.run_on_specific_identifiers(self.identifiers)
        else:
            self.provider.run()
