import random
import sys

from palace.manager.scripts.base import Script
from palace.manager.scripts.input import IdentifierInputScript


class RunCoverageProvidersScript(Script):
    """Alternate between multiple coverage providers."""

    def __init__(self, providers, _db=None):
        super().__init__(_db=_db)
        self.providers = []
        for i in providers:
            if callable(i):
                i = i(self._db)
            self.providers.append(i)

    def do_run(self):
        providers = list(self.providers)
        if not providers:
            self.log.info("No CoverageProviders to run.")

        progress = []
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

    def __init__(self, provider_class, _db=None, providers=None, **kwargs):
        _db = _db or self._db
        providers = providers or list()
        if provider_class:
            providers += self.get_providers(_db, provider_class, **kwargs)
        super().__init__(providers, _db=_db)

    def get_providers(self, _db, provider_class, **kwargs):
        return list(provider_class.all(_db, **kwargs))


class RunWorkCoverageProviderScript(RunCollectionCoverageProviderScript):
    """Run a WorkCoverageProvider on every relevant Work in the system."""

    # This class overrides RunCollectionCoverageProviderScript just to
    # take advantage of the constructor; it doesn't actually use the
    # concept of 'collections' at all.

    def get_providers(self, _db, provider_class, **kwargs):
        return [provider_class(_db, **kwargs)]


class RunCoverageProviderScript(IdentifierInputScript):
    """Run a single coverage provider."""

    @classmethod
    def arg_parser(cls):
        parser = IdentifierInputScript.arg_parser()
        parser.add_argument(
            "--cutoff-time",
            help="Update existing coverage records if they were originally created after this time.",
        )
        return parser

    @classmethod
    def parse_command_line(cls, _db, cmd_args=None, stdin=sys.stdin, *args, **kwargs):
        parser = cls.arg_parser()
        parsed = parser.parse_args(cmd_args)
        stdin = cls.read_stdin_lines(stdin)
        parsed = cls.look_up_identifiers(_db, parsed, stdin, *args, **kwargs)
        if parsed.cutoff_time:
            parsed.cutoff_time = cls.parse_time(parsed.cutoff_time)
        return parsed

    def __init__(
        self, provider, _db=None, cmd_args=None, *provider_args, **provider_kwargs
    ):
        super().__init__(_db)
        parsed_args = self.parse_command_line(self._db, cmd_args)
        if parsed_args.identifier_type:
            self.identifier_type = parsed_args.identifier_type
            self.identifier_types = [self.identifier_type]
        else:
            self.identifier_type = None
            self.identifier_types = []

        if parsed_args.identifiers:
            self.identifiers = parsed_args.identifiers
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

    def extract_additional_command_line_arguments(self):
        """A hook method for subclasses.

        Turns command-line arguments into additional keyword arguments
        to the CoverageProvider constructor.

        By default, pass in a value used only by CoverageProvider
        (as opposed to WorkCoverageProvider).
        """
        return {
            "input_identifiers": self.identifiers,
        }

    def do_run(self):
        if self.identifiers:
            self.provider.run_on_specific_identifiers(self.identifiers)
        else:
            self.provider.run()
