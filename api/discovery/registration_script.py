from __future__ import annotations

from argparse import ArgumentParser
from collections.abc import Callable
from typing import Literal

from flask import url_for
from sqlalchemy.orm import Session

from api.circulation_manager import CirculationManager
from api.config import Configuration
from api.discovery.opds_registration import OpdsRegistrationService
from api.integration.registry.discovery import DiscoveryRegistry
from api.util.flask import PalaceFlask
from core.integration.goals import Goals
from core.model import ConfigurationSetting, Library, get_one
from core.model.discovery_service_registration import (
    DiscoveryServiceRegistration,
    RegistrationStage,
)
from core.scripts import LibraryInputScript
from core.util.problem_detail import ProblemDetail, ProblemError


class LibraryRegistrationScript(LibraryInputScript):
    """Register local libraries with a remote library registry."""

    @classmethod
    def arg_parser(cls, _db: Session) -> ArgumentParser:  # type: ignore[override]
        parser = LibraryInputScript.arg_parser(_db)
        parser.add_argument(
            "--registry-url",
            help="Register libraries with the given registry.",
            default=OpdsRegistrationService.DEFAULT_LIBRARY_REGISTRY_URL,
        )
        parser.add_argument(
            "--stage",
            help="Register these libraries in the 'testing' stage or the 'production' stage.",
            choices=[stage.value for stage in RegistrationStage],
        )
        return parser  # type: ignore[no-any-return]

    def do_run(
        self,
        cmd_args: list[str] | None = None,
        manager: CirculationManager | None = None,
    ) -> PalaceFlask | Literal[False]:
        parsed = self.parse_command_line(self._db, cmd_args)

        url = parsed.registry_url
        protocol = DiscoveryRegistry().get_protocol(OpdsRegistrationService)
        registry = OpdsRegistrationService.for_protocol_goal_and_url(
            self._db, protocol, Goals.DISCOVERY_GOAL, url  # type: ignore[arg-type]
        )
        if registry is None:
            self.log.error(f'No OPDS Registration service found for "{url}"')
            return False

        try:
            stage = RegistrationStage(parsed.stage) if parsed.stage else None
        except ValueError:
            self.log.error(
                f'Invalid registration stage "{parsed.stage}". '
                f'Must be one of {", ".join([stage.value for stage in RegistrationStage])}.'
            )
            return False

        # Set up an application context so we have access to url_for.
        from api.app import app

        app.manager = manager or CirculationManager(self._db)
        base_url = ConfigurationSetting.sitewide(
            self._db, Configuration.BASE_URL_KEY
        ).value
        ctx = app.test_request_context(base_url=base_url)
        ctx.push()
        for library in parsed.libraries:
            if not stage:
                # Check if the library has already been registered.
                registration = get_one(
                    self._db,
                    DiscoveryServiceRegistration,
                    library=library,
                    integration=registry.integration,
                )
                if registration and registration.stage is not None:
                    library_stage = registration.stage
                else:
                    # Don't know what stage to register this library in, so it defaults to test.
                    library_stage = RegistrationStage.TESTING
            else:
                library_stage = stage

            self.process_library(registry, library, library_stage, url_for)
        ctx.pop()

        # For testing purposes, return the application object that was
        # created.
        return app

    def process_library(  # type: ignore[override]
        self,
        registry: OpdsRegistrationService,
        library: Library,
        stage: RegistrationStage,
        url_for: Callable[..., str],
    ) -> bool | ProblemDetail:
        """Push one Library's registration to the given OpdsRegistrationService."""

        self.log.info("Processing library %r", library.short_name)
        self.log.info("Registering with %s as %s", registry.settings.url, stage.value)
        try:
            registry.register_library(library, stage, url_for)
        except ProblemError as e:
            data, status_code, headers = e.problem_detail.response
            self.log.exception(
                "Could not complete registration. Problem detail document: %r" % data
            )
            return e.problem_detail
        except Exception as e:
            self.log.exception(f"Exception during registration: {e}")
            return False

        self.log.info("Success.")
        return True
