from __future__ import annotations

from argparse import ArgumentParser
from collections.abc import Callable
from typing import Any, Literal

from flask import url_for
from sqlalchemy.orm import Session

from palace.manager.api.circulation_manager import CirculationManager
from palace.manager.api.util.flask import PalaceFlask
from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.integration.discovery.opds_registration import (
    OpdsRegistrationService,
)
from palace.manager.integration.goals import Goals
from palace.manager.scripts.input import LibraryInputScript
from palace.manager.service.container import Services
from palace.manager.service.integration_registry.discovery import DiscoveryRegistry
from palace.manager.sqlalchemy.model.discovery_service_registration import (
    DiscoveryServiceRegistration,
    RegistrationStage,
)
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException


class LibraryRegistrationScript(LibraryInputScript):
    """Register local libraries with a remote library registry."""

    def __init__(
        self,
        _db: Session | None = None,
        services: Services | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(_db, services, *args, **kwargs)
        self.base_url = self.services.config.sitewide.base_url()
        if self.base_url is None:
            raise CannotLoadConfiguration(
                f"Missing required environment variable: PALACE_BASE_URL."
            )

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
        integration_registry: DiscoveryRegistry = (
            self.services.integration_registry.discovery()
        )
        protocol = integration_registry.get_protocol(OpdsRegistrationService)
        registry = OpdsRegistrationService.for_protocol_goal_and_url(
            self._db, protocol, Goals.DISCOVERY_GOAL, url
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
        from palace.manager.api.app import app

        app.manager = manager or CirculationManager(self._db)
        with app.test_request_context(base_url=self.base_url):
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
        except ProblemDetailException as e:
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
