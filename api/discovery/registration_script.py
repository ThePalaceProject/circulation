from __future__ import annotations

from argparse import ArgumentParser
from typing import Callable, List, Literal, Optional

from flask import url_for
from sqlalchemy.orm import Session

from api.config import Configuration
from api.controller import CirculationManager
from api.discovery.opds_registration import OpdsRegistrationService
from api.util.flask import PalaceFlask
from core.integration.goals import Goals
from core.model import ConfigurationSetting, ExternalIntegration, Library
from core.model.discoveryserviceregistration import RegistrationStage
from core.scripts import LibraryInputScript
from core.util.problem_detail import ProblemError


class LibraryRegistrationScript(LibraryInputScript):
    """Register local libraries with a remote library registry."""

    PROTOCOL = ExternalIntegration.OPDS_REGISTRATION
    GOAL = Goals.DISCOVERY_GOAL

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
        cmd_args: Optional[List[str]] = None,
        manager: Optional[CirculationManager] = None,
    ) -> PalaceFlask | Literal[False]:
        parsed = self.parse_command_line(self._db, cmd_args)

        url = parsed.registry_url
        registry = OpdsRegistrationService.for_protocol_goal_and_url(
            self._db, self.PROTOCOL, self.GOAL, url
        )
        if registry is None:
            self.log.error(f'No OPDS Registration service found for "{url}"')
            return False

        stage = RegistrationStage[parsed.stage]

        # Set up an application context so we have access to url_for.
        from api.app import app

        app.manager = manager or CirculationManager(self._db)
        base_url = ConfigurationSetting.sitewide(
            self._db, Configuration.BASE_URL_KEY
        ).value
        ctx = app.test_request_context(base_url=base_url)
        ctx.push()
        for library in parsed.libraries:
            self.process_library(registry, library, stage, url_for)
        ctx.pop()

        # For testing purposes, return the application object that was
        # created.
        return app

    def process_library(self, registry: OpdsRegistrationService, library: Library, stage: RegistrationStage, url_for: Callable[..., str]) -> bool:  # type: ignore[override]
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
            return False

        self.log.info("Success.")
        return True
