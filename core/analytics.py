from __future__ import annotations

import importlib
import logging
from collections import defaultdict
from typing import Any, Dict, Optional, Set

from sqlalchemy.orm.session import Session

from .config import CannotLoadConfiguration
from .model import ExternalIntegration
from .service.container import container_instance
from .util.datetime_helpers import utc_now
from .util.log import log_elapsed_time


class Analytics:
    """Loads configuration and dispatches methods for analytics providers.

    SINGLETON!! Only one instance is meant to exist at any given time.

    Configuration is loaded only on the first instantiation or when
    `refresh=True` is passed in to facilitate reload.
    """

    _singleton_instance = None
    log = logging.getLogger("core.analytics.Analytics")

    GLOBAL_ENABLED: Optional[bool] = None
    LIBRARY_ENABLED: Set[int] = set()

    def __new__(cls, _db: Session, refresh: bool = False) -> Analytics:
        instance = cls._singleton_instance
        if instance is None:
            refresh = True
            instance = super().__new__(cls)
            cls._singleton_instance = instance
            cls.log.debug("Set singleton instance.")
        if refresh:
            instance._initialize_instance(_db)
        return instance

    @classmethod
    def _reset_singleton_instance(cls):
        """Reset the singleton instance. Primarily used for tests."""
        cls.log.debug("Resetting singleton instance (should be used only for tests).")
        cls._singleton_instance = None

    @log_elapsed_time(log_method=log.debug, message_prefix="Initializing instance")
    def _initialize_instance(self, _db: Session) -> None:
        """Initialize an instance (usually the singleton) of the class.

        We don't use __init__ because it would be run whether or not
        a new instance were instantiated.
        """
        services = container_instance()
        sitewide_providers = []
        library_providers = defaultdict(list)
        initialization_exceptions: Dict[int, Exception | str] = {}
        global_enabled = False
        library_enabled = set()
        # Find a list of all the ExternalIntegrations set up with a
        # goal of analytics.
        integrations = _db.query(ExternalIntegration).filter(
            ExternalIntegration.goal == ExternalIntegration.ANALYTICS_GOAL
        )
        # Turn each integration into an analytics provider.
        for integration in integrations:
            module = integration.protocol
            libraries = integration.libraries
            try:
                provider_class = self._provider_class_from_module(module)
                if provider_class:
                    if not libraries:
                        provider = provider_class(integration, services)
                        sitewide_providers.append(provider)
                        global_enabled = True
                    else:
                        for library in libraries:
                            provider = provider_class(integration, services, library)
                            library_providers[library.id].append(provider)
                            library_enabled.add(library.id)
                else:
                    initialization_exceptions[integration.id] = (
                        "Module %s does not have Provider defined." % module
                    )
                self.log.info(
                    "Provider {provider!r} for protocol {protocol!r} has {scope} scope.".format(
                        protocol=module,
                        provider=provider_class.__name__,
                        scope=f"per-library ({len(libraries)})"
                        if libraries
                        else "site-wide",
                    )
                )
            except (ImportError, CannotLoadConfiguration) as e:
                initialization_exceptions[integration.id] = e

        # update the instance variables all at once
        self.sitewide_providers = sitewide_providers
        self.library_providers = library_providers
        self.initialization_exceptions = initialization_exceptions
        Analytics.GLOBAL_ENABLED = global_enabled
        Analytics.LIBRARY_ENABLED = library_enabled

    @classmethod
    def _provider_class_from_module(cls, module: str) -> Any:
        # Relative imports, which should be configured only during testing, are
        # relative to this module. sys.path will handle the absolute imports.
        import_kwargs = {"package": __name__} if module.startswith(".") else {}
        provider_module = importlib.import_module(module, **import_kwargs)
        return getattr(provider_module, "Provider", None)

    def collect_event(self, library, license_pool, event_type, time=None, **kwargs):
        if not time:
            time = utc_now()
        providers = list(self.sitewide_providers)
        if library:
            providers.extend(self.library_providers[library.id])
        for provider in providers:
            provider.collect_event(library, license_pool, event_type, time, **kwargs)

    @classmethod
    def is_configured(cls, library):
        if cls.GLOBAL_ENABLED is None:
            Analytics(Session.object_session(library))
        if cls.GLOBAL_ENABLED:
            return True
        else:
            return library.id in cls.LIBRARY_ENABLED
