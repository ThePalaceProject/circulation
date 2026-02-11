from dependency_injector import providers
from dependency_injector.containers import DeclarativeContainer
from dependency_injector.providers import Container

from palace.manager.service.analytics.configuration import AnalyticsConfiguration
from palace.manager.service.analytics.container import AnalyticsContainer
from palace.manager.service.celery.configuration import CeleryConfiguration
from palace.manager.service.celery.container import CeleryContainer
from palace.manager.service.email.configuration import EmailConfiguration
from palace.manager.service.email.container import Email
from palace.manager.service.fcm.configuration import FcmConfiguration
from palace.manager.service.fcm.container import FcmContainer
from palace.manager.service.google_drive.configuration import GoogleDriveConfiguration
from palace.manager.service.google_drive.container import GoogleDrive
from palace.manager.service.integration_registry.container import (
    IntegrationRegistryContainer,
)
from palace.manager.service.logging.configuration import LoggingConfiguration
from palace.manager.service.logging.container import Logging
from palace.manager.service.redis.configuration import RedisConfiguration
from palace.manager.service.redis.container import RedisContainer
from palace.manager.service.search.configuration import SearchConfiguration
from palace.manager.service.search.container import Search
from palace.manager.service.sitewide import SitewideConfiguration
from palace.manager.service.storage.configuration import StorageConfiguration
from palace.manager.service.storage.container import Storage


class Services(DeclarativeContainer):
    config = providers.Configuration()

    storage = Container(
        Storage,
        config=config.storage,
    )

    logging = Container(
        Logging,
        config=config.logging,
    )

    analytics = Container(
        AnalyticsContainer,
        config=config.analytics,
        storage=storage,
    )

    search = Container(
        Search,
        config=config.search,
    )

    email = Container(
        Email,
        config=config.email,
    )

    celery = Container(
        CeleryContainer,
        config=config.celery,
    )

    fcm = Container(
        FcmContainer,
        config=config.fcm,
    )

    integration_registry = Container(
        IntegrationRegistryContainer,
    )

    redis = Container(
        RedisContainer,
        config=config.redis,
    )

    google_drive = Container(
        GoogleDrive,
        config=config.google_drive,
    )


def wire_container(container: Services) -> None:
    container.wire(
        modules=[
            "palace.manager.api.circulation_manager",
            "palace.manager.feed.annotator.circulation",
            "palace.manager.feed.acquisition",
            "palace.manager.sqlalchemy.model.lane",
            "palace.manager.sqlalchemy.model.collection",
            "palace.manager.sqlalchemy.model.patron",
            "palace.manager.sqlalchemy.model.work",
        ]
    )


def create_container() -> Services:
    container = Services()
    container.config.from_dict(
        {
            "sitewide": SitewideConfiguration().model_dump(),
            "storage": StorageConfiguration().model_dump(),
            "logging": LoggingConfiguration().model_dump(),
            "analytics": AnalyticsConfiguration().model_dump(),
            "search": SearchConfiguration().model_dump(),
            "email": EmailConfiguration().model_dump(),
            "celery": CeleryConfiguration().model_dump(),
            "fcm": FcmConfiguration().model_dump(),
            "redis": RedisConfiguration().model_dump(),
            "google_drive": GoogleDriveConfiguration().model_dump(),
        }
    )
    wire_container(container)
    return container


_container_instance: Services | None = None


def container_instance() -> Services:
    # Create a singleton container instance, I'd like this to be used sparingly
    # and eventually have it go away, but there are places in the code that
    # are currently difficult to refactor to pass the container into the
    # constructor.
    # If at all possible please use the container that is stored in the CirculationManager
    # or Scripts classes instead of using this function.
    global _container_instance
    if _container_instance is None:
        _container_instance = create_container()
    return _container_instance
