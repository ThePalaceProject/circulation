from dependency_injector import providers
from dependency_injector.containers import DeclarativeContainer
from dependency_injector.providers import Container

from core.service.analytics.configuration import AnalyticsConfiguration
from core.service.analytics.container import AnalyticsContainer
from core.service.logging.configuration import LoggingConfiguration
from core.service.logging.container import Logging
from core.service.search.configuration import SearchConfiguration
from core.service.search.container import Search
from core.service.storage.configuration import StorageConfiguration
from core.service.storage.container import Storage


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


def wire_container(container: Services) -> None:
    container.wire(
        modules=[
            "api.axis",
            "api.bibliotheca",
            "api.circulation_manager",
            "api.enki",
            "api.odl",
            "api.overdrive",
            "core.feed.annotator.circulation",
            "core.feed.acquisition",
            "core.lane",
            "core.metadata_layer",
            "core.model.collection",
            "core.model.work",
            "core.query.customlist",
        ]
    )


def create_container() -> Services:
    container = Services()
    container.config.from_dict(
        {
            "storage": StorageConfiguration().dict(),
            "logging": LoggingConfiguration().dict(),
            "analytics": AnalyticsConfiguration().dict(),
            "search": SearchConfiguration().dict(),
        }
    )
    wire_container(container)
    return container


_container_instance = None


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
