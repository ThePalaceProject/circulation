from dependency_injector import providers
from dependency_injector.containers import DeclarativeContainer

from core.service.storage.configuration import StorageConfiguration
from core.service.storage.container import Storage


class Services(DeclarativeContainer):

    config = providers.Configuration()

    storage = providers.Container(
        Storage,
        config=config.storage,
    )


def create_container() -> Services:
    container = Services()
    container.config.from_dict({"storage": StorageConfiguration().dict()})
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
