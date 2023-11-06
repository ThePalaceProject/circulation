import pytest

from core.service.container import container_instance


@pytest.fixture(autouse=True)
def services_container_instance():
    # This creates and wires the container
    return container_instance()
