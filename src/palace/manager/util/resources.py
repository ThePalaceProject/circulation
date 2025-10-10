from importlib.resources import files
from importlib.resources.abc import Traversable


def resources_dir(subdir: str) -> Traversable:
    main_dir = files("palace.manager")
    return main_dir / "resources" / subdir
