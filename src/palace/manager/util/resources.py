from importlib.abc import Traversable
from importlib.resources import files


def resources_dir(subdir: str) -> Traversable:
    main_dir = files("palace.manager")
    return main_dir / "resources" / subdir
