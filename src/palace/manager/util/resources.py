import sys
from importlib.resources import files

if sys.version_info >= (3, 11):
    from importlib.resources.abc import Traversable
else:
    from importlib.abc import Traversable


def resources_dir(subdir: str) -> Traversable:
    main_dir = files("palace.manager")
    return main_dir / "resources" / subdir
