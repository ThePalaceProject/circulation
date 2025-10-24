# These constants are put into a _version.py file by the
# docker build. If they are present, then we want to import
# them here, so they can be used by the application.

try:
    from palace.manager._version import __version__
except (ModuleNotFoundError, ImportError):
    __version__ = None

try:
    from palace.manager._version import __commit__
except (ModuleNotFoundError, ImportError):
    __commit__ = None

try:
    from palace.manager._version import __branch__
except (ModuleNotFoundError, ImportError):
    __branch__ = None

__all__ = ["__version__", "__commit__", "__branch__"]
