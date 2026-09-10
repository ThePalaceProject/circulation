# The one place this package's version is written down. Hatch reads it from
# here (see `[tool.hatch.version]`), the way palace-brainfuck-vm's comes from
# its Cargo.toml. Neither is published to PyPI, so neither takes part in the
# release-time version stamping that palace-util and palace-opds do.
__version__ = "0.1.0"

__all__ = ["__version__"]
