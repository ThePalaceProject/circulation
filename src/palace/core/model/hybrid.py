from typing import TYPE_CHECKING

# Once we move to a newer version of SQLAlchemy with better type
# support we should be able to drop this.
# https://github.com/dropbox/sqlalchemy-stubs/issues/98
if TYPE_CHECKING:
    hybrid_property = property
else:
    from sqlalchemy.ext.hybrid import hybrid_property  # noqa: autoflake
