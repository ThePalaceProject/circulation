from __future__ import annotations

from datetime import datetime
from typing import Literal, overload

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, PrivateAttr
from sqlalchemy.orm import Session

from palace.util.exceptions import PalaceValueError
from palace.util.log import LoggerMixin

from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.json import json_hash
from palace.manager.util.log import LoggerMixin


class BaseMutableData(BaseModel, LoggerMixin):
    model_config = ConfigDict(
        frozen=False,
        # We set validate_assignment to True to ensure that the data is validated
        # when we are building up the model incrementally. This has performance implications
        # and means we do a lot of validation work on every assignment.
        # If we see performance problems, we may want to revisit this and find a better
        # way to make sure the model is consistent.
        validate_assignment=True,
    )

    _data_source: DataSource | None = PrivateAttr(default=None)
    _primary_identifier: Identifier | None = PrivateAttr(default=None)
    # Lazily computed and cached for the lifetime of this object. Cleared by
    # __setattr__ whenever a public field is mutated so the hash stays consistent.
    _hash_cache: str | None = PrivateAttr(default=None)

    data_source_name: str
    primary_identifier_data: IdentifierData | None = None
    updated_at: AwareDatetime | None = None
    """
    The time at which the data source claims this information was last updated.
    This may be None if the data source does not provide this information.
    """

    created_at: AwareDatetime = Field(default_factory=utc_now)
    """
    The time at which this object was created. This is set automatically when
    the object is created and should not be modified.
    """

    @overload
    def load_data_source(
        self, _db: Session, autocreate: Literal[True] = ...
    ) -> DataSource: ...

    @overload
    def load_data_source(self, _db: Session, autocreate: bool) -> DataSource | None: ...

    def load_data_source(
        self, _db: Session, autocreate: bool = True
    ) -> DataSource | None:
        """Find the DataSource associated with this circulation information."""
        if self._data_source is None:
            obj = DataSource.lookup(_db, self.data_source_name, autocreate=autocreate)
            self._data_source = obj
            return obj
        return self._data_source

    @overload
    def load_primary_identifier(
        self, _db: Session, autocreate: Literal[True] = ...
    ) -> Identifier: ...

    @overload
    def load_primary_identifier(
        self, _db: Session, autocreate: bool
    ) -> Identifier | None: ...

    def load_primary_identifier(
        self, _db: Session, autocreate: bool = True
    ) -> Identifier | None:
        """Find the Identifier associated with this data."""
        if self._primary_identifier is None:
            if self.primary_identifier_data:
                obj, ignore = self.primary_identifier_data.load(
                    _db, autocreate=autocreate
                )
                self._primary_identifier = obj
                return obj
            else:
                raise PalaceValueError("No primary identifier provided!")
        return self._primary_identifier

    @property
    def as_of_timestamp(self) -> datetime:
        """The most recent timestamp associated with this data."""
        return self.updated_at if self.updated_at is not None else self.created_at

    def fields_excluded_from_hash(self) -> set[str]:
        """
        Return a set of field names that should be excluded from the hash calculation.

        This is useful for fields that are expected to change frequently but do not
        represent a meaningful change in the data, such as timestamps.

        ``updated_at`` is excluded because it is already used in the timestamp comparison
        inside :meth:`should_apply_to`, and including it in the hash would cause spurious
        re-imports when the content is identical but the source timestamp advances.
        """
        return {"created_at", "updated_at"}

    def __setattr__(self, name: str, value: object) -> None:
        """Invalidate the cached hash whenever a public field is mutated."""
        super().__setattr__(name, value)
        if not name.startswith("_"):
            self._hash_cache = None

    def calculate_hash(self) -> str:
        """Calculate a hash of the data in this object.

        The result is cached on the instance and reused across multiple calls
        within the same import cycle. The cache is invalidated automatically
        whenever a public field is mutated via attribute assignment.

        :return: A lowercase hex-encoded SHA-256 digest string.
        """
        if self._hash_cache is None:
            self._hash_cache = json_hash(
                self.model_dump(mode="json", exclude=self.fields_excluded_from_hash())
            )
        return self._hash_cache

    def should_apply_to(self, db_object: Edition | LicensePool | None = None) -> bool:
        """
        Does this data represent information more recent than what is stored in
        the given db object? Does the information appear to have changed?
        """
        if (
            db_object is None
            or db_object.updated_at is None
            or db_object.updated_at_data_hash is None
        ):
            # We don't have a db object, or the db object has never been updated.
            # We should apply this data.
            return True

        if self.as_of_timestamp < db_object.updated_at:
            # The data we have is strictly older than what is stored, no update needed.
            return False

        return self.calculate_hash() != db_object.updated_at_data_hash
