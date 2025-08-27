from __future__ import annotations

from typing import Literal, overload

from pydantic import BaseModel, ConfigDict
from sqlalchemy.orm import Session

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
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

    _data_source: DataSource | None = None
    _primary_identifier: Identifier | None = None

    data_source_name: str
    primary_identifier_data: IdentifierData | None = None

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
