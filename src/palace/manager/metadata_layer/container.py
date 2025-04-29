from __future__ import annotations

from pydantic import BaseModel, ConfigDict
from sqlalchemy.orm import Session

from palace.manager.metadata_layer.identifier import IdentifierData
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.util.log import LoggerMixin


class BaseDataContainer(BaseModel, LoggerMixin):
    model_config = ConfigDict(
        frozen=False,
        validate_assignment=True,
    )

    _data_source: DataSource | None = None
    _primary_identifier: Identifier | None = None

    data_source: str
    primary_identifier: IdentifierData | None = None

    def data_source_db(self, _db: Session) -> DataSource:
        """Find the DataSource associated with this circulation information."""
        if self._data_source is None:
            obj = DataSource.lookup(_db, self.data_source, autocreate=True)
            self._data_source = obj
            return obj
        return self._data_source

    def primary_identifier_db(self, _db: Session) -> Identifier:
        """Find the Identifier associated with this circulation information."""
        if self._primary_identifier is None:
            if self.primary_identifier:
                obj, ignore = self.primary_identifier.load(_db)
                self._primary_identifier = obj
                return obj
            else:
                raise ValueError("No primary identifier provided!")
        return self._primary_identifier
