from __future__ import annotations

from abc import ABC, abstractmethod

from sqlalchemy.orm import Session

from palace.manager.opds.palace_authentication import PalaceAuthentication


class OPDSAuthenticationFlow(ABC):
    """An object that can be represented as an Authentication Flow
    in an Authentication For OPDS document.
    """

    @property
    @abstractmethod
    def flow_type(self) -> str:
        """The value of the `type` field in an Authentication Flow
        document.
        """
        ...

    def authentication_flow_document(self, _db: Session) -> PalaceAuthentication:
        """Convert this object into a :class:`PalaceAuthentication` model
        for use in the ``authentication`` list of an Authentication For
        OPDS document.
        """
        return self._authentication_flow_document(_db)

    @abstractmethod
    def _authentication_flow_document(self, _db: Session) -> PalaceAuthentication: ...
