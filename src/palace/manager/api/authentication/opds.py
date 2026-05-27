from __future__ import annotations

from abc import ABC, abstractmethod

from sqlalchemy.orm import Session

from palace.opds.authentication.document import PalaceAuthentication


class OPDSAuthenticationFlow(ABC):
    """An object that can be represented as an Authentication Flow
    in an Authentication For OPDS document.
    """

    @property
    @abstractmethod
    def flow_type(self) -> str:
        """The value of the ``type`` field in an Authentication Flow object."""
        ...

    def authentication_flow_document(self, _db: Session) -> PalaceAuthentication:
        """Build the :class:`PalaceAuthentication` for this object's flow.

        Subclasses build the flow in :meth:`_authentication_flow_document`. This
        method guarantees the flow's ``type`` matches :attr:`flow_type`.
        """
        flow = self._authentication_flow_document(_db)
        if flow.type != self.flow_type:
            flow = flow.model_copy(update={"type": self.flow_type})
        return flow

    @abstractmethod
    def _authentication_flow_document(self, _db: Session) -> PalaceAuthentication:
        """Build the authentication flow object for this provider."""
        ...
