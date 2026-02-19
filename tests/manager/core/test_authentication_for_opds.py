from unittest.mock import MagicMock

from sqlalchemy.orm import Session

from palace.manager.opds.palace_authentication import PalaceAuthentication
from palace.manager.util.authentication_for_opds import OPDSAuthenticationFlow


class MockFlow(OPDSAuthenticationFlow):
    """A mock OPDSAuthenticationFlow."""

    @property
    def flow_type(self) -> str:
        return "http://mock1/"

    def __init__(self, description: str):
        self.description = description

    def _authentication_flow_document(self, _db: Session) -> PalaceAuthentication:
        return PalaceAuthentication(
            type=self.flow_type,
            description=self.description,
        )


class TestOPDSAuthenticationFlow:
    def test_flow_returns_model_with_type(self):
        """An OPDSAuthenticationFlow returns a PalaceAuthentication model
        with the correct type.
        """
        flow = MockFlow("description")
        db = MagicMock(spec=Session)
        model = flow.authentication_flow_document(db)
        assert isinstance(model, PalaceAuthentication)
        assert model.type == "http://mock1/"
        assert model.description == "description"

    def test_flow_delegates_to_private_method(self):
        """authentication_flow_document delegates to _authentication_flow_document."""
        db = MagicMock(spec=Session)
        flow = MockFlow("description")
        expected = PalaceAuthentication(type="http://mock1/", description="test")
        flow._authentication_flow_document = MagicMock(return_value=expected)  # type: ignore[method-assign]
        result = flow.authentication_flow_document(db)
        assert result is expected
        flow._authentication_flow_document.assert_called_once_with(db)
