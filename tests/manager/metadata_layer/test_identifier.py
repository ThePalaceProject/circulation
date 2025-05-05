from palace.manager.metadata_layer.identifier import IdentifierData
from palace.manager.sqlalchemy.model.identifier import Identifier


class TestIdentifierData:
    def test_constructor(self):
        data = IdentifierData(Identifier.ISBN, "foo", 0.5)
        assert Identifier.ISBN == data.type
        assert "foo" == data.identifier
        assert 0.5 == data.weight
