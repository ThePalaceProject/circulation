from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.sqlalchemy.model.identifier import Identifier
from tests.fixtures.database import DatabaseTransactionFixture


class TestIdentifierData:
    def test_hash(self) -> None:
        # Test that IdentifierData is hashable
        hash(IdentifierData(type=Identifier.ISBN, identifier="foo"))

    def test_constructor(self) -> None:
        data = IdentifierData(type=Identifier.ISBN, identifier="foo", weight=0.5)
        assert data.type == Identifier.ISBN
        assert data.identifier == "foo"
        assert data.weight == 0.5

    def test_from_identifier(self, db: DatabaseTransactionFixture) -> None:
        identifier = db.identifier()

        data = IdentifierData.from_identifier(identifier)
        assert data.type == identifier.type
        assert data.identifier == identifier.identifier

        # Calling from_identifier() on an IdentifierData object is a no-op
        # and returns the same object.
        assert IdentifierData.from_identifier(data) is data

    def test_parse_urn(self, db: DatabaseTransactionFixture) -> None:
        urn = "urn:isbn:9781449358068"
        data = IdentifierData.parse_urn(urn)
        assert data.type == Identifier.ISBN
        assert data.identifier == "9781449358068"

        # Test with a different type
        urn = db.fresh_url()
        data = IdentifierData.parse_urn(urn)
        assert data.type == "URI"
        assert data.identifier == urn
