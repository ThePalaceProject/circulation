import pytest
from pydantic import TypeAdapter, ValidationError

from palace.manager.opds.types.currency import CurrencyCode


class TestCurrency:
    def test_validation(self):
        ta = TypeAdapter(CurrencyCode)

        assert ta.validate_python("USD") == "USD"
        assert ta.validate_python("usd") == "usd"
        assert ta.validate_python("eur") == "eur"
        assert ta.validate_python("EUR") == "EUR"

    def test_validation_failure(self):
        ta = TypeAdapter(CurrencyCode)

        with pytest.raises(ValidationError, match="Invalid currency code"):
            ta.validate_python("foo")
