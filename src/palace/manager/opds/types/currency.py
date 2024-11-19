from typing import Any

import pycountry
from pydantic import GetCoreSchemaHandler
from pydantic_core import PydanticCustomError, core_schema


class CurrencyCode(str):
    """Currency parses currency subset of the [ISO 4217](https://en.wikipedia.org/wiki/ISO_4217) format.

    This very similar to: https://docs.pydantic.dev/latest/api/pydantic_extra_types_currency_code/
    But we can't use that type from the 'pydantic_extra_types' package because it expects the currency
    code to be uppercase.

    All the examples we have of currency codes in existing feeds are lowercase. So we need a custom
    type to allow lowercase currency codes.
    """

    @classmethod
    def _validate(cls, currency_symbol: str, _: core_schema.ValidationInfo) -> str:
        """
        Validate a currency code in [ISO4217](https://en.wikipedia.org/wiki/ISO_4217) format.

        Args:
            currency_symbol: The str value to be validated.
            _: The Pydantic ValidationInfo.

        Returns:
            The validated ISO 4217 currency code.

        Raises:
            PydanticCustomError: If the ISO 4217 currency code is not valid.
        """
        if pycountry.currencies.get(alpha_3=currency_symbol) is None:
            raise PydanticCustomError(
                "InvalidCurrency",
                "Invalid currency code."
                " See https://en.wikipedia.org/wiki/ISO_4217. ",
            )
        return currency_symbol

    @classmethod
    def __get_pydantic_core_schema__(
        cls, _: type[Any], __: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        """
        Return a Pydantic CoreSchema for validating this object.
        """
        return core_schema.with_info_after_validator_function(
            cls._validate,
            core_schema.str_schema(min_length=3, max_length=3),
        )
