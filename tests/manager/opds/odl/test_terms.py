from datetime import date, datetime, timezone

import pytest
from pydantic import ValidationError

from palace.manager.opds.odl.terms import Terms


class TestTerms:
    def test_expires(self) -> None:
        """
        Test that expires can either be a datetime with a timezone set, or a date.
        """

        terms = Terms.model_validate(
            {
                "expires": "2022-01-01",
            }
        )
        assert isinstance(terms.expires, date)
        assert terms.expires == date(2022, 1, 1)
        assert terms.expires_datetime == datetime(
            2022, 1, 1, 0, 0, 0, tzinfo=timezone.utc
        )

        terms = Terms.model_validate(
            {
                "expires": "2022-01-01T00:00:00Z",
            }
        )
        assert isinstance(terms.expires, datetime)
        assert terms.expires == datetime(2022, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert terms.expires_datetime == terms.expires

        with pytest.raises(ValidationError):
            Terms.model_validate(
                {
                    "expires": "2022-01-01T01:00:00",
                }
            )
