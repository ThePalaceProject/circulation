import datetime

import pytest
from freezegun import freeze_time
from pydantic import ValidationError

from palace.manager.api.model.token import OAuthTokenResponse
from tests.fixtures.files import BoundlessFilesFixture


class TestOAuthTokenResponse:
    def test_token_response(self, boundless_files_fixture: BoundlessFilesFixture):
        data = boundless_files_fixture.sample_data("token.json")
        parsed = OAuthTokenResponse.model_validate_json(data)

        assert parsed.access_token == (
            "gAAAACHFFENqnHA709FcDPNQCaaTRE3bJoI-gODKxUm9_LvZd837GmqzgMA4WnLQGI"
            "HunW1PatTuSx3ECPSxidxx3TNtm11a8tsas2S1Qy0hv2QPAYiVLuTQ9u1Wc8f9jL08"
            "nVS5PHPrBZG5zyq9NvkzAch9bCRyEYLDxxZvrK634y1hNAEAAIAAAABSwHdTefBbCP"
            "id1-9RUcDWY3gTNLu-qvOkUnGvAj-0W8aSt84hB2bMFMuB5c7KVR-2j2yM3dW2ICJI"
            "cKP2JRrLdn7k9j6L0z1Ia5lnW2hmKEPoXDF7l8q891JwPk62sVksIupD1kWqpzrbGr"
            "txnZynj2h6-WGueukOLYIqbNPZQkNRf-LsXUGOOzDjVis9WtHNc2QOCxu0YgU6N00Y"
            "R-6j2yoOcQ3qDq_hK_WwZq2S_W_k2UqhpHcWljiOFstqEWzxh44MYOsokQuIcU1TS8"
            "GClyS_YuSrK1SI9tBx1aj9vGq6WzaxTXMaG8Wx_wtm_9MPi743y0Hs5whaBZlYs2-K"
            "SBKbQtiKPx7bbVEjXpfCrXq_eVukYsw6fNjI12B0M8rZm1TB9TDRnOyg1Mkhn0C-"
        )
        assert parsed.token_type == "Bearer"
        assert parsed.expires_in == 600
        assert not parsed.expired

    def test_token_expired(self) -> None:
        with freeze_time() as frozen_time:
            token = OAuthTokenResponse(
                access_token="token", token_type="Bearer", expires_in=400
            )
            assert not token.expired

            frozen_time.tick(delta=datetime.timedelta(seconds=400))
            assert token.expired

    @pytest.mark.parametrize("token_type", ["Bearer", "bearer", "BEARER", "bEaReR"])
    def test_token_type_case_insensitive(self, token_type: str) -> None:
        token = OAuthTokenResponse.model_validate(
            {"access_token": "token", "token_type": token_type, "expires_in": 600}
        )
        assert token.token_type == "Bearer"

    def test_token_type_invalid(self) -> None:
        with pytest.raises(ValidationError, match="token_type"):
            OAuthTokenResponse.model_validate(
                {"access_token": "token", "token_type": "mac", "expires_in": 600}
            )

    def test_scope(self) -> None:
        token = OAuthTokenResponse(
            access_token="token", token_type="Bearer", expires_in=600
        )
        assert token.scope is None

        token = OAuthTokenResponse.model_validate(
            {
                "access_token": "token",
                "token_type": "bearer",
                "expires_in": 600,
                "scope": "websiteid:12345 authorizationname:default",
            }
        )
        assert token.scope == "websiteid:12345 authorizationname:default"
