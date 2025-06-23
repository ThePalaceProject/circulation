from functools import partial

import pytest

from palace.manager.api.axis.constants import ServerNickname
from palace.manager.api.axis.settings import Axis360Settings
from palace.manager.util.problem_detail import ProblemDetailException


def test__migrate_url_to_server_nickname() -> None:
    create_settings = partial(
        Axis360Settings,
        username="testuser",
        password="testpass",
        external_account_id="testlibrary",
    )

    settings = create_settings(
        url="https://axis360apiqa.baker-taylor.com/Services/VendorAPI.svc",
    )
    assert settings.server_nickname == ServerNickname.qa

    settings = create_settings(
        url="https://axis360apiqa.baker-taylor.com/Services/VendorAPI/",
    )
    assert settings.server_nickname == ServerNickname.qa

    settings = create_settings(
        url="https://axis360apiqa.baker-taylor.com/Services/VendorAPI/",
        server_nickname=ServerNickname.production,
    )
    assert settings.server_nickname == ServerNickname.production

    settings = create_settings()
    assert settings.server_nickname == ServerNickname.production

    settings = create_settings(
        server_nickname=ServerNickname.qa,
    )
    assert settings.server_nickname == ServerNickname.qa

    with pytest.raises(ProblemDetailException, match="Invalid configuration option"):
        create_settings(url="http://questionable-url.com/?")
