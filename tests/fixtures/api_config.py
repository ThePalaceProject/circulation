import json
from dataclasses import dataclass
from textwrap import dedent
from typing import Callable, Generator, Tuple

import pytest
from Crypto.PublicKey.RSA import import_key

from core.model import ConfigurationSetting


@dataclass(frozen=True)
class KeyPairFixture:
    public: str
    private: str
    json: str


def get_key_pair_fixture() -> KeyPairFixture:
    # Just a dummy key used for testing.
    key_string = """\
        -----BEGIN RSA PRIVATE KEY-----
        MIIBOQIBAAJBALFOBYf91uHhGQufTEOCZ9/L/Ge0/Lw4DRDuFBh9p+BpOxQJE9gi
        4FaJc16Wh53Sg5vQTOZMEGgjjTaP7K6NWgECAwEAAQJAEsR4b2meCjDCbumAsBCo
        oBa+c9fDfMTOFUGuHN2IHIe5zObxWAKD3xq73AO+mpeEl+KpeLeq2IJNqCZdf1yK
        MQIhAOGeurU6vgn/yA9gXECzvWYaxiAzHsOeW4RDhb/+14u1AiEAyS3VWo6jPt0i
        x8oiahujtCqaKLy611rFHQuK+yKNfJ0CIFuQVIuaNGfQc3uyCp6Dk3jtoryMoo6X
        JOLvmEdMAGQFAiB4D+psiQPT2JWRNokjWitwspweA8ReEcXhd6oSBqT54QIgaVc5
        wNybPDDs9mU+du+r0U+5iXaZzS5StYZpo9B4KjA=
        -----END RSA PRIVATE KEY-----
    """

    key = import_key(dedent(key_string))
    public_key = key.publickey().exportKey().decode("utf8")
    private_key = key.exportKey().decode("utf8")
    value = json.dumps([public_key, private_key])

    return KeyPairFixture(public_key, private_key, value)


def get_mock_config_key_pair(
    fixture: KeyPairFixture,
) -> Callable[[ConfigurationSetting], Tuple[str, str]]:
    def mock_key_pair(setting: ConfigurationSetting) -> Tuple[str, str]:
        public = None
        private = None

        try:
            public, private = setting.json_value
        except Exception as e:
            pass

        if not public or not private:
            setting.value = fixture.json
            public = fixture.public
            private = fixture.private

        return public, private

    return mock_key_pair


@pytest.fixture(scope="session")
def mock_config_key_pair() -> Generator[KeyPairFixture, None, None]:
    """
    Key pair generation takes a significant amount of time, and has to be done each time
    we set up for testing. This mocks out the Configuration.key_pair function to reduce the amount
    of time tests take.
    """

    # Need to import MonkeyPatch directly like this instead of using the monkeypatch fixture,
    # because this fixture has session scope, and the monkeypatch fixture doesn't.
    from _pytest.monkeypatch import MonkeyPatch

    fixture = get_key_pair_fixture()
    mock = get_mock_config_key_pair(fixture)

    patch = MonkeyPatch()
    patch.setattr("api.config.Configuration.key_pair", mock)
    yield fixture
    patch.undo()
