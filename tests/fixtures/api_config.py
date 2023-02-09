import json
from textwrap import dedent

import pytest
from Crypto.PublicKey.RSA import import_key


def get_mock_config_key_pair():
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

    def mock_key_pair(setting):
        public = None
        private = None

        try:
            public, private = setting.json_value
        except Exception as e:
            pass

        if not public or not private:
            setting.value = value
            public = public_key
            private = private_key

        return public, private

    return mock_key_pair


@pytest.fixture(scope="session")
def mock_config_key_pair():
    """
    Key pair generation takes a significant amount of time, and has to be done each time
    we setup for testing. This mocks out the Configuration.key_pair function to reduce the amount
    of time tests take.
    """

    from _pytest.monkeypatch import MonkeyPatch

    patch = MonkeyPatch()
    patch.setattr("api.config.Configuration.key_pair", get_mock_config_key_pair())
    yield
    patch.undo()
