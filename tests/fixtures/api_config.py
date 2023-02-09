import json
from textwrap import dedent

import pytest
from Crypto.PublicKey.RSA import import_key


def get_mock_config_key_pair():
    key_string = """\
        -----BEGIN RSA PRIVATE KEY-----
        MIICXAIBAAKBgQCojFeEmXs1QJpR6lKvSEQG3HOmArwP0hu/gcIJybA44uvo7u13
        olFQtRIE3B2QzpHSWuq7vuo22vu/o+O3O8DMqniQJdzwlYuZABdfy5RNYkgMVyVm
        QAQy4s6MqpDST9M/V3vby8EOFXSsl1XEZLA0kGp+9f6xYaZAdgvw70vrtwIDAQAB
        AoGADokr8w+ZhJoxtObUgrFkzIKupp6NwX+JTRbPuTBftkg7uDcC29Jv2NoE185z
        7k0iXlXg8JgicuCn3Xsw5FiO4/PrFIXvUmYD1C3FFPuz15a77v3KbNLyK8iunhRm
        rtfk5ZwnGYDTL2B+UfhBMVp++pE954MPGPX65eEvbapa8+ECQQDRvpr/Op5oxS3m
        zB56CKT5099VlmOwJdyNr2T2nr3JGlupak8bdftzIPPJIZcXJsgBqa+bjw+GzaIj
        XnV9GJFzAkEAzbfx2ALfP+Dzode4276Znb2nlVVHKkZjnDbx8D4G0kisjiZPRhf4
        6/0CPu621CJ39i6/uoWNshaWKpQrd5ubrQJAOtlT+9CiqZrJajxTQMI0J7R+sTDk
        /4NgApD3rwqTDV3L4hjl4TqVNpREUmaOUfybmXvWvbrCDHydxXa3WEYQaQJBAKgV
        EkHnXtdHimiC5KSO3961dfaavdG5v2uErTsYwuQPwwVGIeFodtcCW1JmIvXCz/dS
        jQ7uTi7jK4DQnY9VkeUCQGRc7d2gONaHq6pP6JkRSMKVc0eRyhPzPAL43PAXaLpE
        wv/qnmBP4FOvDvtTLiOPCziqJEg3PSnBAsiRCKstHUc=
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


@pytest.fixture
def mock_config_key_pair():
    """
    Key pair generation takes a significant amount of time, and has to be done each time
    we setup for testing. This mocks out the RSA generation function to reduce the amount
    of time tests take.

    The key below is just a dummy key used for testing.
    """

    from _pytest.monkeypatch import MonkeyPatch

    patch = MonkeyPatch()
    patch.setattr("api.config.Configuration.key_pair", get_mock_config_key_pair())
    yield
    patch.undo()
