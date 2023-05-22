from argparse import Namespace
from io import StringIO
from unittest.mock import Mock, call, patch

import pytest
from Crypto.Cipher import AES

from core.jobs.integration_test import (
    FailedIntegrationTest,
    IntegrationTest,
    IntegrationTestDetails,
)
from tests.fixtures.database import DatabaseTransactionFixture

BASIC_YAML = """
---
- name: Test
  endpoint: localhost
"""
BASIC_YAML_DICT = [{"name": "Test", "endpoint": "localhost"}]


class IntegrationTestFixture:
    def __init__(self, db: DatabaseTransactionFixture) -> None:
        self.script = IntegrationTest(db.session)


@pytest.fixture(scope="function")
def integration_test(db: DatabaseTransactionFixture):
    return IntegrationTestFixture(db)


class TestIntegrationTest:
    def test_read_config(self, integration_test: IntegrationTestFixture):
        with patch("core.jobs.integration_test.open") as mock_open:
            mock_open.return_value = StringIO(BASIC_YAML)
            data = integration_test.script._read_config("on/disk/filepath")

            assert mock_open.call_count == 1
            assert mock_open.call_args == call("on/disk/filepath", "rb")
            assert data == BASIC_YAML_DICT

        with patch(
            "core.jobs.integration_test.HTTP.get_with_timeout"
        ) as get_with_timeout:
            response = get_with_timeout.return_value
            response.status_code = 400
            pytest.raises(Exception, integration_test.script._read_config, "http://...")
            assert get_with_timeout.call_count == 1

            get_with_timeout.reset_mock()
            response.status_code = 200
            response.content = StringIO(BASIC_YAML)
            data = integration_test.script._read_config("http://...")

            assert get_with_timeout.call_count == 1
            assert get_with_timeout.call_args == call("http://...")
            assert data == BASIC_YAML_DICT

    def test_read_config_decrypt(self, integration_test: IntegrationTestFixture):
        with patch("core.jobs.integration_test.read_file_bytes") as read_file_bytes:
            # Cipher content
            content = b"16bytespadded   "
            key = b"0" * 32
            iv = b"i" * 16
            cipher = AES.new(key, AES.MODE_CBC, iv=iv)
            encrypted = iv + cipher.encrypt(content)
            read_file_bytes.side_effect = [encrypted, key]

            # Decrypt during the read
            decrypted_data = integration_test.script._read_config(
                "on/disk/filepath", key_file="keyfile", raw=True
            )
            assert decrypted_data == content

    def test__run_test(self, integration_test: IntegrationTestFixture):
        with patch("core.jobs.integration_test.HTTP.request_with_timeout") as request:
            request.return_value = Mock(
                status_code=204, json=lambda: dict(status="created")
            )
            details = IntegrationTestDetails(
                name="test",
                endpoint="http://...",
                method="POST",
                request_headers=dict(Authorization="Basic Auth"),
                request_body=dict(key="value"),
                expected_json=dict(status="created"),
                expected_status_code=204,
            )
            integration_test.script._run_test(details)

            assert request.call_count == 1
            assert request.call_args == call(
                "POST",
                "http://...",
                headers=details.request_headers,
                data=details.request_body,
                json=None,
            )

            # Error tests
            # Incorrect status code
            request.reset_mock()
            request.return_value.status_code = 200
            with pytest.raises(FailedIntegrationTest) as raised:
                integration_test.script._run_test(details)

            assert str(raised.value) == "Status code 200 != 204"

            # Incorrect response body
            request.reset_mock()
            request.return_value.status_code = 204
            request.return_value.json = lambda: dict(status="deleted")

            with pytest.raises(FailedIntegrationTest) as raised:
                integration_test.script._run_test(details)

            assert (
                str(raised.value)
                == "JSON response did not match the expected: {'status': 'deleted'} != {'status': 'created'}"
            )

    def test_generate_key_file(self, integration_test: IntegrationTestFixture):
        with patch("core.jobs.integration_test.open") as open:
            integration_test.script._generate_key_file("keyfile")
        assert open.call_args == call("keyfile", "wb")
        assert len(open.return_value.__enter__.return_value.write.call_args[0][0]) == 32

    def test_encrypt(self, integration_test: IntegrationTestFixture):
        script = integration_test.script
        with (
            patch.object(script, "_read_config") as read_config,
            patch("core.jobs.integration_test.read_file_bytes") as read_file_bytes,
            patch("core.jobs.integration_test.get_random_bytes") as get_random_bytes,
            patch("core.jobs.integration_test.AES") as aes,
            patch("core.jobs.integration_test.open") as open,
        ):
            read_file_bytes.return_value = b"filebytes"
            read_config.return_value = b"7 bytes"
            get_random_bytes.return_value = b"RANDOMBYTESIV"
            aes.new.return_value.encrypt.return_value = b"encrypted bytes"
            script._encrypt("filepath", "keyfile", "encryptfile")

            # Assert the setup methods
            assert read_file_bytes.call_args == call("keyfile")
            assert aes.new.call_args == call(
                b"filebytes", aes.MODE_CBC, iv=b"RANDOMBYTESIV"
            )
            assert aes.new.return_value.encrypt.call_args == call(
                b"7 bytes" + (b" " * 9)
            )  # padded to 16 bytes
            assert open.call_args == call("encryptfile", "wb")

            # The encrypted values written
            assert open.return_value.__enter__.return_value.write.call_args == call(
                b"RANDOMBYTESIVencrypted bytes"
            )

    def test__do_run(self, integration_test: IntegrationTestFixture):
        script = integration_test.script
        script.parse_command_line = Mock()  # type: ignore

        script.parse_command_line.return_value = Namespace(generate_key_file="keyfile")
        with patch.object(script, "_generate_key_file") as generate_key_file:
            script.do_run()
            assert generate_key_file.call_count == 1
            assert generate_key_file.call_args == call("keyfile")

        script.parse_command_line.return_value = Namespace(
            config="configfile",
            key_file="keyfile",
            encrypt_file="encryptfile",
            generate_key_file=None,
        )
        with patch.object(script, "_encrypt") as encrypt:
            script.do_run()
            assert encrypt.call_count == 1
            assert encrypt.call_args == call("configfile", "keyfile", "encryptfile")

        script.parse_command_line.return_value = Namespace(
            config="configfile",
            key_file="keyfile",
            generate_key_file=None,
            encrypt_file=None,
        )
        with (
            patch.object(script, "_run_test") as run_test,
            patch.object(script, "_read_config") as read_config,
        ):
            read_config.return_value = BASIC_YAML_DICT
            script.do_run()

            assert read_config.call_args == call("configfile", key_file="keyfile")
            assert run_test.call_count == 1
            assert run_test.call_args == call(
                IntegrationTestDetails(**BASIC_YAML_DICT[0])  # type: ignore
            )

            # Test the exception case
            run_test.side_effect = FailedIntegrationTest(
                "Error", exception=Exception("...")
            )
            # Script does not fail
            script.do_run()
