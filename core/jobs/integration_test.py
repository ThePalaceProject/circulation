from __future__ import annotations

import datetime
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from dataclasses import dataclass
from json import JSONDecodeError
from ssl import get_server_certificate
from typing import Any, List, cast
from urllib.parse import urlparse

import pytz
import yaml
from OpenSSL.crypto import FILETYPE_PEM, X509, load_certificate

from core.crypt.aes import CryptAESCBC
from core.scripts import Script
from core.util.datetime_helpers import utc_now
from core.util.http import (
    HTTP,
    BadResponseException,
    RequestNetworkException,
    RequestTimedOut,
)


@dataclass
class IntegrationTestDetails:
    """All the supported configurations"""

    name: str
    endpoint: str
    method: str = "GET"
    request_headers: dict | None = None
    request_body: Any | None = None
    request_json: dict | None = None
    expected_json: dict | None = None
    expected_status_code: int = 200


def read_file_bytes(filepath):
    with open(filepath, "rb") as fp:
        return fp.read()


class IntegrationTest(Script):
    CMDLINE_DESCRIPTION = "Tests the API integrations based on a yml configuration file"

    CMDLINE_USAGE = """
Generate a key file using `--generate-key-file <output filename>`.
Then encrypt your config file with `--config <input> --encrypt-file <encrypted output> --key-file <key file>`.
Running the tests via the encrypted config is done via `--config <input> --key-file <key file>`.
If --key-file is not present the script assumes this is an unencrypted config file."""

    CMDLINE_EPILOG = """
The config file format is a YML file of the form:
---
- name: str
  endpoint: str
  method: str = "GET"
  request_headers: dict = None
  request_body: Any = None
  request_json: dict = None
  expected_json: dict = None
  expected_status_code: int = 200"""

    SSL_EXPIRY_THRESHOLD_DAYS = 7

    @classmethod
    def arg_parser(cls):  # pragma: no cover
        parser = ArgumentParser(
            "Test API Integrations",
            description=cls.CMDLINE_DESCRIPTION,
            usage=cls.CMDLINE_USAGE,
            epilog=cls.CMDLINE_EPILOG,
            formatter_class=RawDescriptionHelpFormatter,
        )
        parser.add_argument(
            "--config",
            help="The location of the yml file describing the tests",
            required=False,
        )
        parser.add_argument(
            "--key-file",
            help="The location of the secret key file to decrypt the api file, if required",
        )
        parser.add_argument(
            "--generate-key-file",
            help="Generate an AES key file that can be used to encrypt config files by this script",
            required=False,
        )
        parser.add_argument(
            "--encrypt-file",
            help="Encrypt a config file using a key file (--key-file). Using AES.MODE_CBC.",
            required=False,
        )
        return parser

    def _read_config(
        self, filepath: str, key_file: str | None = None, raw: bool = False
    ) -> List | bytes:
        """Read the config yml from a source.
        The file should be a yml with a list of IntegrationTestDetails as the content.
        :param filepath: The path to the file, could be an URL or a file on the local directory
        :param secret_file: (Optional)The path to the secrets file to decrypt the config
        :param raw: (Optional) If True then do not parse the yml content, just return the text
        """
        if filepath.startswith("http"):
            response = HTTP.get_with_timeout(filepath)
            if response.status_code != 200:
                raise Exception(f"Could not read remote file: {response.content!r}")
            content = response.content
        else:
            content = read_file_bytes(filepath)

        # Decryption
        if key_file:
            key = read_file_bytes(key_file)
            content = CryptAESCBC(key).decrypt(content)

        if not raw:
            yml = yaml.load(content, yaml.Loader)
            return yml

        return content

    def _generate_key_file(self, filepath: str):
        """Generate a 32 byte random key file"""
        key = CryptAESCBC.generate_key()
        with open(filepath, "wb") as wfp:
            wfp.write(key)
        self.log.info(
            f"Successfully wrote a {CryptAESCBC.KEY_LENGTH} byte key into {filepath}"
        )

    def _encrypt(self, filepath: str, key_file: str, encrypt_file: str):
        """Encrypt a textfile and write it to the encrypt_file.
        :param filepath: The file to encrypt
        :param key_file: The key file to use for the encryption
        :param encrypt_file: The output file to write the encrypted content"""
        content = cast(bytes, self._read_config(filepath, raw=True))
        key = read_file_bytes(key_file)
        output = CryptAESCBC(key).encrypt(content)

        with open(encrypt_file, "wb") as efp:
            efp.write(output)

        self.log.info(f"Wrote the encrypted file to {encrypt_file}")

    def do_run(self) -> None:
        args = self.parse_command_line()

        if args.generate_key_file:
            self._generate_key_file(args.generate_key_file)
            return

        if args.encrypt_file:
            self._encrypt(args.config, args.key_file, args.encrypt_file)
            return

        data = cast(List[dict], self._read_config(args.config, key_file=args.key_file))

        for datapoint in data:
            test = IntegrationTestDetails(**datapoint)
            try:
                self._run_test(test)
            except FailedIntegrationTest as ex:
                self.log.error(
                    f"Test run failed for {test.name} {test.endpoint}: {ex.args[0]}"
                )
                if ex.exception:
                    self.log.error(f"Test run exception {ex.exception}")

    def _run_test(self, test: IntegrationTestDetails) -> None:
        """Run a single test defined by an IntegrationTestDetails object"""
        try:
            result = HTTP.request_with_timeout(
                test.method,
                test.endpoint,
                headers=test.request_headers or {},
                data=test.request_body,
                json=test.request_json,
            )
        except (RequestNetworkException, RequestTimedOut, BadResponseException) as ex:
            raise FailedIntegrationTest(
                f"Network Failure: {ex.url}", exception=ex.args[0]
            )

        # Run tests on the SSL certificate
        self._test_ssl_validity(test)

        # Test the status code
        if (
            test.expected_status_code
            and result.status_code != test.expected_status_code
        ):
            raise FailedIntegrationTest(
                f"Status code {result.status_code} != {test.expected_status_code}"
            )

        # Test the response body
        try:
            if test.expected_json and result.json() != test.expected_json:
                raise FailedIntegrationTest(
                    f"JSON response did not match the expected: {result.json()} != {test.expected_json}"
                )
        except JSONDecodeError:
            raise FailedIntegrationTest(
                f"Response incorrect: {result.content!r} not a valid JSON response"
            )

        self.log.info(f"Test run successful {test.name} {test.endpoint}")

    def _test_ssl_validity(self, test: IntegrationTestDetails):
        """Test the SSL certificate validity for the near future"""
        url = test.endpoint

        parsed = urlparse(url)
        if parsed.scheme != "https":
            return

        # Fetch and parse the certificate
        if parsed.hostname:
            context = get_server_certificate((parsed.hostname, parsed.port or 443))
        else:
            raise FailedIntegrationTest(f"Could not parse url structure {url}")

        if not context:
            raise FailedIntegrationTest(f"No SSL certificate found for {url}")

        cert: X509 = load_certificate(FILETYPE_PEM, context.encode())
        not_after = cert.get_notAfter()
        if not not_after:
            raise FailedIntegrationTest(f"No SSL expiry found for {url}")

        # Parse the expiry and make it TZ aware
        expires = datetime.datetime.strptime(not_after.decode(), "%Y%m%d%H%M%SZ")
        expires = expires.replace(tzinfo=pytz.UTC)

        # Validate the certificate
        if expires - utc_now() < datetime.timedelta(
            days=self.SSL_EXPIRY_THRESHOLD_DAYS
        ):
            raise FailedIntegrationTest(f"The SSL certificate expires on {expires}")


class FailedIntegrationTest(Exception):
    def __init__(self, *args: object, exception=None) -> None:
        self.exception = exception
        super().__init__(*args)
