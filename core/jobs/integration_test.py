from argparse import ArgumentParser
from dataclasses import dataclass
from json import JSONDecodeError
from typing import Any, List

import yaml

from core.scripts import Script
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
    request_headers: dict = None
    request_body: Any = None
    request_json: dict = None
    expected_json: dict = None
    expected_status_code: int = 200


class IntegrationTest(Script):
    @classmethod
    def arg_parser(cls):
        parser = ArgumentParser("Test Integrations")
        parser.add_argument(
            dest="config_path", help="The location of the yml file describing the tests"
        )
        parser.add_argument(
            "--secret-key-file",
            help="The location of the secret key file to decrypt the api file, if required",
        )
        return parser

    def _read_config(self, filepath: str, secret_file: str = None) -> List:
        """Read the config yml from a source.
        The file should be a yml with a list of IntegrationTestDetails as the content.
        :param filepath: The path to the file, could be an URL or a file on the local directory
        :param secret_file: (Optional)The path to the secrets file to decrypt the config
        """
        if filepath.startswith("http"):
            response = HTTP.get_with_timeout(filepath)
            if response.status_code != 200:
                raise Exception(f"Could not read remote file: {response.content}")
            content = response.content
        else:
            with open(filepath) as fp:
                content = fp.read()

        # TODO: Encryption
        if secret_file:
            pass

        yml = yaml.load(content, yaml.Loader)
        return yml

    def do_run(self) -> None:
        args = self.parse_command_line()
        data = self._read_config(args.config_path)

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
                f"Response incorrect: {result.content} not a valid JSON response"
            )

        self.log.info(f"Test run successful {test.name} {test.endpoint}")


class FailedIntegrationTest(Exception):
    def __init__(self, *args: object, exception=None) -> None:
        self.exception = exception
        super().__init__(*args)
