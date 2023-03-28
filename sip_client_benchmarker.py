import argparse
import csv
import datetime
import logging
import statistics
import sys
from statistics import mean, median
from typing import Dict

from api.sip.client import SIPClient

# set up logging to the console.
root = logging.getLogger()
root.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
root.addHandler(handler)


class TestSIPClient(SIPClient):
    """This class exists only to allow the connection timeout to be overriden.
    Before this code is merged, the changes in this class - ie making
    the connection timeout configurable - should be added to the SIPClient
    so that this class can be removed.
    """

    def __init__(self, connection_timeout: int = 2, *args, **kwargs):
        self._connection_timeout = connection_timeout
        super().__init__(*args, **kwargs)

    def connect(self):

        try:
            if self.connection:
                # If we are still connected then disconnect.
                self.disconnect()
            if self.use_ssl:
                self.connection = self.make_secure_connection()
            else:
                self.connection = self.make_insecure_connection()

            self.connection.settimeout(self._connection_timeout)
            self.connection.connect((self.target_server, self.target_port))
        except OSError as message:
            raise OSError(
                "Could not connect to %s:%s - %s"
                % (self.target_server, self.target_port, message)
            )

        # Since this is a new socket connection, reset the message count
        self.reset_connection_state()


class SIPClientTester:
    """
    This is the main test script.
    """

    def __init__(
        self,
        host: str,
        port: int,
        use_ssl: bool = True,
        sip_username: str = None,
        sip_password: str = None,
        patron_username: str = None,
        patron_password: str = None,
        library: str = None,
        sip_integration: str = None,
        ils: str = None,
        timeout_in_seconds: int = 2,
        iterations: int = 25,
    ):
        self.host = host
        self.port = port
        self.use_ssl = use_ssl
        self.sip_username = sip_username
        self.sip_password = sip_password
        self.patron_username = patron_username
        self.patron_password = patron_password
        self.library = library
        self.sip_integration = sip_integration
        self.ils = ils
        self.timeout_in_seconds = timeout_in_seconds
        self.iterations = iterations
        self.test_data = []

    def _create_client(self) -> SIPClient:
        return TestSIPClient(
            self.timeout_in_seconds,
            self.host,
            self.port,
            login_user_id=self.sip_username,
            login_password=self.sip_password,
            use_ssl=self.use_ssl,
            # dialect=self.ils,
        )

    def do_test(self) -> Dict:
        """This is the heart of the testing routine.  It records execution time for four different SIPClient methods:
        connect, login, patron_information, and end_session and returns the result in a dictionary.
        """
        logging.info(f"Beginning test for {self.host}...")
        for i in range(self.iterations):
            logging.info(f"Beginning iteration {i} for {self.host}...")
            try:
                sip = self._create_client()

                self.time_execution(sip.connect)
                self.time_execution(sip.login)

                def patron_information():
                    sip.patron_information(self.patron_username, self.patron_password)

                self.time_execution(patron_information)

                def end_session():
                    sip.end_session(self.patron_username, self.patron_password)

                self.time_execution(end_session)
                self.time_execution(sip.disconnect)

            except OSError as e:
                error = e

        logging.info(f"Completed iteration {i} for {self.host}.")
        return self.summarize_results()

    def time_execution(self, func):
        """This method captures the time it takes
        to run the specified function"""
        start = datetime.datetime.now()
        exception = None
        try:
            func()
        except Exception as ex:
            exception = ex
            logging.exception(ex)

        stop = datetime.datetime.now()
        delta: datetime.timedelta = stop - start
        elapsed_seconds = delta.total_seconds()
        success = exception is None
        logging.info(
            f"function: {func.__name__}, host={self.host}, success={success}, elapsed_seconds={elapsed_seconds}"
        )

        self.test_data.append(
            {
                "function": func.__name__,
                "host": self.host,
                "success": success,
                "elapsed_seconds": elapsed_seconds,
            }
        )
        if exception:
            raise exception

    def summarize_results(self) -> Dict:
        """This method summarizes the results of the test and
        returns the summary as dictionary."""
        # extract result lists by function
        results_by_function = {}
        for x in self.test_data:
            fname = x["function"]
            list = results_by_function.get(fname, None)
            if not list:
                list = []
                results_by_function[fname] = list
            list.append(x)

        summary = {
            "library": self.library,
            "sip_integration": self.sip_integration,
            "ils": self.ils,
            "host": self.host,
            "port": self.port,
            "use_ssl": self.use_ssl,
            "iterations": self.iterations,
            "timeout": self.timeout_in_seconds,
        }

        function_summaries = []
        summary["functions"] = function_summaries

        # for each function
        for fname in results_by_function.keys():

            function_summary = {"function": fname}

            function_summaries.append(function_summary)

            # summarize results
            results = results_by_function[fname]
            results_count = len(results)
            success_list = [x for x in results if x["success"]]
            failure_list = [x for x in results if not x["success"]]
            success_count = len(success_list)
            success_rate = success_count / results_count
            function_summary["success_rate"] = success_rate
            logging.info(f"success rate {round(success_rate, 2)}%")
            for name, the_list in [
                (
                    "success",
                    success_list,
                ),
                (
                    "failure",
                    failure_list,
                ),
            ]:
                elapsed_seconds_list = [x["elapsed_seconds"] for x in the_list]
                list_mean = (
                    mean(elapsed_seconds_list)
                    if len(elapsed_seconds_list) > 0
                    else None
                )
                list_median = (
                    median(elapsed_seconds_list)
                    if len(elapsed_seconds_list) > 0
                    else None
                )
                list_max = (
                    max(elapsed_seconds_list) if len(elapsed_seconds_list) > 0 else None
                )
                list_min = (
                    min(elapsed_seconds_list) if len(elapsed_seconds_list) > 0 else None
                )
                list_stdev = (
                    statistics.stdev(elapsed_seconds_list)
                    if len(elapsed_seconds_list) > 0
                    else None
                )
                logging.info(
                    f"{name}: elapsed_time mean: {list_mean}, median:{list_median}, max: {list_max}, min: {list_min}"
                )

                function_summary[name] = {
                    "mean": list_mean,
                    "median": list_median,
                    "stdev": list_stdev,
                    "max": list_max,
                    "min": list_min,
                }

        return summary


def main(csvfile=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--csv",
        help="A csv file path with the following columns: sip_host, port, "
        "sip_username, sip_password, patron_username, patron_password, "
        "use_ssl (True|False), ils (GenericILS|AutoGraphicsVerso), sip_connection_timeout",
        default=None,
    )
    parser.add_argument(
        "--iterations",
        help="The number of tests to perform on each row in the csv",
        default=1,
    )

    parsed_args = parser.parse_args(sys.argv[1:])
    iterations = int(parsed_args.iterations)
    csv_file_path = parsed_args.csv
    test_all(csv_file_path, iterations)


def test_all(csv_file_path, iterations):
    """
    This method loops through each row in the CSV file, extracts the test configuration
    information from the row, and runs the test with the specified number of iterations.
    The results are gathered and finally written to a CSV  output file.
    :param csv_file_path:
    :param iterations:
    :return:
    """
    with open(csv_file_path) as csv_file:
        reader = csv.DictReader(csv_file)
        summaries = []
        for row in reader:
            library = str(row["library"])
            sip_integration = str(row["sip_integration_name"])
            host = str(row["sip_host"])
            port = int(row["sip_port"])
            sip_username = str(row["sip_username"])
            sip_password = str(row["sip_password"])
            patron_username = str(row["patron_username"])
            patron_password = str(row["patron_password"])
            ils = str(row["ils"])
            use_ssl = False  # bool(row.get('use_ssl', 'True'))
            timeout = int(row.get("sip_connection_timeout", "2"))
            logging.info(
                f"testing {host}:{port} use_ssl={use_ssl}, "
                f"sip_connection_timeout={timeout}"
            )

            test = SIPClientTester(
                host=host,
                port=port,
                sip_username=sip_username,
                sip_password=sip_password,
                patron_username=patron_username,
                patron_password=patron_password,
                library=library,
                sip_integration=sip_integration,
                use_ssl=use_ssl,
                ils=ils,
                iterations=iterations,
                timeout_in_seconds=timeout,
            )

            summaries.append(test.do_test())

        # write summaries to csv
        with open(
            f"test_results-{datetime.datetime.now().isoformat()}.csv", mode="w"
        ) as output_file:
            fieldnames = [
                "library",
                "sip_integration",
                "host",
                "port",
                "iterations",
                "function",
                "use_ssl",
                "ils",
                "timeout",
                "success_rate",
                "success_mean",
                "success_median",
                "success_stdev",
                "success_min",
                "success_max",
                "failure_mean",
                "failure_median",
                "failure_stdev",
                "failure_min",
                "failure_max",
            ]
            writer = csv.DictWriter(output_file, fieldnames=fieldnames)
            writer.writeheader()
            for summary in summaries:
                for f in summary["functions"]:
                    success = f["success"]
                    failure = f["failure"]
                    writer.writerow(
                        {
                            "library": summary["library"],
                            "sip_integration": summary["sip_integration"],
                            "host": summary["host"],
                            "port": summary["port"],
                            "iterations": iterations,
                            "function": f["function"],
                            "use_ssl": summary["use_ssl"],
                            "ils": summary["ils"],
                            "timeout": timeout,
                            "success_rate": f["success_rate"],
                            "success_mean": success["mean"],
                            "success_median": success["median"],
                            "success_stdev": success["stdev"],
                            "success_min": success["min"],
                            "success_max": success["max"],
                            "failure_mean": failure["mean"],
                            "failure_median": failure["median"],
                            "failure_stdev": failure["stdev"],
                            "failure_min": failure["min"],
                            "failure_max": failure["max"],
                        }
                    )


if __name__ == "__main__":
    main()
