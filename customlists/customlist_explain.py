import argparse
import csv
import json
import logging

from customlists.customlist_report import (
    CustomListProblemBookBrokenOnSourceCM,
    CustomListProblemBookMismatch,
    CustomListProblemBookMissing,
    CustomListProblemBookRequestFailed,
    CustomListProblemCollectionMissing,
    CustomListProblemCollectionRequestFailed,
    CustomListProblemListAlreadyExists,
    CustomListProblemListBroken,
    CustomListProblemListUpdateFailed,
    CustomListReport,
    CustomListsReport,
)


class CustomListImportExplainer:
    @staticmethod
    def _parse_arguments(args: list[str]) -> argparse.Namespace:
        parser: argparse.ArgumentParser = argparse.ArgumentParser(
            description="Explain what went wrong during an import."
        )
        parser.add_argument(
            "--verbose",
            "-v",
            action="count",
            default=0,
            help="Increase verbosity",
        )
        parser.add_argument(
            "--report-schema-file",
            help="The schema file for custom list reports",
            required=False,
            default="customlists/customlists-report.schema.json",
        )
        parser.add_argument(
            "--report-file",
            help="The report file that was produced during importing",
            required=True,
        )
        parser.add_argument(
            "--output-csv-file",
            help="The output CSV file containing the list of books to be fixed",
            required=True,
        )
        return parser.parse_args(args)

    def _load_report(self) -> CustomListsReport:
        _schema_dict = {}
        with open(self._report_schema_file, "rb") as f:
            _schema_dict = json.load(f)
        _report_dict = {}
        with open(self._report_file, "rb") as f:
            _report_dict = json.load(f)
        return CustomListsReport.parse(_schema_dict, _report_dict)

    def _generate_csv_for_list(self, csv_writer, list_report: CustomListReport) -> int:
        wrote_rows = 0
        for problem in list_report.problems():
            ++wrote_rows
            if isinstance(problem, CustomListProblemBookBrokenOnSourceCM):
                csv_writer.writerow(
                    [
                        list_report.name(),
                        problem.id(),
                        problem.title(),
                        problem.author(),
                        type(problem).TYPE,
                        problem.message(),
                        "The book could not be retrieved on the source CM, but it might be available with a manual search "
                        "on the target CM.".replace("\n", ""),
                    ]
                )
            elif isinstance(problem, CustomListProblemBookMismatch):
                csv_writer.writerow(
                    [
                        list_report.name(),
                        problem.received_id(),
                        problem.received_title(),
                        problem.author(),
                        type(problem).TYPE,
                        problem.message(),
                        "The book's identifier or title changed. Please compare the book on the target CM with the source "
                        "CM and manually add it to the list if it looks acceptable.".replace(
                            "\n", ""
                        ),
                    ]
                )
            elif isinstance(problem, CustomListProblemBookMissing):
                csv_writer.writerow(
                    [
                        list_report.name(),
                        problem.id(),
                        problem.title(),
                        problem.author(),
                        type(problem).TYPE,
                        problem.message(),
                        "Try a search for the book on the target CM and add it to the list manually if it exists.",
                    ]
                )
            elif isinstance(problem, CustomListProblemBookRequestFailed):
                csv_writer.writerow(
                    [
                        list_report.name(),
                        problem.id(),
                        problem.title(),
                        problem.author(),
                        type(problem).TYPE,
                        problem.message(),
                        "Try a search for the book on the target CM and add it to the list manually if it exists.",
                    ]
                )
            elif isinstance(problem, CustomListProblemCollectionMissing):
                csv_writer.writerow(
                    [
                        list_report.name(),
                        "",
                        "",
                        "",
                        type(problem).TYPE,
                        problem.message(),
                        f"If there is a collection that corresponds to {problem.name()} on the target CM, then add that "
                        f"collection to the custom list manually.".replace("\n", ""),
                    ]
                )
            elif isinstance(problem, CustomListProblemCollectionRequestFailed):
                csv_writer.writerow(
                    [
                        list_report.name(),
                        "",
                        "",
                        "",
                        type(problem).TYPE,
                        problem.message(),
                        f"If there is a collection that corresponds to {problem.name()} on the target CM, then add that "
                        f"collection to the custom list manually.".replace("\n", ""),
                    ]
                )
            elif isinstance(problem, CustomListProblemListAlreadyExists):
                csv_writer.writerow(
                    [
                        list_report.name(),
                        "",
                        "",
                        "",
                        type(problem).TYPE,
                        problem.message(),
                        "Please either choose a different name for the custom list, or delete the existing one.",
                    ]
                )
            elif isinstance(problem, CustomListProblemListUpdateFailed):
                csv_writer.writerow(
                    [
                        list_report.name(),
                        "",
                        "",
                        "",
                        type(problem).TYPE,
                        problem.message(),
                        "Please try running the import again. This might be a temporary failure.",
                    ]
                )
            elif isinstance(problem, CustomListProblemListBroken):
                csv_writer.writerow(
                    [
                        list_report.name(),
                        "",
                        "",
                        "",
                        type(problem).TYPE,
                        problem.message(),
                        "Please let someone on the backend team know; this list seems to be broken!",
                    ]
                )
            else:
                raise ValueError(f"Unrecognized problem type {problem}")
        return wrote_rows

    def _generate_csv(self, report: CustomListsReport) -> None:
        with open(self._output_csv_file, "w", newline="") as csvfile:
            csv_writer = csv.writer(
                csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL
            )
            csv_writer.writerow(
                [
                    "List",
                    "Book ID",
                    "Book Title",
                    "Book Author",
                    "Error Code",
                    "Error",
                    "Suggestion",
                ]
            )
            wrote_rows = 0
            for list_report in report.reports():
                wrote_rows += self._generate_csv_for_list(csv_writer, list_report)
            csvfile.flush()
            self._logger.info(f"wrote {wrote_rows} CSV rows")

    def execute(self) -> None:
        _report = self._load_report()
        self._generate_csv(_report)

    def __init__(self, args: argparse.Namespace):
        self._logger = logging.getLogger("CustomListImportExplainer")
        self._report_schema_file = args.report_schema_file
        self._report_file = args.report_file
        self._output_csv_file = args.output_csv_file

    @staticmethod
    def create(args: list[str]) -> "CustomListImportExplainer":
        return CustomListImportExplainer(
            CustomListImportExplainer._parse_arguments(args)
        )
