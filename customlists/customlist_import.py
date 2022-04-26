import argparse
import json
import logging
import os
from typing import Dict, List, Set

import feedparser
import requests
from requests import Response, Session

from customlists.customlist_export import (
    Book,
    CustomList,
    CustomListExports,
    ProblematicBook,
)
from customlists.customlist_report import (
    CustomListProblemBookBrokenOnSourceCM,
    CustomListProblemBookMismatch,
    CustomListProblemBookMissing,
    CustomListProblemBookRequestFailed,
    CustomListProblemListAlreadyExists,
    CustomListProblemListUpdateFailed,
    CustomListReport,
    CustomListsReport,
)


class CustomListImportFailed(Exception):
    def __init__(self, message: str):
        super(CustomListImportFailed, self).__init__(message)


class CustomListImporter:
    _session: Session
    _logger: logging.Logger
    _server_base: str
    _email: str
    _password: str
    _output_file: str
    _schema_file: str
    _schema_report_file: str

    @staticmethod
    def _fatal(message: str):
        raise CustomListImportFailed(message)

    @staticmethod
    def _error_response(message: str, response: Response) -> str:
        if response.headers.get("content-type") == "application/api-problem+json":
            error_text = json.loads(response.content)
            return f"{message}: {response.status_code} {response.reason}: {error_text['title']}: {error_text['detail']}"
        else:
            return f"{message}: {response.status_code} {response.reason}"

    @staticmethod
    def _fatal_response(message: str, response: Response) -> None:
        CustomListImporter._fatal(CustomListImporter._error_response(message, response))

    @staticmethod
    def _parse_arguments(args: List[str]) -> argparse.Namespace:
        parser: argparse.ArgumentParser = argparse.ArgumentParser(
            description="Import custom lists."
        )
        parser.add_argument("--server", help="The address of the CM", required=True)
        parser.add_argument("--username", help="The CM admin username", required=True)
        parser.add_argument("--password", help="The CM admin password", required=True)
        parser.add_argument(
            "--schema-file", help="The schema file for custom lists", required=False
        )
        parser.add_argument(
            "--schema-report-file",
            help="The schema file for custom list reports",
            required=False,
        )
        parser.add_argument("--file", help="The customlists file", required=True)
        parser.add_argument("--output", help="The output report", required=True)
        parser.add_argument(
            "--dry-run",
            help="Show what would be done, but don't do it.",
            required=False,
            action="count",
            default=0,
        )
        parser.add_argument(
            "--verbose",
            "-v",
            action="count",
            default=0,
            help="Increase verbosity (can be specified multiple times)",
        )
        return parser.parse_args(args)

    def _sign_in(self) -> None:
        server_login_endpoint: str = f"{self._server_base}/admin/sign_in_with_password"
        headers = {"User-Agent": "circulation-customlists-import/1.0"}
        payload = {
            "email": self._email,
            "password": self._password,
            "redirect": f"{self._server_base}/admin/web/",
        }

        logging.info("signing in...")
        response = self._session.post(
            server_login_endpoint, headers=headers, data=payload, allow_redirects=True
        )
        if response.status_code >= 400:
            self._fatal_response("Failed to sign in", response)

        if not response.cookies.get("csrf_token"):
            self._logger.warning(
                "the server did not return a CSRF token; expect errors to occur!"
            )

    def _open_customlists(self) -> CustomListExports:
        with open(self._schema_file, "rb") as schema_file:
            schema: str = json.load(schema_file)
            return CustomListExports.parse_file(self._file, schema)

    def _process_check_book(
        self,
        report: CustomListReport,
        customlist: CustomList,
        book: Book,
        rejected_books: Set[str],
    ) -> None:
        """Check that the book on the target CM has a matching ID and title."""
        server_work_endpoint: str = (
            f"{self._server_base}/admin/works/{book.id_type()}/{book.id()}"
        )
        response = self._session.get(server_work_endpoint)
        if response.status_code == 404:
            problem = CustomListProblemBookMissing.create(
                id=book.id(), title=book.title()
            )
            report.add_problem(problem)
            self._logger.error(problem.message())
            rejected_books.add(book.id())
            return

        if response.status_code >= 400:
            problem = CustomListProblemBookRequestFailed.create(
                id=book.id(), title=book.title(), error=self._error_response(response)
            )
            report.add_problem(problem)
            self._logger.error(problem.message())
            rejected_books.add(book.id())
            return

        feed = feedparser.parse(response.content)
        for entry in feed.entries:
            if entry.id != book.id() or entry.title != book.title():
                report.add_problem(
                    CustomListProblemBookMismatch.create(
                        expected_id=book.id(),
                        expected_title=book.title(),
                        received_id=entry.id,
                        received_title=entry.title,
                    )
                )
                rejected_books.add(book.id())

    def _process_check_problematic_book(
        self,
        report: CustomListReport,
        book: ProblematicBook,
    ) -> None:
        """Add all of the known problematic books to the output report."""
        report.add_problem(
            CustomListProblemBookBrokenOnSourceCM(
                id=book.id(), title=book.title(), message=book.message()
            )
        )

    def _process_customlist_check_books(
        self,
        list_report: CustomListReport,
        customlist: CustomList,
        rejected_books: Set[str],
    ) -> None:
        for book in customlist.books():
            self._process_check_book(
                report=list_report,
                customlist=customlist,
                book=book,
                rejected_books=rejected_books,
            )
        for problem_book in customlist.problematic_books():
            self._process_check_problematic_book(
                report=list_report,
                book=problem_book,
            )

    def _process_customlist_check_list(
        self,
        list_report: CustomListReport,
        customlist: CustomList,
        rejected_lists: Set[int],
    ) -> None:
        server_list_endpoint: str = f"{self._server_base}/admin/custom_lists"
        response = self._session.get(server_list_endpoint)
        if response.status_code >= 400:
            self._fatal_response("Failed to retrieve custom lists", response)

        raw_lists = json.loads(response.content)
        for raw_list in raw_lists["custom_lists"]:
            raw_id = raw_list["id"]
            raw_name = raw_list["name"]
            if customlist.id() == raw_id and customlist.name() == raw_name:
                list_report.add_problem(
                    CustomListProblemListAlreadyExists(
                        message=f"A list with id {customlist.id()} and name '{customlist.name()}' already exists and won't be modified",
                        id=customlist.id(),
                        name=customlist.name(),
                    )
                )
                rejected_lists.add(customlist.id())

    def _process_customlist_update_list(
        self,
        list_report: CustomListReport,
        customlist: CustomList,
        rejected_books: Set[str],
    ) -> None:
        output = []
        for book in customlist.books():
            if book.id() in rejected_books:
                continue
            output.append({"id": book.id(), "title": book.title()})

        # Send the new list to the server.
        server_list_endpoint: str = (
            f"{self._server_base}/{customlist.library_id()}/admin/custom_lists"
        )
        response = self._session.post(
            server_list_endpoint,
            files=(
                ("name", (None, customlist.name())),
                ("entries", (None, json.dumps(output, sort_keys=True))),
            ),
        )
        if response.status_code >= 400:
            list_report.add_problem(
                CustomListProblemListUpdateFailed(
                    message=CustomListImporter._error_response(
                        "Failed to update custom list", response
                    ),
                    id=customlist.id(),
                    name=customlist.name(),
                )
            )

    def _process_customlists(
        self,
        report: CustomListsReport,
        customlists: CustomListExports,
    ) -> None:
        list_reports: Dict[int, CustomListReport] = {}
        rejected_books: Set[str] = set({})
        rejected_lists: Set[int] = set({})

        for customlist in customlists.lists():
            list_report = CustomListReport(customlist.id(), customlist.name())
            list_reports[customlist.id()] = list_report
            report.add_report(list_report)
            self._process_customlist_check_books(
                list_report=list_report,
                customlist=customlist,
                rejected_books=rejected_books,
            )
        for customlist in customlists.lists():
            self._process_customlist_check_list(
                customlist=customlist,
                list_report=list_reports[customlist.id()],
                rejected_lists=rejected_lists,
            )
        for customlist in customlists.lists():
            if customlist.id() in rejected_lists:
                continue
            self._process_customlist_update_list(
                customlist=customlist,
                list_report=list_reports[customlist.id()],
                rejected_books=rejected_books,
            )

    def _save_customlists_report(self, document: CustomListsReport) -> None:
        with open(self._schema_report_file, "rb") as schema_file:
            schema: str = json.load(schema_file)

        output_file_tmp: str = self._output_file + ".tmp"
        serialized: str = document.serialize(schema)
        with open(output_file_tmp, "wb") as out:
            out.write(serialized.encode("utf-8"))

        os.rename(output_file_tmp, self._output_file)

    def execute(self) -> None:
        customlists = self._open_customlists()
        self._sign_in()
        report = CustomListsReport()
        self._process_customlists(customlists=customlists, report=report)
        self._save_customlists_report(report)

    def __init__(self, args: argparse.Namespace):
        self._session = requests.Session()
        self._logger = logging.getLogger("CustomListImporter")
        self._server_base = args.server.rstrip("/")
        self._email = args.username
        self._password = args.password
        self._file = args.file
        self._output_file = args.output
        self._dry_run = args.dry_run
        self._schema_file = args.schema_file or "customlists.schema.json"
        self._schema_report_file = (
            args.schema_report_file or "customlists-report.schema.json"
        )
        verbose: int = args.verbose or 0
        if verbose > 0:
            self._logger.setLevel(logging.INFO)
        if verbose > 1:
            self._logger.setLevel(logging.DEBUG)

    @staticmethod
    def create(args: List[str]) -> "CustomListImporter":
        return CustomListImporter(CustomListImporter._parse_arguments(args))
