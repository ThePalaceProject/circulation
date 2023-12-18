import argparse
import json
import logging
import os
import re
from urllib.parse import unquote

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
    CustomListProblemCollectionMissing,
    CustomListProblemListAlreadyExists,
    CustomListProblemListUpdateFailed,
    CustomListReport,
    CustomListsReport,
)


class CustomListImportFailed(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class CustomListImporter:
    _session: Session
    _logger: logging.Logger
    _server_base: str
    _email: str
    _password: str
    _output_file: str
    _schema_file: str
    _schema_report_file: str
    _library_name: str

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
    def _parse_arguments(args: list[str]) -> argparse.Namespace:
        parser: argparse.ArgumentParser = argparse.ArgumentParser(
            description="Import custom lists."
        )
        parser.add_argument("--server", help="The address of the CM", required=True)
        parser.add_argument("--username", help="The CM admin username", required=True)
        parser.add_argument("--password", help="The CM admin password", required=True)
        parser.add_argument(
            "--schema-file",
            help="The schema file for custom lists",
            required=False,
            default="customlists/customlists.schema.json",
        )
        parser.add_argument(
            "--schema-report-file",
            help="The schema file for custom list reports",
            required=False,
            default="customlists/customlists-report.schema.json",
        )
        parser.add_argument(
            "--library-name", help="The destination library short name", required=True
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

        self._logger.info("Signing in...")
        response = self._session.post(
            server_login_endpoint, headers=headers, data=payload, allow_redirects=True
        )
        if response.status_code >= 400:
            self._fatal_response("Failed to sign in", response)

        if not response.cookies.get("csrf_token"):
            self._logger.warning(
                "The server did not return a CSRF token; expect errors to occur!"
            )

    def _open_customlists(self) -> CustomListExports:
        with open(self._schema_file, "rb") as schema_file:
            schema: dict = json.load(schema_file)
            return CustomListExports.parse_file(self._file, schema)

    def _process_check_book(
        self,
        report: CustomListReport,
        customlist: CustomList,
        book: Book,
        rejected_books: set[str],
    ) -> None:
        self._logger.info(
            f"Checking that book '{book.title()}' ({book.id()}) has a matching ID and title on the target CM."
        )

        """Check that the book on the target CM has a matching ID and title."""
        server_work_endpoint: str = f"{self._server_base}/{self._library_name}/admin/works/{book.id_type()}/{book.id_value()}"
        response = self._session.get(server_work_endpoint)
        if response.status_code == 404:
            problem_missing = CustomListProblemBookMissing.create(
                id=book.id(),
                id_type=book.id_type(),
                author=book.author(),
                title=book.title(),
            )
            report.add_problem(problem_missing)
            self._logger.error(problem_missing.message())
            rejected_books.add(book.id())
            return

        if response.status_code >= 400:
            problem_request = CustomListProblemBookRequestFailed.create(
                id=book.id(),
                id_type=book.id_type(),
                author=book.author(),
                title=book.title(),
                error=self._error_response("Book request failed", response),
            )
            report.add_problem(problem_request)
            self._logger.error(problem_request.message())
            rejected_books.add(book.id())
            return

        feed = feedparser.parse(response.content)
        for entry in feed.entries:
            for link in entry.links:
                if link.rel == "alternate":
                    match = re.search("^(.*)/works/([^/]+)/(.*)$", link.href)
                    if match is not None:
                        entry_id_unquoted = unquote(entry.id)
                        matches_id = entry_id_unquoted == book.id()
                        matches_title = entry.title == book.title()
                        self._logger.debug(
                            f"comparing id '{entry_id_unquoted}' with '{book.id()}' -> {matches_id}"
                        )
                        self._logger.debug(
                            f"comparing title '{entry.title}' with '{book.title()}' -> {matches_title}"
                        )

                        if not (matches_id and matches_title):
                            problem = CustomListProblemBookMismatch.create(
                                expected_id=book.id(),
                                expected_id_type=book.id_type(),
                                expected_title=book.title(),
                                received_id=entry_id_unquoted,
                                received_title=entry.title,
                                author=book.author(),
                            )
                            self._logger.error(problem.message())
                            report.add_problem(problem)
                            rejected_books.add(book.id())
                            break

    def _process_check_problematic_book(
        self,
        report: CustomListReport,
        book: ProblematicBook,
    ) -> None:
        """Add all of the known problematic books to the output report."""
        problem = CustomListProblemBookBrokenOnSourceCM(
            id=book.id(),
            author=book.author(),
            id_type="?",
            title=book.title(),
            message=f"Book '{book.title()}' ({book.id()}) was excluded from list updates due to a problem on the source CM: {book.message()}",
        )
        self._logger.error(problem.message())
        report.add_problem(problem)

    def _process_customlist_check_collections(
        self,
        list_report: CustomListReport,
        customlist: CustomList,
        rejected_collections: set[str],
    ) -> None:
        self._logger.info(
            "Checking that all referenced collections exist on the target CM"
        )

        server_collections_endpoint: str = f"{self._server_base}/admin/collections"
        response = self._session.get(server_collections_endpoint)
        if response.status_code >= 400:
            self._fatal_response("Unable to retrieve collections", response)

        raw_content = json.loads(response.content.decode("utf-8"))
        raw_collections = raw_content["collections"]

        for collection in customlist.collections():
            located = False
            for raw_collection in raw_collections:
                if collection.name() == raw_collection["name"]:
                    located = True
                    collection.update_id(raw_collection["id"])
                    break
            if not located:
                problem_missing = CustomListProblemCollectionMissing.create(
                    collection.name()
                )
                list_report.add_problem(problem_missing)
                self._logger.error(problem_missing.message())
                rejected_collections.add(collection.name())

    def _process_customlist_check_books(
        self,
        list_report: CustomListReport,
        customlist: CustomList,
        rejected_books: set[str],
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
        rejected_lists: set[int],
    ) -> None:
        self._logger.info(
            f"Checking that list '{customlist.name()}' ({customlist.id()}) does not exist on the target CM"
        )

        server_list_endpoint: str = (
            f"{self._server_base}/{self._library_name}/admin/custom_lists"
        )
        response = self._session.get(server_list_endpoint)
        if response.status_code >= 400:
            self._fatal_response("Failed to retrieve custom lists", response)

        raw_lists = json.loads(response.content)
        for raw_list in raw_lists["custom_lists"]:
            raw_id = raw_list["id"]
            raw_name = raw_list["name"]
            if customlist.id() == raw_id and customlist.name() == raw_name:
                problem = CustomListProblemListAlreadyExists(
                    message=f"A list with id {customlist.id()} and name '{customlist.name()}' already exists and won't be modified",
                    id=customlist.id(),
                    name=customlist.name(),
                )
                self._logger.error(problem.message())
                list_report.add_problem(problem)
                rejected_lists.add(customlist.id())

    def _process_customlist_update_list(
        self,
        list_report: CustomListReport,
        customlist: CustomList,
        rejected_books: set[str],
        rejected_collections: set[str],
    ) -> None:
        self._logger.info(
            f"Updating list '{customlist.name()}' ({customlist.id()}) on the target CM with {customlist.size()} books"
        )
        if not self._dry_run:
            output_books: list[dict] = []
            for book in customlist.books():
                if book.id() in rejected_books:
                    continue
                output_books.append({"id": book.id(), "title": book.title()})

            output_collections: list[int] = []
            for collection in customlist.collections():
                if collection.name() in rejected_collections:
                    continue
                output_collections.append(collection.id())

            # We're required to manually set the X-CSRF-Token header.
            headers = {}
            if self._session.cookies.get("csrf_token"):
                headers["X-CSRF-Token"] = self._session.cookies.get("csrf_token")

            # Send the new list to the server.
            server_list_endpoint: str = (
                f"{self._server_base}/{self._library_name}/admin/custom_lists"
            )

            response = self._session.post(
                server_list_endpoint,
                headers=headers,
                files=(
                    ("name", (None, customlist.name())),
                    ("entries", (None, json.dumps(output_books, sort_keys=True))),
                    ("deletedEntries", (None, b"[]")),
                    (
                        "collections",
                        (None, json.dumps(output_collections, sort_keys=True)),
                    ),
                ),
            )
            if response.status_code >= 400:
                problem = CustomListProblemListUpdateFailed(
                    message=CustomListImporter._error_response(
                        "Failed to update custom list", response
                    ),
                    id=customlist.id(),
                    name=customlist.name(),
                )
                self._logger.error(problem.message())
                list_report.add_problem(problem)
        else:
            self._logger.info(
                f"Skipping update of list '{customlist.name()}' ({customlist.id()}) because this is a dry run"
            )

    def _process_customlists(
        self,
        report: CustomListsReport,
        customlists: CustomListExports,
    ) -> None:
        list_reports: dict[int, CustomListReport] = {}
        rejected_books: set[str] = set({})
        rejected_lists: set[int] = set({})
        rejected_collections: set[str] = set({})

        for customlist in customlists.lists():
            list_report = CustomListReport(customlist.id(), customlist.name())
            list_reports[customlist.id()] = list_report
            report.add_report(list_report)
            self._process_customlist_check_collections(
                list_report=list_report,
                customlist=customlist,
                rejected_collections=rejected_collections,
            )
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
                rejected_collections=rejected_collections,
            )

    def _save_customlists_report(self, document: CustomListsReport) -> None:
        with open(self._schema_report_file, "rb") as schema_file:
            schema: dict = json.load(schema_file)

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
        self._schema_file = args.schema_file
        self._schema_report_file = args.schema_report_file
        self._library_name = args.library_name
        verbose: int = args.verbose or 0
        if verbose > 0:
            self._logger.setLevel(logging.INFO)
        if verbose > 1:
            self._logger.setLevel(logging.DEBUG)

    @staticmethod
    def create(args: list[str]) -> "CustomListImporter":
        return CustomListImporter(CustomListImporter._parse_arguments(args))
