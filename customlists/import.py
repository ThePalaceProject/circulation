#!/usr/bin/env python

import argparse
import json
import logging
import os
from typing import Dict, Set

import feedparser
import requests
from customlist_export import Book, CustomList, CustomListExports, ProblematicBook
from customlist_report import (
    CustomListProblemBookBrokenOnSourceCM,
    CustomListProblemBookMismatch,
    CustomListProblemBookMissing,
    CustomListProblemListAlreadyExists,
    CustomListReport,
    CustomListsReport,
)
from requests import Response

logging.basicConfig()
logger = logging.getLogger()


def fatal(message: str) -> None:
    logging.fatal(message)
    if __name__ == "__main__":
        exit(1)
    else:
        raise RuntimeError


def fatal_response(response: Response) -> None:
    if response.headers.get("content-type") == "application/api-problem+json":
        error_text = json.loads(response.content)
        fatal(
            f"{response.status_code} {response.reason}: {error_text['title']}: {error_text['detail']}"
        )
    else:
        fatal(f"{response.status_code} {response.reason}")


def parse_arguments() -> argparse.Namespace:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Import custom lists."
    )
    parser.add_argument("--server", help="The address of the CM", required=True)
    parser.add_argument("--username", help="The CM admin username", required=True)
    parser.add_argument("--password", help="The CM admin password", required=True)
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
    return parser.parse_args()


def sign_in(
    session: requests.Session, server_base: str, email: str, password: str
) -> None:
    server_login_endpoint: str = f"{server_base}/admin/sign_in_with_password"
    headers = {"User-Agent": "circulation-customlists-import/1.0"}
    payload = {
        "email": email,
        "password": password,
        "redirect": f"{server_base}/admin/",
    }

    logging.info("signing in...")
    response = session.post(
        server_login_endpoint, headers=headers, data=payload, allow_redirects=False
    )
    if response.status_code >= 400:
        fatal_response(response)


def open_customlists(file: str) -> CustomListExports:
    with open("customlists.schema.json", "rb") as schema_file:
        schema: str = json.load(schema_file)
        return CustomListExports.parse_file(file, schema)


def process_check_book(
    session: requests.Session,
    server_base: str,
    report: CustomListReport,
    customlist: CustomList,
    book: Book,
    rejected_books: Set[str],
) -> None:
    server_work_endpoint: str = (
        f"{server_base}/admin/works/{book.id_type()}/{book.id()}"
    )
    response = session.get(server_work_endpoint)
    if response.status_code == 404:
        problem = CustomListProblemBookMissing.create(id=book.id(), title=book.title())
        report.add_problem(problem)
        logger.error(problem.message())
        rejected_books.add(book.id())
        return

    if response.status_code >= 400:
        fatal_response(response)

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


def process_check_problematic_book(
    session: requests.Session,
    server_base: str,
    report: CustomListReport,
    customlist: CustomList,
    book: ProblematicBook,
) -> None:
    report.add_problem(
        CustomListProblemBookBrokenOnSourceCM(
            id=book.id(), title=book.title(), message=book.message()
        )
    )


def process_customlist_check_books(
    session: requests.Session,
    server_base: str,
    list_report: CustomListReport,
    customlist: CustomList,
    rejected_books: Set[str],
) -> None:
    for book in customlist.books():
        process_check_book(
            session=session,
            server_base=server_base,
            report=list_report,
            customlist=customlist,
            book=book,
            rejected_books=rejected_books,
        )
    for problem_book in customlist.problematic_books():
        process_check_problematic_book(
            session=session,
            server_base=server_base,
            report=list_report,
            customlist=customlist,
            book=problem_book,
        )


def process_customlist_check_list(
    session: requests.Session,
    server_base: str,
    list_report: CustomListReport,
    customlist: CustomList,
    rejected_lists: Set[int],
) -> None:
    server_list_endpoint: str = f"{server_base}/admin/custom_lists"
    response = session.get(server_list_endpoint)
    if response.status_code >= 400:
        fatal_response(response)

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


def process_customlist_update_list(
    session: requests.Session,
    server_base: str,
    list_report: CustomListReport,
    customlist: CustomList,
    rejected_books: Set[str],
) -> None:
    server_list_endpoint: str = f"{server_base}/admin/custom_lists"
    output = []
    for book in customlist.books():
        if book.id() in rejected_books:
            continue
        output.append({"id": book.id(), "title": book.title()})
    response = session.post(
        server_list_endpoint,
        files=(
            ("name", (None, customlist.name())),
            ("entries", (None, json.dumps(output, sort_keys=True))),
        ),
    )
    if response.status_code >= 400:
        fatal_response(response)


def process_customlists(
    session: requests.Session,
    server_base: str,
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
        process_customlist_check_books(
            session=session,
            server_base=server_base,
            list_report=list_report,
            customlist=customlist,
            rejected_books=rejected_books,
        )
    for customlist in customlists.lists():
        process_customlist_check_list(
            session=session,
            server_base=server_base,
            customlist=customlist,
            list_report=list_reports[customlist.id()],
            rejected_lists=rejected_lists,
        )
    for customlist in customlists.lists():
        if customlist.id() in rejected_lists:
            continue
        process_customlist_update_list(
            session=session,
            server_base=server_base,
            customlist=customlist,
            list_report=list_reports[customlist.id()],
            rejected_books=rejected_books,
        )


def save_customlists_report(document: CustomListsReport, output_file: str) -> None:
    with open("customlists-report.schema.json", "rb") as schema_file:
        schema: str = json.load(schema_file)

    output_file_tmp: str = output_file + ".tmp"
    serialized: str = document.serialize(schema)
    with open(output_file_tmp, "wb") as out:
        out.write(serialized.encode("utf-8"))

    os.rename(output_file_tmp, output_file)


def main():
    args = parse_arguments()

    verbose: int = args.verbose or 0
    if verbose > 0:
        logger.setLevel(logging.INFO)
    if verbose > 1:
        logger.setLevel(logging.DEBUG)

    customlists = open_customlists(args.file)
    session = requests.Session()
    server: str = args.server.rstrip("/")

    sign_in(
        session=session, server_base=server, email=args.username, password=args.password
    )

    report = CustomListsReport()
    process_customlists(
        session=session, server_base=server, customlists=customlists, report=report
    )
    save_customlists_report(report, args.output)


if __name__ == "__main__":
    main()
