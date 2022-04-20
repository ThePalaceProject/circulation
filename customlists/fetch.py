#!/usr/bin/env python

import argparse
import json
import logging
import os
from typing import List

import feedparser
import jsonschema
import requests

logging.basicConfig()
logger = logging.getLogger()


def fatal(message: str) -> None:
    logging.fatal(message)
    if __name__ == "__main__":
        exit(1)
    else:
        raise RuntimeError


def parse_arguments() -> argparse.Namespace:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Fetch a custom list."
    )
    parser.add_argument("--server", help="The address of the CM", required=True)
    parser.add_argument("--username", help="The CM admin username", required=True)
    parser.add_argument("--password", help="The CM admin password", required=True)
    parser.add_argument("--output", help="The output file", required=True)
    parser.add_argument(
        "--list-name", help="The name of the custom list", required=True
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="Increase verbosity (can be specified multiple times)",
    )
    return parser.parse_args()


def make_custom_list(
    session: requests.Session, server_base: str, raw_list: dict
) -> dict:
    id: int = raw_list["id"]
    server_list_endpoint: str = f"{server_base}/admin/custom_list/{id}"

    response = session.get(server_list_endpoint)
    if response.status_code >= 400:
        fatal(
            f"failed to retrieve custom list {id}: {response.status_code} {response.reason}"
        )

    books = []
    feed = feedparser.parse(url_file_stream_or_string=response.content)
    for entry in feed.entries:
        book = {}
        book["%type"] = "book"
        book["id"] = entry.id
        book["title"] = entry.title
        books.append(book)

    customlist = {
        "%type": "customlist",
        "books": books,
        "id": raw_list["id"],
        "name": raw_list["name"],
    }
    logger.info(f"retrieved {len(books)} books for list {id}")
    return customlist


def make_custom_lists_document(session: requests.Session, server_base: str) -> dict:
    logging.info("fetching lists...")
    server_lists_endpoint: str = f"{server_base}/admin/custom_lists"
    response = session.get(server_lists_endpoint)
    if response.status_code >= 400:
        fatal(
            f"failed to retrieve custom lists: {response.status_code} {response.reason}"
        )

    raw_document = json.loads(response.content)
    raw_lists: list = raw_document["custom_lists"] or []

    custom_lists: List[dict] = []
    document = {
        "customlists": custom_lists,
        "%id": "https://schemas.thepalaceproject.io/customlists/1.0",
    }

    for raw_list in raw_lists:
        custom_lists.append(make_custom_list(session, server_base, raw_list))

    logger.info(f"retrieved {len(custom_lists)} custom lists")
    return document


def sign_in(
    session: requests.Session, server_base: str, email: str, password: str
) -> None:
    server_login_endpoint: str = f"{server_base}/admin/sign_in_with_password"
    headers = {"User-Agent": "circulation-customlists-fetch/1.0"}
    payload = {"email": email, "password": password}

    logging.info("signing in...")
    response = session.post(
        server_login_endpoint, headers=headers, data=payload, allow_redirects=False
    )
    if response.status_code >= 400:
        fatal(f"failed to sign in: {response.status_code} {response.reason}")


def save_customlists_document(document: dict, output_file: str) -> None:
    with open("customlists.schema.json", "rb") as schema_file:
        schema: str = json.load(schema_file)

    jsonschema.validate(document, schema)
    logger.info(f"validated output against schema")

    output_file_tmp: str = output_file + ".tmp"

    serialized: str = json.dumps(document, sort_keys=True, indent=2)
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

    session = requests.Session()
    server: str = args.server.rstrip("/")

    sign_in(
        session=session, server_base=server, email=args.username, password=args.password
    )
    document = make_custom_lists_document(session=session, server_base=server)
    save_customlists_document(document, args.output)


if __name__ == "__main__":
    main()
