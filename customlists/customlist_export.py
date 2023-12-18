import argparse
import json
import logging
import os
import re
from collections.abc import Iterable, Mapping
from typing import IO, Any
from urllib.parse import unquote

import feedparser
import jsonschema
import requests
from requests import Session


class CollectionReference:
    _id: int
    _id_updated: int
    _name: str
    _protocol: str

    def __init__(self, id: int, name: str, protocol: str):
        self._id = id
        self._id_updated = id
        self._name = name
        self._protocol = protocol

    def to_dict(self) -> dict:
        return {
            "%type": "collection",
            "id": self._id,
            "name": self._name,
            "protocol": self._protocol,
        }

    def id_original(self) -> int:
        return self._id

    def id(self) -> int:
        return self._id_updated

    def name(self) -> str:
        return self._name

    def protocol(self) -> str:
        return self._protocol

    def update_id(self, id: int):
        assert type(id) == int
        self._id_updated = id


class Book:
    _id_value: str
    _id_type: str
    _id_full: str
    _title: str
    _author: str

    def __init__(
        self, id_value: str, id_type: str, id_full: str, title: str, author: str
    ):
        self._id_value = id_value
        self._id_type = id_type
        self._id_full = id_full
        self._title = title
        self._author = author
        assert id_value
        assert id_type
        assert id_full
        assert title
        assert author

    def id(self) -> str:
        return self._id_full

    def id_type(self) -> str:
        return self._id_type

    def id_value(self) -> str:
        return self._id_value

    def title(self) -> str:
        return self._title

    def author(self) -> str:
        return self._author

    def to_dict(self) -> dict:
        return {
            "%type": "book",
            "id-value": self._id_value,
            "id-type": self._id_type,
            "id-full": self._id_full,
            "title": self._title,
            "author": self._author,
        }


class ProblematicBook:
    _id: str
    _title: str
    _message: str

    def __init__(self, id: str, title: str, message: str, author: str):
        self._id = id
        self._message = message
        self._title = title
        self._author = author
        assert id
        assert message
        assert title
        assert author

    def id(self) -> str:
        return self._id

    def message(self) -> str:
        return self._message

    def title(self) -> str:
        return self._title

    def author(self) -> str:
        return self._author

    def to_dict(self) -> dict:
        return {
            "%type": "problematic-book",
            "id": self._id,
            "message": self._message,
            "title": self._title,
            "author": self._author,
        }


class ProblematicCustomList:
    _id: int
    _name: str
    _error: str

    def __init__(self, id: int, name: str, error: str):
        self._id = id
        self._name = name
        self._error = error

    def id(self) -> int:
        return self._id

    def to_dict(self) -> dict:
        return {
            "%type": "problematic-customlist",
            "id": self._id,
            "name": self._name,
            "error": self._error,
        }

    def error(self) -> str:
        return self._error

    def name(self) -> str:
        return self._name


class CustomList:
    _books: list[Book]
    _problematic_books: list[ProblematicBook]
    _collections: list[CollectionReference]
    _id: int
    _name: str
    _library_id: str

    def __init__(self, id: int, name: str, library_id: str):
        self._books = []
        self._problematic_books = []
        self._collections = []
        self._id = id
        self._name = name
        self._library_id = library_id

    def id(self) -> int:
        return self._id

    def library_id(self) -> str:
        return self._library_id

    def add_collection(self, collection: CollectionReference) -> None:
        assert type(collection) == CollectionReference
        self._collections.append(collection)

    def add_book(self, book: Book) -> None:
        assert type(book) == Book
        self._books.append(book)

    def add_problematic_book(self, book: ProblematicBook) -> None:
        assert type(book) == ProblematicBook
        self._problematic_books.append(book)

    def size(self) -> int:
        return len(self._books)

    def to_dict(self) -> dict:
        return {
            "%type": "customlist",
            "books": list(map(lambda b: b.to_dict(), self._books)),
            "id": self._id,
            "library-id": self._library_id,
            "name": self._name,
            "problematic-books": list(
                map(lambda b: b.to_dict(), self._problematic_books)
            ),
            "collections": list(map(lambda c: c.to_dict(), self._collections)),
        }

    def books(self) -> Iterable[Book]:
        return (b for b in self._books)

    def problematic_books(self) -> Iterable[ProblematicBook]:
        return (b for b in self._problematic_books)

    def collections(self) -> Iterable[CollectionReference]:
        return (c for c in self._collections)

    def name(self) -> str:
        return self._name


class CustomListExports:
    _lists: list[CustomList]
    _problematic_lists: list[ProblematicCustomList]

    def __init__(self):
        self._lists = []
        self._problematic_lists = []

    def add_list(self, list: CustomList) -> None:
        assert type(list) == CustomList
        self._lists.append(list)

    def add_problematic_list(self, list: ProblematicCustomList) -> None:
        assert type(list) == ProblematicCustomList
        self._problematic_lists.append(list)

    def to_dict(self) -> dict:
        return {
            "%id": "https://schemas.thepalaceproject.io/customlists/1.0",
            "customlists": list(map(lambda c: c.to_dict(), self._lists)),
            "problematic-customlists": list(
                map(lambda c: c.to_dict(), self._problematic_lists)
            ),
        }

    def size(self) -> int:
        return len(self._lists)

    def serialize(self, schema: Mapping[str, Any]) -> str:
        document_dict = self.to_dict()
        jsonschema.validate(document_dict, schema)
        return json.dumps(document_dict, sort_keys=True, indent=2)

    @staticmethod
    def parse(document: dict, schema: dict) -> "CustomListExports":
        assert type(schema) == dict

        jsonschema.validate(document, schema)
        exports = CustomListExports()

        # Load the lists.
        for raw_list in document["customlists"]:
            custom_list = CustomList(
                raw_list["id"], raw_list["name"], raw_list["library-id"]
            )
            for raw_book in raw_list["books"]:
                book = Book(
                    id_value=raw_book["id-value"],
                    id_type=raw_book["id-type"],
                    id_full=raw_book["id-full"],
                    title=raw_book["title"],
                    author=raw_book["author"],
                )
                custom_list.add_book(book)
            for raw_book in raw_list["problematic-books"]:
                problem_book = ProblematicBook(
                    id=raw_book["id-full"],
                    message=raw_book["message"],
                    title=raw_book["title"],
                    author=raw_book["author"],
                )
                custom_list.add_problematic_book(problem_book)
            for raw_collection in raw_list["collections"]:
                collection = CollectionReference(
                    id=raw_collection["id"],
                    name=raw_collection["name"],
                    protocol=raw_collection["protocol"],
                )
                custom_list.add_collection(collection)
            exports.add_list(custom_list)

        # Load the problematic lists.
        for raw_list in document["problematic-customlists"]:
            problem_list = ProblematicCustomList(
                raw_list["id"], raw_list["name"], raw_list["error"]
            )
            exports.add_problematic_list(problem_list)

        return exports

    @staticmethod
    def parse_fd(file: IO[bytes], schema: dict) -> "CustomListExports":
        assert type(schema) == dict
        return CustomListExports.parse(json.load(file), schema)

    @staticmethod
    def parse_file(file: str, schema: dict) -> "CustomListExports":
        assert type(schema) == dict
        with open(file, "rb") as source_file:
            return CustomListExports.parse_fd(source_file, schema)

    def lists(self) -> Iterable[CustomList]:
        return (cl for cl in self._lists)

    def problematic_lists(self) -> Iterable[ProblematicCustomList]:
        return (cl for cl in self._problematic_lists)


class CustomListExportFailed(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class CustomListExporter:
    _session: Session
    _logger: logging.Logger
    _server_base: str
    _email: str
    _password: str
    _output_file: str
    _library_name: str
    _schema_file: str
    _lists: list[str]

    @staticmethod
    def _fatal(message: str):
        raise CustomListExportFailed(message)

    @staticmethod
    def _parse_arguments(args: list[str]) -> argparse.Namespace:
        parser: argparse.ArgumentParser = argparse.ArgumentParser(
            description="Fetch one or more custom lists."
        )
        parser.add_argument(
            "--schema-file",
            help="The path to customlists.schema.json",
            required=False,
            default="customlists/customlists.schema.json",
        )
        parser.add_argument("--server", help="The address of the CM", required=True)
        parser.add_argument("--username", help="The CM admin username", required=True)
        parser.add_argument("--password", help="The CM admin password", required=True)
        parser.add_argument("--output", help="The output file", required=True)
        parser.add_argument(
            "--library-name",
            help="The short name of the library that owns the lists.",
            required=True,
        )
        parser.add_argument(
            "--list", help="Only export the named list (may be repeated)", nargs="+"
        )
        parser.add_argument(
            "--verbose",
            "-v",
            action="count",
            default=0,
            help="Increase verbosity (can be specified multiple times to export multiple lists)",
        )
        return parser.parse_args(args)

    def _make_custom_list(self, raw_list: dict) -> CustomList | ProblematicCustomList:
        id: int = raw_list["id"]
        name: str = raw_list["name"]

        # The /admin/custom_list/ URL will yield an OPDS feed of the list contents.
        server_list_endpoint: str = (
            f"{self._server_base}/{self._library_name}/admin/custom_list/{id}"
        )
        response = self._session.get(server_list_endpoint)
        if response.status_code >= 400:
            return ProblematicCustomList(
                id=id,
                name=name,
                error=f"Failed to retrieve custom list {id}: {response.status_code} {response.reason}",
            )

        feed = feedparser.parse(url_file_stream_or_string=response.content)
        custom_list = CustomList(id=id, name=name, library_id=self._library_name)

        # Now, for each book, extract the book identifier and identifier type.
        for entry in feed.entries:
            entry_id: str = unquote(entry.id)
            added = False
            for link in entry.links:
                if link.rel == "alternate":
                    match = re.search("^(.*)/works/([^/]+)/(.*)$", link.href)
                    if match is not None:
                        link_id_quoted: str = match.group(3)
                        link_id_type_quoted: str = match.group(2)
                        link_id: str = unquote(link_id_quoted)
                        link_id_type: str = unquote(link_id_type_quoted)

                        self._logger.debug(f"storing link id (id_value) {link_id}")
                        self._logger.debug(
                            f"storing link id type (id_type) {link_id_type}"
                        )
                        self._logger.debug(f"storing entry id (id_full) {entry_id}")
                        self._logger.debug(f"storing entry title {entry.title}")
                        self._logger.debug(f"storing entry author {entry.author}")

                        custom_list.add_book(
                            Book(
                                id_value=link_id,
                                id_type=link_id_type,
                                id_full=entry_id,
                                title=entry.title,
                                author=entry.author,
                            )
                        )
                        added = True
                        break
            if not added:
                custom_list.add_problematic_book(
                    ProblematicBook(
                        id=entry_id,
                        title=entry.title,
                        message=f"Could not determine the identifier type for book {entry.title} (OPDS entry ID: {entry_id})",
                        author=entry.author,
                    )
                )

        # Extract any collection references
        raw_collections = raw_list["collections"] or []
        for raw_collection in raw_collections:
            collection = CollectionReference(
                id=raw_collection["id"],
                name=raw_collection["name"],
                protocol=raw_collection["protocol"],
            )
            custom_list.add_collection(collection)

        self._logger.info(f"Retrieved {custom_list.size()} books for list {id}")
        return custom_list

    def _make_custom_lists_document(self) -> CustomListExports:
        self._logger.info("Fetching lists...")
        server_lists_endpoint: str = (
            f"{self._server_base}/{self._library_name}/admin/custom_lists"
        )
        response = self._session.get(server_lists_endpoint)
        if response.status_code >= 400:
            CustomListExporter._fatal(
                f"Failed to retrieve custom lists: {response.status_code} {response.reason}"
            )

        raw_document = json.loads(response.content)
        raw_lists: list = raw_document["custom_lists"] or []

        custom_lists = CustomListExports()
        for raw_list in raw_lists:
            name = raw_list["name"]
            if self._lists:
                if name not in self._lists:
                    self._logger.info(f"Excluding list '{name}'")
                    continue

            result_list = self._make_custom_list(raw_list)
            if type(result_list) is ProblematicCustomList:
                custom_lists.add_problematic_list(result_list)
            else:
                if type(result_list) is CustomList:
                    custom_lists.add_list(result_list)

        self._logger.info(f"Retrieved {custom_lists.size()} custom lists")
        return custom_lists

    def _sign_in(self) -> None:
        server_login_endpoint: str = f"{self._server_base}/admin/sign_in_with_password"
        headers = {"User-Agent": "circulation-customlists-fetch/1.0"}
        payload = {"email": self._email, "password": self._password}

        self._logger.info("Signing in...")
        response = self._session.post(
            server_login_endpoint, headers=headers, data=payload, allow_redirects=False
        )
        if response.status_code >= 400:
            CustomListExporter._fatal(
                f"Failed to sign in: {response.status_code} {response.reason}"
            )

    def _save_customlists_document(self, document: CustomListExports) -> None:
        with open(self._schema_file, "rb") as schema_file:
            schema: Mapping[str, Any] = json.load(schema_file)

        output_file_tmp: str = self._output_file + ".tmp"
        serialized: str = document.serialize(schema)
        with open(output_file_tmp, "wb") as out:
            out.write(serialized.encode("utf-8"))

        os.rename(output_file_tmp, self._output_file)

    def execute(self) -> None:
        self._sign_in()
        document = self._make_custom_lists_document()
        self._save_customlists_document(document)

    def __init__(self, args: argparse.Namespace):
        self._session = requests.Session()
        self._logger = logging.getLogger("CustomListExporter")
        self._server_base = args.server.rstrip("/")
        self._email = args.username
        self._password = args.password
        self._output_file = args.output
        self._schema_file = args.schema_file
        self._library_name = args.library_name
        self._lists = args.list
        verbose: int = args.verbose or 0
        if verbose > 0:
            self._logger.setLevel(logging.INFO)
        if verbose > 1:
            self._logger.setLevel(logging.DEBUG)

    @staticmethod
    def create(args: list[str]) -> "CustomListExporter":
        return CustomListExporter(CustomListExporter._parse_arguments(args))
