import json
from typing import IO, Iterable, List

import jsonschema


class Book:
    _id: str
    _id_type: str
    _title: str

    def __init__(self, id: str, id_type: str, title: str):
        self._id = id
        self._id_type = id_type
        self._title = title

    def id(self) -> str:
        return self._id

    def id_type(self) -> str:
        return self._id_type

    def title(self) -> str:
        return self._title

    def to_dict(self) -> dict:
        return {
            "%type": "book",
            "id": self._id,
            "id-type": self._id_type,
            "title": self._title,
        }


class ProblematicBook:
    _id: str
    _title: str
    _message: str

    def __init__(self, id: str, title: str, message: str):
        self._id = id
        self._message = message
        self._title = title

    def id(self) -> str:
        return self._id

    def message(self) -> str:
        return self._message

    def title(self) -> str:
        return self._title

    def to_dict(self) -> dict:
        return {
            "%type": "problematic-book",
            "id": self._id,
            "message": self._message,
            "title": self._title,
        }


class CustomList:
    _books: List[Book]
    _problematic_books: List[ProblematicBook]
    _id: int
    _name: str

    def __init__(self, id: int, name: str):
        self._books = []
        self._problematic_books = []
        self._id = id
        self._name = name

    def id(self) -> int:
        return self._id

    def add_book(self, book: Book) -> None:
        self._books.append(book)

    def add_problematic_book(self, book: ProblematicBook) -> None:
        self._problematic_books.append(book)

    def size(self) -> int:
        return len(self._books)

    def to_dict(self) -> dict:
        return {
            "%type": "customlist",
            "books": list(map(lambda b: b.to_dict(), self._books)),
            "problematic-books": list(
                map(lambda b: b.to_dict(), self._problematic_books)
            ),
            "id": self._id,
            "name": self._name,
        }

    def books(self) -> Iterable[Book]:
        return (b for b in self._books)

    def problematic_books(self) -> Iterable[ProblematicBook]:
        return (b for b in self._problematic_books)

    def name(self) -> str:
        return self._name


class CustomListExports:
    _lists: List[CustomList]

    def __init__(self):
        self._lists = []

    def add_list(self, list: CustomList) -> None:
        self._lists.append(list)

    def to_dict(self) -> dict:
        return {
            "%id": "https://schemas.thepalaceproject.io/customlists/1.0",
            "customlists": list(map(lambda c: c.to_dict(), self._lists)),
        }

    def size(self) -> int:
        return len(self._lists)

    def serialize(self, schema: str) -> str:
        document_dict = self.to_dict()
        jsonschema.validate(document_dict, schema)
        return json.dumps(document_dict, sort_keys=True, indent=2)

    @classmethod
    def parse(cls, document: dict, schema: str) -> "CustomListExports":
        jsonschema.validate(document, schema)
        exports = CustomListExports()
        for raw_list in document["customlists"]:
            custom_list = CustomList(raw_list["id"], raw_list["name"])
            for raw_book in raw_list["books"]:
                book = Book(
                    id=raw_book["id"],
                    id_type=raw_book["id-type"],
                    title=raw_book["title"],
                )
                custom_list.add_book(book)
            for raw_book in raw_list["problematic-books"]:
                problem_book = ProblematicBook(
                    id=raw_book["id"],
                    message=raw_book["message"],
                    title=raw_book["title"],
                )
                custom_list.add_problematic_book(problem_book)
            exports.add_list(custom_list)

        return exports

    @classmethod
    def parse_fd(cls, file: IO[bytes], schema: str) -> "CustomListExports":
        return CustomListExports.parse(json.load(file), schema)

    @classmethod
    def parse_file(cls, file: str, schema: str) -> "CustomListExports":
        with open(file, "rb") as source_file:
            return CustomListExports.parse_fd(source_file, schema)

    def lists(self) -> Iterable[CustomList]:
        return (cl for cl in self._lists)
