import json
import logging
from collections.abc import Iterable

import jsonschema

logger = logging.getLogger()


class CustomListProblem:
    _message: str

    def __init__(self, message: str):
        self._message = message
        assert message

    def message(self) -> str:
        return self._message

    def to_dict(self) -> dict:
        raise NotImplementedError


class CustomListBookProblem(CustomListProblem):
    _id: str
    _id_type: str
    _title: str
    _author: str

    def __init__(self, message: str, id: str, id_type: str, title: str, author: str):
        super().__init__(message)
        self._id = id
        self._id_type = id_type
        self._title = title
        self._author = author
        assert id
        assert id_type
        assert title
        assert author

    def id(self) -> str:
        return self._id

    def id_type(self) -> str:
        return self._id_type

    def title(self) -> str:
        return self._title

    def author(self) -> str:
        return self._author

    def to_dict(self) -> dict:
        raise NotImplementedError


class CustomListProblemBookMismatch(CustomListBookProblem):
    TYPE = "problem-book-mismatch"

    def __init__(
        self,
        message: str,
        expected_id: str,
        expected_id_type: str,
        expected_title: str,
        received_id: str,
        received_title: str,
        author: str,
    ):
        super().__init__(
            message,
            id=expected_id,
            id_type=expected_id_type,
            title=expected_title,
            author=author,
        )
        self._expected_id = expected_id
        self._expected_id_type = expected_id_type
        self._expected_title = expected_title
        self._received_id = received_id
        self._received_title = received_title
        self._author = author

    @classmethod
    def create(
        cls,
        expected_id: str,
        expected_id_type: str,
        expected_title: str,
        received_id: str,
        received_title: str,
        author: str,
    ) -> "CustomListProblemBookMismatch":
        return CustomListProblemBookMismatch(
            f"Book is mismatched on the importing CM. Expected title is '{expected_title}', received title is '{received_title}'. Expected ID is '{expected_id}', received ID is '{received_id}'.",
            expected_id=expected_id,
            expected_id_type=expected_id_type,
            expected_title=expected_title,
            received_id=received_id,
            received_title=received_title,
            author=author,
        )

    def to_dict(self) -> dict:
        return {
            "%type": CustomListProblemBookMismatch.TYPE,
            "expected-id": self.expected_id(),
            "expected-id-type": self._expected_id_type,
            "expected-title": self.expected_title(),
            "received-id": self.received_id(),
            "received-title": self.received_title(),
            "author": self.author(),
            "message": self.message(),
        }

    def expected_id(self) -> str:
        return self._expected_id

    def expected_title(self) -> str:
        return self._expected_title

    def received_id(self) -> str:
        return self._received_id

    def received_title(self) -> str:
        return self._received_title


class CustomListProblemBookMissing(CustomListBookProblem):
    TYPE = "problem-book-missing"

    def __init__(self, message: str, id: str, id_type: str, title: str, author: str):
        super().__init__(message, id=id, id_type=id_type, title=title, author=author)

    @classmethod
    def create(
        cls, id: str, id_type: str, title: str, author: str
    ) -> "CustomListProblemBookMissing":
        return CustomListProblemBookMissing(
            f"The book '{title}' (id {id}) does not appear to be present in the target library on the importing CM",
            id=id,
            id_type=id_type,
            title=title,
            author=author,
        )

    def to_dict(self) -> dict:
        return {
            "%type": CustomListProblemBookMissing.TYPE,
            "id": self.id(),
            "id-type": self.id_type(),
            "title": self.title(),
            "message": self.message(),
            "author": self.author(),
        }


class CustomListProblemBookRequestFailed(CustomListBookProblem):
    TYPE = "problem-book-request-failed"

    def __init__(self, message: str, id: str, id_type: str, title: str, author: str):
        super().__init__(message, id=id, id_type=id_type, title=title, author=author)

    @classmethod
    def create(
        cls, id: str, id_type: str, title: str, error: str, author: str
    ) -> "CustomListProblemBookRequestFailed":
        return CustomListProblemBookRequestFailed(
            f"A request for book '{title}' (id {id}) failed on the target CM: {error}",
            id=id,
            id_type=id_type,
            title=title,
            author=author,
        )

    def to_dict(self) -> dict:
        return {
            "%type": CustomListProblemBookRequestFailed.TYPE,
            "id": self.id(),
            "id-type": self.id_type(),
            "title": self.title(),
            "message": self.message(),
            "author": self.author(),
        }


class CustomListProblemBookBrokenOnSourceCM(CustomListBookProblem):
    TYPE = "problem-book-broken-on-source"

    def __init__(self, message: str, id: str, id_type: str, title: str, author: str):
        super().__init__(message, id=id, id_type=id_type, title=title, author=author)

    def to_dict(self) -> dict:
        return {
            "%type": CustomListProblemBookBrokenOnSourceCM.TYPE,
            "id": self.id(),
            "id-type": self.id_type(),
            "title": self.title(),
            "message": self.message(),
            "author": self.author(),
        }


class CustomListProblemListAlreadyExists(CustomListProblem):
    TYPE = "problem-list-already-exists"

    def __init__(self, message: str, id: int, name: str):
        super().__init__(message)
        self._id = id
        self._name = name

    def to_dict(self) -> dict:
        return {
            "%type": CustomListProblemListAlreadyExists.TYPE,
            "id": self._id,
            "name": self._name,
            "message": self.message(),
        }


class CustomListProblemListUpdateFailed(CustomListProblem):
    TYPE = "problem-list-update-failed"

    def __init__(self, message: str, id: int, name: str):
        super().__init__(message)
        self._id = id
        self._name = name

    def to_dict(self) -> dict:
        return {
            "%type": CustomListProblemListUpdateFailed.TYPE,
            "id": self._id,
            "name": self._name,
            "message": self.message(),
        }


class CustomListProblemListBroken(CustomListProblem):
    TYPE = "problem-list-broken"

    def __init__(self, message: str, id: int, name: str):
        super().__init__(message)
        self._id = id
        self._name = name

    def to_dict(self) -> dict:
        return {
            "%type": CustomListProblemListBroken.TYPE,
            "id": self._id,
            "name": self._name,
            "message": self.message(),
        }


class CustomListProblemCollectionMissing(CustomListProblem):
    TYPE = "problem-collection-missing"

    def __init__(self, message: str, name: str):
        super().__init__(message)
        self._name = name

    @classmethod
    def create(cls, name: str) -> "CustomListProblemCollectionMissing":
        return CustomListProblemCollectionMissing(
            f"The collection '{name}' appears to be missing on the importing CM",
            name=name,
        )

    def to_dict(self) -> dict:
        return {
            "%type": CustomListProblemCollectionMissing.TYPE,
            "name": self._name,
            "message": self.message(),
        }

    def name(self) -> str:
        return self._name


class CustomListProblemCollectionRequestFailed(CustomListProblem):
    TYPE = "problem-collection-request-failed"

    def __init__(self, message: str, name: str):
        super().__init__(message)
        self._name = name

    @classmethod
    def create(
        cls, name: str, error: str
    ) -> "CustomListProblemCollectionRequestFailed":
        return CustomListProblemCollectionRequestFailed(
            f"The request for collection '{name}' failed: {error}", name=name
        )

    def to_dict(self) -> dict:
        return {
            "%type": CustomListProblemCollectionRequestFailed.TYPE,
            "name": self._name,
            "message": self.message(),
        }

    def name(self) -> str:
        return self._name


class CustomListReport:
    _errors: list[CustomListProblem]
    _id: int
    _name: str

    def __init__(self, id: int, name: str):
        self._errors = []
        self._id = id
        self._name = name

    def id(self) -> int:
        return self._id

    def name(self) -> str:
        return self._name

    def add_problem(self, problem: CustomListProblem) -> None:
        self._errors.append(problem)

    def problems(self) -> Iterable[CustomListProblem]:
        return (p for p in self._errors)

    def to_dict(self) -> dict:
        return {
            "list-id": self._id,
            "list-name": self._name,
            "problems": list(map(lambda p: p.to_dict(), self._errors)),
        }

    @staticmethod
    def _parse_problem(raw_problem: dict) -> CustomListProblem:
        problem_type = raw_problem["%type"]
        if problem_type == CustomListProblemListBroken.TYPE:
            return CustomListProblemListBroken(
                message=raw_problem["message"],
                id=raw_problem["id"],
                name=raw_problem["name"],
            )
        if problem_type == CustomListProblemListUpdateFailed.TYPE:
            return CustomListProblemListUpdateFailed(
                message=raw_problem["message"],
                id=raw_problem["id"],
                name=raw_problem["name"],
            )
        if problem_type == CustomListProblemListAlreadyExists.TYPE:
            return CustomListProblemListAlreadyExists(
                message=raw_problem["message"],
                id=raw_problem["id"],
                name=raw_problem["name"],
            )
        if problem_type == CustomListProblemBookBrokenOnSourceCM.TYPE:
            return CustomListProblemBookBrokenOnSourceCM(
                message=raw_problem["message"],
                id=raw_problem["id"],
                id_type=raw_problem["id-type"],
                title=raw_problem["title"],
                author=raw_problem["author"],
            )
        if problem_type == CustomListProblemBookRequestFailed.TYPE:
            return CustomListProblemBookRequestFailed(
                message=raw_problem["message"],
                id=raw_problem["id"],
                id_type=raw_problem["id-type"],
                title=raw_problem["title"],
                author=raw_problem["author"],
            )
        if problem_type == CustomListProblemBookMissing.TYPE:
            return CustomListProblemBookMissing(
                message=raw_problem["message"],
                id=raw_problem["id"],
                id_type=raw_problem["id-type"],
                title=raw_problem["title"],
                author=raw_problem["author"],
            )
        if problem_type == CustomListProblemBookMismatch.TYPE:
            return CustomListProblemBookMismatch(
                message=raw_problem["message"],
                expected_id=raw_problem["expected-id"],
                expected_id_type=raw_problem["expected-id-type"],
                expected_title=raw_problem["expected-title"],
                received_id=raw_problem["received-id"],
                received_title=raw_problem["received-title"],
                author=raw_problem["author"],
            )
        if problem_type == CustomListProblemCollectionRequestFailed.TYPE:
            return CustomListProblemCollectionRequestFailed(
                message=raw_problem["message"], name=raw_problem["name"]
            )
        if problem_type == CustomListProblemCollectionMissing.TYPE:
            return CustomListProblemCollectionMissing(
                message=raw_problem["message"], name=raw_problem["name"]
            )
        else:
            raise RuntimeError(f"Unexpected type: {problem_type}")

    @staticmethod
    def _parse(document: dict) -> "CustomListReport":
        report = CustomListReport(id=document["list-id"], name=document["list-name"])
        for problem in document["problems"]:
            report.add_problem(CustomListReport._parse_problem(problem))
        return report


class CustomListsReport:
    _reports: list[CustomListReport]

    def __init__(self):
        self._reports = []

    def add_report(self, report: CustomListReport) -> None:
        self._reports.append(report)

    def reports(self) -> Iterable[CustomListReport]:
        return (p for p in self._reports)

    def to_dict(self) -> dict:
        return {
            "%id": "https://schemas.thepalaceproject.io/customlists-report/1.0",
            "reports": list(map(lambda p: p.to_dict(), self._reports)),
        }

    def serialize(self, schema: dict) -> str:
        assert type(schema) == dict

        document_dict = self.to_dict()
        jsonschema.validate(document_dict, schema)
        return json.dumps(document_dict, sort_keys=True, indent=2)

    @staticmethod
    def parse(schema: dict, document: dict) -> "CustomListsReport":
        assert type(schema) == dict

        jsonschema.validate(document, schema)
        report = CustomListsReport()
        for raw_report in document["reports"]:
            report.add_report(CustomListReport._parse(raw_report))
        return report
