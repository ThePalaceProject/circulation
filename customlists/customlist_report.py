import json
import logging
from typing import Iterable, List

import jsonschema

logger = logging.getLogger()


class CustomListProblem:
    _message: str

    def __init__(self, message: str):
        self._message = message

    def message(self) -> str:
        return self._message

    def to_dict(self) -> dict:
        raise NotImplementedError


class CustomListProblemBookMismatch(CustomListProblem):
    def __init__(
        self,
        message: str,
        expected_id: str,
        expected_title: str,
        received_id: str,
        received_title: str,
    ):
        super().__init__(message)
        self._expected_id = expected_id
        self._expected_title = expected_title
        self._received_id = received_id
        self._received_title = received_title

    @classmethod
    def create(
        cls,
        expected_id: str,
        expected_title: str,
        received_id: str,
        received_title: str,
    ) -> "CustomListProblemBookMismatch":
        return CustomListProblemBookMismatch(
            f"The book '{expected_title}' (id {expected_id}) appears to have title '{received_title}' (id {received_id}) on the importing CM",
            expected_id=expected_id,
            expected_title=expected_title,
            received_id=received_id,
            received_title=received_title,
        )

    def to_dict(self) -> dict:
        return {
            "%type": "problem-book-mismatch",
            "expected-id": self._expected_id,
            "expected-title": self._expected_title,
            "received-id": self._received_id,
            "received-title": self._received_title,
            "message": self.message(),
        }


class CustomListProblemBookMissing(CustomListProblem):
    def __init__(self, message: str, id: str, title: str):
        super().__init__(message)
        self._id = id
        self._title = title

    @classmethod
    def create(cls, id: str, title: str) -> "CustomListProblemBookMissing":
        return CustomListProblemBookMissing(
            f"The book '{title}' (id {id}) appears to be missing on the importing CM",
            id=id,
            title=title,
        )

    def to_dict(self) -> dict:
        return {
            "%type": "problem-book-missing",
            "id": self._id,
            "title": self._title,
            "message": self.message(),
        }


class CustomListProblemBookRequestFailed(CustomListProblem):
    def __init__(self, message: str, id: str, title: str):
        super().__init__(message)
        self._id = id
        self._title = title

    @classmethod
    def create(
        cls, id: str, title: str, error: str
    ) -> "CustomListProblemBookRequestFailed":
        return CustomListProblemBookRequestFailed(
            f"A request for book '{title}' (id {id}) failed on the target CM: {error}",
            id=id,
            title=title,
        )

    def to_dict(self) -> dict:
        return {
            "%type": "problem-request-failed",
            "id": self._id,
            "title": self._title,
            "message": self.message(),
        }


class CustomListProblemBookBrokenOnSourceCM(CustomListProblem):
    def __init__(self, message: str, id: str, title: str):
        super().__init__(message)
        self._id = id
        self._title = title

    def to_dict(self) -> dict:
        return {
            "%type": "problem-book-broken-on-source",
            "id": self._id,
            "title": self._title,
            "message": self.message(),
        }


class CustomListProblemListAlreadyExists(CustomListProblem):
    def __init__(self, message: str, id: int, name: str):
        super().__init__(message)
        self._id = id
        self._name = name

    def to_dict(self) -> dict:
        return {
            "%type": "problem-list-already-exists",
            "id": self._id,
            "name": self._name,
            "message": self.message(),
        }


class CustomListProblemListUpdateFailed(CustomListProblem):
    def __init__(self, message: str, id: int, name: str):
        super().__init__(message)
        self._id = id
        self._name = name

    def to_dict(self) -> dict:
        return {
            "%type": "problem-list-update-failed",
            "id": self._id,
            "name": self._name,
            "message": self.message(),
        }


class CustomListProblemListBroken(CustomListProblem):
    def __init__(self, message: str, id: int, name: str):
        super().__init__(message)
        self._id = id
        self._name = name

    def to_dict(self) -> dict:
        return {
            "%type": "problem-list-broken",
            "id": self._id,
            "name": self._name,
            "message": self.message(),
        }


class CustomListReport:
    _errors: List[CustomListProblem]
    _id: int
    _name: str

    def __init__(self, id: int, name: str):
        self._errors = []
        self._id = id
        self._name = name

    def id(self) -> int:
        return self._id

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
        if problem_type == "problem-list-broken":
            return CustomListProblemListBroken(
                message=raw_problem["message"],
                id=raw_problem["id"],
                name=raw_problem["name"],
            )
        if problem_type == "problem-list-update-failed":
            return CustomListProblemListUpdateFailed(
                message=raw_problem["message"],
                id=raw_problem["id"],
                name=raw_problem["name"],
            )
        if problem_type == "problem-list-already-exists":
            return CustomListProblemListAlreadyExists(
                message=raw_problem["message"],
                id=raw_problem["id"],
                name=raw_problem["name"],
            )
        if problem_type == "problem-book-broken-on-source":
            return CustomListProblemBookBrokenOnSourceCM(
                message=raw_problem["message"],
                id=raw_problem["id"],
                title=raw_problem["title"],
            )
        if problem_type == "problem-book-request-failed":
            return CustomListProblemBookRequestFailed(
                message=raw_problem["message"],
                id=raw_problem["id"],
                title=raw_problem["title"],
            )
        if problem_type == "problem-book-missing":
            return CustomListProblemBookMissing(
                message=raw_problem["message"],
                id=raw_problem["id"],
                title=raw_problem["title"],
            )
        if problem_type == "problem-book-mismatch":
            return CustomListProblemBookMismatch(
                message=raw_problem["message"],
                expected_id=raw_problem["expected_id"],
                expected_title=raw_problem["expected_title"],
                received_id=raw_problem["received_id"],
                received_title=raw_problem["received_title"],
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
    _reports: List[CustomListReport]

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
