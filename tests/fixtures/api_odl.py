from __future__ import annotations

import datetime
import json
import uuid
from typing import TYPE_CHECKING, Any

import pytest
from jinja2 import Template

from api.odl import BaseODLImporter, ODLImporter
from api.odl2 import ODL2API, ODL2Importer
from core.coverage import CoverageFailure
from core.model import Edition, LicensePool, Work
from tests.fixtures.files import APIFilesFixture

if TYPE_CHECKING:
    from tests.fixtures.database import DatabaseTransactionFixture
    from tests.fixtures.odl import ODLTestFixture


class LicenseHelper:
    """Represents an ODL license."""

    def __init__(
        self,
        identifier: str | None = None,
        checkouts: int | None = None,
        concurrency: int | None = None,
        expires: datetime.datetime | str | None = None,
    ) -> None:
        """Initialize a new instance of LicenseHelper class.

        :param identifier: License's identifier
        :param checkouts: Total number of checkouts before a license expires
        :param concurrency: Number of concurrent checkouts allowed
        :param expires: Date & time when a license expires
        """
        self.identifier: str = identifier if identifier else f"urn:uuid:{uuid.uuid1()}"
        self.checkouts: int | None = checkouts
        self.concurrency: int | None = concurrency
        if isinstance(expires, datetime.datetime):
            self.expires = expires.isoformat()
        else:
            self.expires: str | None = expires  # type: ignore


class LicenseInfoHelper:
    """Represents information about the current state of a license stored in the License Info Document."""

    def __init__(
        self,
        license: LicenseHelper,
        available: int,
        status: str = "available",
        left: int | None = None,
    ) -> None:
        """Initialize a new instance of LicenseInfoHelper class."""
        self.license: LicenseHelper = license
        self.status: str = status
        self.left: int | None = left
        self.available: int = available

    def __str__(self) -> str:
        """Return a JSON representation of a part of the License Info Document."""
        output = {
            "identifier": self.license.identifier,
            "status": self.status,
            "terms": {
                "concurrency": self.license.concurrency,
            },
            "checkouts": {
                "available": self.available,
            },
        }
        if self.license.expires is not None:
            output["terms"]["expires"] = self.license.expires  # type: ignore
        if self.left is not None:
            output["checkouts"]["left"] = self.left  # type: ignore
        return json.dumps(output)


class ODLAPIFilesFixture(APIFilesFixture):
    """A fixture providing access to ODL files."""

    def __init__(self):
        super().__init__("odl")


@pytest.fixture()
def api_odl_files_fixture() -> ODLAPIFilesFixture:
    """A fixture providing access to ODL files."""
    return ODLAPIFilesFixture()


class ODL2APIFilesFixture(APIFilesFixture):
    """A fixture providing access to ODL2 files."""

    def __init__(self):
        super().__init__("odl2")


@pytest.fixture()
def api_odl2_files_fixture() -> ODL2APIFilesFixture:
    """A fixture providing access to ODL2 files."""
    return ODL2APIFilesFixture()


class MockGet:
    def __init__(self):
        self.responses = []

    def get(self, *args: Any, **kwargs: Any) -> tuple[int, dict[str, str], bytes]:
        return 200, {}, self.responses.pop(0)

    def add(self, item: LicenseInfoHelper | str | bytes) -> None:
        if isinstance(item, LicenseInfoHelper):
            self.responses.append(str(item).encode("utf-8"))
        elif isinstance(item, str):
            self.responses.append(item.encode("utf-8"))
        elif isinstance(item, bytes):
            self.responses.append(item)


@pytest.fixture()
def odl_mock_get() -> MockGet:
    return MockGet()


@pytest.fixture()
def odl_importer(
    db: DatabaseTransactionFixture,
    odl_test_fixture: ODLTestFixture,
    odl_mock_get: MockGet,
) -> ODLImporter:
    library = odl_test_fixture.library()
    return ODLImporter(
        db.session,
        collection=odl_test_fixture.collection(library),
        http_get=odl_mock_get.get,
    )


@pytest.fixture()
def odl2_importer(
    db: DatabaseTransactionFixture,
    odl_test_fixture: ODLTestFixture,
    odl_mock_get: MockGet,
) -> ODL2Importer:
    library = odl_test_fixture.library()
    return ODL2Importer(
        db.session,
        collection=odl_test_fixture.collection(library, ODL2API),
        http_get=odl_mock_get.get,
    )


class OdlImportTemplatedFixture:
    def __init__(
        self,
        odl_mock_get: MockGet,
        importer: BaseODLImporter,
        files_fixture: APIFilesFixture,
        feed_template: str,
    ):
        self.mock_get = odl_mock_get
        self.importer = importer
        self.files_fixture = files_fixture
        self.feed_template = feed_template

    def __call__(
        self, licenses: list[LicenseInfoHelper]
    ) -> tuple[
        list[Edition],
        list[LicensePool],
        list[Work],
        dict[str, list[CoverageFailure]],
    ]:
        feed_licenses = [l.license for l in licenses]
        for _license in licenses:
            self.mock_get.add(_license)
        feed = self.get_templated_feed(
            files=self.files_fixture,
            filename=self.feed_template,
            licenses=feed_licenses,
        )
        return self.importer.import_from_feed(feed)

    def get_templated_feed(
        self, files: APIFilesFixture, filename: str, licenses: list[LicenseHelper]
    ) -> str:
        """Get the test ODL feed with specific licensing information.

        :param files: Access to test files
        :param filename: Name of template to load
        :param licenses: List of ODL licenses

        :return: Test ODL feed
        """
        text = files.sample_text(filename)
        template = Template(text)
        feed = template.render(licenses=licenses)
        return feed


@pytest.fixture(params=["odl", "odl2"])
def odl_import_templated(
    request: pytest.FixtureRequest,
    odl_mock_get: MockGet,
    odl_importer: ODLImporter,
    odl2_importer: ODL2Importer,
    api_odl_files_fixture: ODLAPIFilesFixture,
    api_odl2_files_fixture: ODL2APIFilesFixture,
) -> OdlImportTemplatedFixture:
    if request.param == "odl":
        return OdlImportTemplatedFixture(
            odl_mock_get, odl_importer, api_odl_files_fixture, "feed_template.xml.jinja"
        )
    elif request.param == "odl2":
        return OdlImportTemplatedFixture(
            odl_mock_get,
            odl2_importer,
            api_odl2_files_fixture,
            "feed_template.json.jinja",
        )

    raise ValueError("Unknown param")
