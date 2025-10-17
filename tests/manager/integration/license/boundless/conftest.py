from functools import partial

import pytest

from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.contributor import ContributorData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.subject import SubjectData
from palace.manager.integration.license.boundless.api import BoundlessApi
from palace.manager.integration.license.boundless.importer import BoundlessImporter
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.util.datetime_helpers import datetime_utc
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import BoundlessFilesFixture
from tests.fixtures.http import MockHttpClientFixture
from tests.fixtures.services import ServicesFixture


class BoundlessFixture:
    # Sample bibliographic and availability data you can use in a test
    # without having to parse it from an XML file.

    CIRCULATION_DATA = CirculationData(
        data_source_name=DataSource.BOUNDLESS,
        primary_identifier_data=IdentifierData(
            type=Identifier.AXIS_360_ID, identifier="0003642860"
        ),
        licenses_owned=9,
        licenses_available=8,
        licenses_reserved=0,
        patrons_in_hold_queue=0,
        last_checked=datetime_utc(2015, 5, 20, 2, 9, 8),
    )

    BIBLIOGRAPHIC_DATA = BibliographicData(
        data_source_name=DataSource.BOUNDLESS,
        publisher="Random House Inc",
        language="eng",
        title="Faith of My Fathers : A Family Memoir",
        imprint="Random House Inc2",
        published=datetime_utc(2000, 3, 7, 0, 0),
        primary_identifier_data=CIRCULATION_DATA.primary_identifier_data,
        identifiers=[IdentifierData(type=Identifier.ISBN, identifier="9780375504587")],
        contributors=[
            ContributorData(
                sort_name="McCain, John", roles=[Contributor.Role.PRIMARY_AUTHOR]
            ),
            ContributorData(sort_name="Salter, Mark", roles=[Contributor.Role.AUTHOR]),
        ],
        subjects=[
            SubjectData(
                type=Subject.BISAC, identifier="BIOGRAPHY & AUTOBIOGRAPHY / Political"
            ),
            SubjectData(type=Subject.FREEFORM_AUDIENCE, identifier="Adult"),
        ],
        circulation=CIRCULATION_DATA,
    )

    def __init__(
        self,
        db: DatabaseTransactionFixture,
        http_client: MockHttpClientFixture,
        files: BoundlessFilesFixture,
        services_fixture: ServicesFixture,
    ):
        self.db = db
        self.files = files
        self.collection = db.collection(
            protocol=BoundlessApi, library=db.default_library()
        )
        self.http_client = http_client
        self.api = BoundlessApi(db.session, self.collection)
        registry = services_fixture.services.integration_registry().license_providers()
        self.create_importer = partial(
            BoundlessImporter,
            self.db.session,
            collection=self.collection,
            registry=registry,
        )


@pytest.fixture(scope="function")
def boundless(
    db: DatabaseTransactionFixture,
    http_client: MockHttpClientFixture,
    boundless_files_fixture: BoundlessFilesFixture,
    services_fixture: ServicesFixture,
) -> BoundlessFixture:
    # Typically the first request to the api will trigger a token refresh, so we queue
    # up a response for that.
    http_client.queue_response(
        200,
        content=boundless_files_fixture.sample_text("token.json"),
    )

    return BoundlessFixture(
        db,
        http_client,
        boundless_files_fixture,
        services_fixture,
    )
