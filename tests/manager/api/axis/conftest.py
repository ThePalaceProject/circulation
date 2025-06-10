import pytest
from fixtures.work import WorkIdPolicyQueuePresentationRecalculationFixture

from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.contributor import ContributorData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.subject import SubjectData
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.util.datetime_helpers import datetime_utc
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import AxisFilesFixture
from tests.mocks.axis import MockAxis360API


class Axis360Fixture:
    # Sample bibliographic and availability data you can use in a test
    # without having to parse it from an XML file.

    CIRCULATION_DATA = CirculationData(
        data_source_name=DataSource.AXIS_360,
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
        data_source_name=DataSource.AXIS_360,
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
        files: AxisFilesFixture,
        work_policy_recalc_fixture: WorkIdPolicyQueuePresentationRecalculationFixture,
    ):
        self.db = db
        self.files = files
        self.collection = MockAxis360API.mock_collection(
            db.session, db.default_library()
        )
        self.api = MockAxis360API(db.session, self.collection)
        self.work_policy_recalc_fixture = work_policy_recalc_fixture

    def sample_data(self, filename):
        return self.files.sample_data(filename)

    def sample_text(self, filename):
        return self.files.sample_text(filename)


@pytest.fixture(scope="function")
def axis360(
    db: DatabaseTransactionFixture,
    axis_files_fixture: AxisFilesFixture,
    work_policy_recalc_fixture: WorkIdPolicyQueuePresentationRecalculationFixture,
) -> Axis360Fixture:
    return Axis360Fixture(db, axis_files_fixture, work_policy_recalc_fixture)
