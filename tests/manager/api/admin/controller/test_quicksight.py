import uuid
from collections.abc import Generator
from functools import partial
from typing import Any, cast
from unittest import mock

import pytest

from palace.manager.api.admin.controller.quicksight import QuickSightController
from palace.manager.sqlalchemy.model.admin import Admin, AdminRole
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.util import create
from palace.manager.util.problem_detail import ProblemDetailException
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.flask import FlaskAppFixture


class QuickSightControllerFixture:
    def __init__(self, db: DatabaseTransactionFixture, mock_boto: mock.MagicMock):
        self.mock_boto = mock_boto
        self.db = db
        self.mock_generate = (
            self.mock_boto.client.return_value.generate_embed_url_for_anonymous_user
        )
        self.mock_generate.return_value = {"Status": 201, "EmbedUrl": "https://embed"}
        self.arns = dict(
            primary=[
                "arn:aws:quicksight:us-west-1:aws-account-id:dashboard/uuid1",
                "arn:aws:quicksight:us-west-1:aws-account-id:dashboard/uuid2",
            ],
        )
        self.controller = partial(
            QuickSightController, db.session, authorized_arns=self.arns
        )


@pytest.fixture
def quicksight_fixture(
    db: DatabaseTransactionFixture,
) -> Generator[QuickSightControllerFixture, None, None]:
    with mock.patch(
        "palace.manager.api.admin.controller.quicksight.boto3"
    ) as mock_boto:
        yield QuickSightControllerFixture(db, mock_boto)


class TestQuicksightController:
    def test_generate_quicksight_url(
        self,
        db: DatabaseTransactionFixture,
        quicksight_fixture: QuickSightControllerFixture,
        flask_app_fixture: FlaskAppFixture,
    ):
        system_admin, _ = create(db.session, Admin, email="admin@email.com")
        system_admin.add_role(AdminRole.SYSTEM_ADMIN)
        default = db.default_library()
        library1 = db.library()

        arns = dict(
            primary=[
                "arn:aws:quicksight:us-west-1:aws-account-id:dashboard/uuid1",
                "arn:aws:quicksight:us-west-1:aws-account-id:dashboard/uuid2",
            ],
            secondary=[
                "arn:aws:quicksight:us-west-1:aws-account-id:dashboard/uuid2",
                "arn:aws:quicksight:us-west-1:aws-account-id:dashboard/uuid1",
            ],
        )

        ctrl = quicksight_fixture.controller(authorized_arns=arns)
        random_uuid = str(uuid.uuid4())

        with flask_app_fixture.test_request_context(
            f"/?library_uuids={default.uuid},{library1.uuid},{random_uuid}",
            admin=system_admin,
        ):
            response = ctrl.generate_quicksight_url("primary")

        # Assert the right client was created, with a region
        assert quicksight_fixture.mock_boto.client.call_args == mock.call(
            "quicksight", region_name="us-west-1"
        )
        # Assert the request and response formats
        assert response["embedUrl"] == "https://embed"
        assert quicksight_fixture.mock_generate.call_args == mock.call(
            AwsAccountId="aws-account-id",
            Namespace="default",
            AuthorizedResourceArns=arns["primary"],
            ExperienceConfiguration={"Dashboard": {"InitialDashboardId": "uuid1"}},
            SessionTags=[
                dict(
                    Key="library_short_name_0",
                    Value="|".join([str(library1.short_name), str(default.short_name)]),
                )
            ],
        )

        # Specific library roles
        admin1, _ = create(db.session, Admin, email="admin1@email.com")
        admin1.add_role(AdminRole.LIBRARY_MANAGER, library1)

        with flask_app_fixture.test_request_context(
            f"/?library_uuids={default.uuid},{library1.uuid}",
            admin=admin1,
        ):
            quicksight_fixture.mock_generate.reset_mock()
            ctrl.generate_quicksight_url("secondary")

        assert quicksight_fixture.mock_generate.call_args == mock.call(
            AwsAccountId="aws-account-id",
            Namespace="default",
            AuthorizedResourceArns=arns["secondary"],
            ExperienceConfiguration={"Dashboard": {"InitialDashboardId": "uuid2"}},
            SessionTags=[
                dict(
                    Key="library_short_name_0",
                    Value="|".join([str(library1.short_name)]),
                )
            ],
        )

    def test_generate_quicksight_url_with_a_large_number_of_libraries(
        self,
        db: DatabaseTransactionFixture,
        quicksight_fixture: QuickSightControllerFixture,
        flask_app_fixture: FlaskAppFixture,
    ):
        system_admin, _ = create(db.session, Admin, email="admin@email.com")
        system_admin.add_role(AdminRole.SYSTEM_ADMIN)
        db.default_library()
        ctrl = quicksight_fixture.controller()

        libraries: list[Library] = []
        for x in range(0, 37):
            libraries.append(db.library(short_name="TL" + str(x).zfill(4)))

        with flask_app_fixture.test_request_context(
            f"/?library_uuids={','.join(cast(list[str], [x.uuid for x in libraries ]))}",
            admin=system_admin,
        ):
            ctrl.generate_quicksight_url("primary")

        assert quicksight_fixture.mock_generate.call_args == mock.call(
            AwsAccountId="aws-account-id",
            Namespace="default",
            AuthorizedResourceArns=quicksight_fixture.arns["primary"],
            ExperienceConfiguration={"Dashboard": {"InitialDashboardId": "uuid1"}},
            SessionTags=[
                dict(
                    Key="library_short_name_0",
                    Value="|".join(
                        cast(list[str], [x.short_name for x in libraries[0:36]])
                    ),
                ),
                dict(
                    Key="library_short_name_1",
                    Value="|".join(
                        cast(list[str], [x.short_name for x in libraries[36:37]])
                    ),
                ),
            ],
        )

    def test_generate_quicksight_url_errors(
        self,
        db: DatabaseTransactionFixture,
        quicksight_fixture: QuickSightControllerFixture,
        flask_app_fixture: FlaskAppFixture,
    ):
        library = db.library()
        library_not_allowed = db.library()
        admin, _ = create(db.session, Admin, email="admin@email.com")
        admin.add_role(AdminRole.LIBRARY_MANAGER, library=library)

        ctrl = quicksight_fixture.controller(authorized_arns=None)
        with flask_app_fixture.test_request_context(
            f"/?library_uuids={library.uuid}",
            admin=admin,
        ):
            with pytest.raises(ProblemDetailException) as raised:
                ctrl.generate_quicksight_url("primary")
            assert (
                raised.value.problem_detail.detail
                == "Quicksight has not been configured for this server."
            )

        ctrl = quicksight_fixture.controller()
        with flask_app_fixture.test_request_context(
            f"/?library_uuids={library.uuid}",
            admin=admin,
        ):
            with pytest.raises(ProblemDetailException) as raised:
                ctrl.generate_quicksight_url("secondary")
            assert (
                raised.value.problem_detail.detail
                == "The requested Dashboard ARN is not recognized by this server."
            )

        with flask_app_fixture.test_request_context(
            f"/?library_uuids={library_not_allowed.uuid}",
            admin=admin,
        ):
            with pytest.raises(ProblemDetailException) as raised:
                ctrl.generate_quicksight_url("primary")
            assert (
                raised.value.problem_detail.detail
                == "No library was found for this Admin that matched the request."
            )

    @pytest.mark.parametrize(
        "generate_response",
        [
            pytest.param(dict(Status=400, ErrorMsg="Bad Request"), id="400"),
            pytest.param(dict(Status=200), id="Missing EmbedUrl"),
            pytest.param(dict(EmbedUrl="http://embed"), id="Missing Status"),
            pytest.param(Exception("Boto error"), id="Boto exception"),
        ],
    )
    def test_generate_quicksight_url_boto_errors(
        self,
        db: DatabaseTransactionFixture,
        quicksight_fixture: QuickSightControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        generate_response: dict[str, Any] | Exception,
    ):
        library = db.library()
        ctrl = quicksight_fixture.controller()
        with flask_app_fixture.test_request_context_system_admin(
            f"/?library_uuids={library.uuid}",
        ):
            if isinstance(generate_response, Exception):
                quicksight_fixture.mock_generate.side_effect = generate_response
            else:
                quicksight_fixture.mock_generate.return_value = generate_response
            with pytest.raises(ProblemDetailException) as raised:
                ctrl.generate_quicksight_url("primary")
            assert (
                raised.value.problem_detail.detail
                == "Error while fetching the Quicksight Embed url."
            )

    def test_generate_quicksight_url_without_library_uuids_param(
        self,
        db: DatabaseTransactionFixture,
        quicksight_fixture: QuickSightControllerFixture,
        flask_app_fixture: FlaskAppFixture,
    ):
        """Test that when library_uuids is not provided, all allowed libraries are used."""
        library = db.library()
        admin, _ = create(db.session, Admin, email="admin@email.com")
        admin.add_role(AdminRole.LIBRARY_MANAGER, library=library)

        ctrl = quicksight_fixture.controller()
        with flask_app_fixture.test_request_context(
            "/",
            admin=admin,
        ):
            response = ctrl.generate_quicksight_url("primary")

        # Assert the right client was created, with a region
        assert quicksight_fixture.mock_boto.client.call_args == mock.call(
            "quicksight", region_name="us-west-1"
        )
        # Assert the request and response formats
        assert response["embedUrl"] == "https://embed"
        # Assert that the one allowable library was used
        assert quicksight_fixture.mock_generate.call_args == mock.call(
            AwsAccountId="aws-account-id",
            Namespace="default",
            AuthorizedResourceArns=quicksight_fixture.arns["primary"],
            ExperienceConfiguration={"Dashboard": {"InitialDashboardId": "uuid1"}},
            SessionTags=[
                dict(
                    Key="library_short_name_0",
                    Value=str(library.short_name),
                )
            ],
        )

    def test_generate_quicksight_url_empty_library_uuids_param(
        self,
        db: DatabaseTransactionFixture,
        quicksight_fixture: QuickSightControllerFixture,
        flask_app_fixture: FlaskAppFixture,
    ):
        """Test that when an empty library_uuids is not provided a value error occurs"""
        library = db.library()
        admin, _ = create(db.session, Admin, email="admin@email.com")
        admin.add_role(AdminRole.LIBRARY_MANAGER, library=library)

        ctrl = quicksight_fixture.controller()

        with pytest.raises(ValueError):
            with flask_app_fixture.test_request_context(
                "/?library_uuids=",
                admin=admin,
            ):
                ctrl.generate_quicksight_url("primary")

    def test_get_dashboard_names(self, quicksight_fixture: QuickSightControllerFixture):
        ctrl = quicksight_fixture.controller(
            authorized_arns=dict(primary=[], secondary=[], tertiary=[])
        )
        assert ctrl.get_dashboard_names() == {
            "names": ["primary", "secondary", "tertiary"]
        }
