import uuid
from typing import cast
from unittest import mock

import pytest

from core.model import Library, create
from core.model.admin import Admin, AdminRole
from core.util.problem_detail import ProblemError
from tests.fixtures.api_admin import AdminControllerFixture
from tests.fixtures.api_controller import ControllerFixture


class QuickSightControllerFixture(AdminControllerFixture):
    def __init__(self, controller_fixture: ControllerFixture):
        super().__init__(controller_fixture)


@pytest.fixture
def quicksight_fixture(
    controller_fixture: ControllerFixture,
) -> QuickSightControllerFixture:
    return QuickSightControllerFixture(controller_fixture)


class TestQuicksightController:
    def test_generate_quicksight_url(
        self, quicksight_fixture: QuickSightControllerFixture
    ):
        ctrl = quicksight_fixture.manager.admin_quicksight_controller
        db = quicksight_fixture.ctrl.db

        system_admin, _ = create(db.session, Admin, email="admin@email.com")
        system_admin.add_role(AdminRole.SYSTEM_ADMIN)
        default = db.default_library()
        library1 = db.library()

        with mock.patch(
            "api.admin.controller.quicksight.boto3"
        ) as mock_boto, mock.patch(
            "api.admin.controller.quicksight.Configuration.quicksight_authorized_arns"
        ) as mock_qs_arns:
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
            mock_qs_arns.return_value = arns
            generate_method: mock.MagicMock = (
                mock_boto.client().generate_embed_url_for_anonymous_user
            )
            generate_method.return_value = {"Status": 201, "EmbedUrl": "https://embed"}

            random_uuid = str(uuid.uuid4())
            with quicksight_fixture.request_context_with_admin(
                f"/?library_uuids={default.uuid},{library1.uuid},{random_uuid}",
                admin=system_admin,
            ) as ctx:
                response = ctrl.generate_quicksight_url("primary")

                # Assert the right client was created, with a region
                assert mock_boto.client.call_args == mock.call(
                    "quicksight", region_name="us-west-1"
                )
                # Assert the reqest and response formats
                assert response["embedUrl"] == "https://embed"
                assert generate_method.call_args == mock.call(
                    AwsAccountId="aws-account-id",
                    Namespace="default",
                    AuthorizedResourceArns=arns["primary"],
                    ExperienceConfiguration={
                        "Dashboard": {"InitialDashboardId": "uuid1"}
                    },
                    SessionTags=[
                        dict(
                            Key="library_short_name_0",
                            Value="|".join(
                                [str(library1.short_name), str(default.short_name)]
                            ),
                        )
                    ],
                )

            # Specific library roles
            admin1, _ = create(db.session, Admin, email="admin1@email.com")
            admin1.add_role(AdminRole.LIBRARY_MANAGER, library1)

            with quicksight_fixture.request_context_with_admin(
                f"/?library_uuids={default.uuid},{library1.uuid}",
                admin=admin1,
            ) as ctx:
                generate_method.reset_mock()
                ctrl.generate_quicksight_url("secondary")

                assert generate_method.call_args == mock.call(
                    AwsAccountId="aws-account-id",
                    Namespace="default",
                    AuthorizedResourceArns=arns["secondary"],
                    ExperienceConfiguration={
                        "Dashboard": {"InitialDashboardId": "uuid2"}
                    },
                    SessionTags=[
                        dict(
                            Key="library_short_name_0",
                            Value="|".join([str(library1.short_name)]),
                        )
                    ],
                )

    def test_generate_quicksight_url_with_a_large_number_of_libraries(
        self, quicksight_fixture: QuickSightControllerFixture
    ):
        ctrl = quicksight_fixture.manager.admin_quicksight_controller
        db = quicksight_fixture.ctrl.db

        system_admin, _ = create(db.session, Admin, email="admin@email.com")
        system_admin.add_role(AdminRole.SYSTEM_ADMIN)
        default = db.default_library()

        libraries: list[Library] = []
        for x in range(0, 37):
            libraries.append(db.library(short_name="TL" + str(x).zfill(4)))

        with mock.patch(
            "api.admin.controller.quicksight.boto3"
        ) as mock_boto, mock.patch(
            "api.admin.controller.quicksight.Configuration.quicksight_authorized_arns"
        ) as mock_qs_arns:
            arns = dict(
                primary=[
                    "arn:aws:quicksight:us-west-1:aws-account-id:dashboard/uuid1",
                    "arn:aws:quicksight:us-west-1:aws-account-id:dashboard/uuid2",
                ],
            )
            mock_qs_arns.return_value = arns
            generate_method: mock.MagicMock = (
                mock_boto.client().generate_embed_url_for_anonymous_user
            )
            generate_method.return_value = {"Status": 201, "EmbedUrl": "https://embed"}

            random_uuid = str(uuid.uuid4())
            with quicksight_fixture.request_context_with_admin(
                f"/?library_uuids={','.join(cast(list[str], [x.uuid for x in libraries ]))}",
                admin=system_admin,
            ) as ctx:
                response = ctrl.generate_quicksight_url("primary")

                # Assert the right client was created, with a region
                assert mock_boto.client.call_args == mock.call(
                    "quicksight", region_name="us-west-1"
                )
                # Assert the reqest and response formats
                assert response["embedUrl"] == "https://embed"
                assert generate_method.call_args == mock.call(
                    AwsAccountId="aws-account-id",
                    Namespace="default",
                    AuthorizedResourceArns=arns["primary"],
                    ExperienceConfiguration={
                        "Dashboard": {"InitialDashboardId": "uuid1"}
                    },
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
                                cast(
                                    list[str], [x.short_name for x in libraries[36:37]]
                                )
                            ),
                        ),
                    ],
                )

    def test_generate_quicksight_url_errors(
        self, quicksight_fixture: QuickSightControllerFixture
    ):
        ctrl = quicksight_fixture.manager.admin_quicksight_controller
        db = quicksight_fixture.ctrl.db

        library = db.library()
        library_not_allowed = db.library()
        admin, _ = create(db.session, Admin, email="admin@email.com")
        admin.add_role(AdminRole.LIBRARY_MANAGER, library=library)

        with mock.patch(
            "api.admin.controller.quicksight.boto3"
        ) as mock_boto, mock.patch(
            "api.admin.controller.quicksight.Configuration.quicksight_authorized_arns"
        ) as mock_qs_arns:
            arns = dict(
                primary=[
                    "arn:aws:quicksight:us-west-1:aws-account-id:dashboard/uuid1",
                    "arn:aws:quicksight:us-west-1:aws-account-id:dashboard/uuid2",
                ]
            )
            mock_qs_arns.return_value = arns

            with quicksight_fixture.request_context_with_admin(
                f"/?library_uuids={library.uuid}",
                admin=admin,
            ) as ctx:
                with pytest.raises(ProblemError) as raised:
                    ctrl.generate_quicksight_url("secondary")
                assert (
                    raised.value.problem_detail.detail
                    == "The requested Dashboard ARN is not recognized by this server."
                )

                mock_qs_arns.return_value = []
                with pytest.raises(ProblemError) as raised:
                    ctrl.generate_quicksight_url("primary")
                assert (
                    raised.value.problem_detail.detail
                    == "Quicksight has not been configured for this server."
                )

            with quicksight_fixture.request_context_with_admin(
                f"/?library_uuids={library_not_allowed.uuid}",
                admin=admin,
            ) as ctx:
                mock_qs_arns.return_value = arns
                with pytest.raises(ProblemError) as raised:
                    ctrl.generate_quicksight_url("primary")
                assert (
                    raised.value.problem_detail.detail
                    == "No library was found for this Admin that matched the request."
                )

            with quicksight_fixture.request_context_with_admin(
                f"/?library_uuids={library.uuid}",
                admin=admin,
            ) as ctx:
                # Bad response from boto
                mock_boto.generate_embed_url_for_anonymous_user.return_value = dict(
                    status=400, embed_url="http://embed"
                )
                with pytest.raises(ProblemError) as raised:
                    ctrl.generate_quicksight_url("primary")
                assert (
                    raised.value.problem_detail.detail
                    == "Error while fetching the Quicksight Embed url."
                )

                # 200 status, but no url
                mock_boto.generate_embed_url_for_anonymous_user.return_value = dict(
                    status=200,
                )
                with pytest.raises(ProblemError) as raised:
                    ctrl.generate_quicksight_url("primary")
                assert (
                    raised.value.problem_detail.detail
                    == "Error while fetching the Quicksight Embed url."
                )

                # Boto threw an error
                mock_boto.generate_embed_url_for_anonymous_user.side_effect = Exception(
                    ""
                )
                with pytest.raises(ProblemError) as raised:
                    ctrl.generate_quicksight_url("primary")
                assert (
                    raised.value.problem_detail.detail
                    == "Error while fetching the Quicksight Embed url."
                )

    def test_get_dashboard_names(self, quicksight_fixture: QuickSightControllerFixture):
        with mock.patch(
            "api.admin.controller.quicksight.Configuration.quicksight_authorized_arns"
        ) as mock_qs_arns:
            mock_qs_arns.return_value = dict(primary=[], secondary=[], tertiary=[])
            ctrl = quicksight_fixture.manager.admin_quicksight_controller
            assert ctrl.get_dashboard_names() == {
                "names": ["primary", "secondary", "tertiary"]
            }
