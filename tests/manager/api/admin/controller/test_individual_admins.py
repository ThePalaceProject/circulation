import json
from contextlib import nullcontext

import flask
import pytest
from werkzeug.datastructures import ImmutableMultiDict

from palace.manager.api.admin.controller.individual_admin_settings import (
    IndividualAdminSettingsController,
)
from palace.manager.api.admin.exceptions import AdminNotAuthorized
from palace.manager.api.admin.problem_details import (
    ADMIN_AUTH_NOT_CONFIGURED,
    INCOMPLETE_CONFIGURATION,
    INVALID_EMAIL,
    UNKNOWN_ROLE,
)
from palace.manager.api.problem_details import LIBRARY_NOT_FOUND
from palace.manager.sqlalchemy.model.admin import Admin, AdminRole
from palace.manager.sqlalchemy.util import create, get_one
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.flask import FlaskAppFixture


class IndividualAdminControllerFixture:
    def __init__(self, db: DatabaseTransactionFixture) -> None:
        self.controller = IndividualAdminSettingsController(db.session)

        self.l1 = db.library(short_name="l1")
        self.system, ignore = create(db.session, Admin, email="system@example.com")
        self.system.add_role(AdminRole.SYSTEM_ADMIN)
        assert self.system.is_system_admin()

        self.sitewide_manager, ignore = create(
            db.session, Admin, email="sitewide_manager@example.com"
        )
        self.sitewide_manager.add_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
        assert self.sitewide_manager.is_sitewide_library_manager()

        self.sitewide_librarian, ignore = create(
            db.session, Admin, email="sitewide_librarian@example.com"
        )
        self.sitewide_librarian.add_role(AdminRole.SITEWIDE_LIBRARIAN)
        assert self.sitewide_manager.is_sitewide_librarian()

        self.manager1, ignore = create(
            db.session, Admin, email="library_manager_l1@example.com"
        )
        self.manager1.add_role(AdminRole.LIBRARY_MANAGER, self.l1)
        assert self.manager1.is_library_manager(self.l1)

        self.librarian1, ignore = create(
            db.session, Admin, email="librarian_l1@example.com"
        )
        self.librarian1.add_role(AdminRole.LIBRARIAN, self.l1)
        assert self.librarian1.is_librarian(self.l1)

        self.l2 = db.library(short_name="l2")
        self.manager2, ignore = create(
            db.session, Admin, email="library_manager_l2@example.com"
        )
        self.manager2.add_role(AdminRole.LIBRARY_MANAGER, self.l2)
        assert self.manager2.is_library_manager(self.l2)

        self.librarian2, ignore = create(
            db.session, Admin, email="librarian_l2@example.com"
        )
        self.librarian2.add_role(AdminRole.LIBRARIAN, self.l2)
        assert self.librarian2.is_librarian(self.l2)

        self.manager1_2, ignore = create(
            db.session, Admin, email="library_manager_l1_l2@example.com"
        )
        self.manager1_2.add_role(AdminRole.LIBRARY_MANAGER, self.l1)
        self.manager1_2.add_role(AdminRole.LIBRARY_MANAGER, self.l2)


@pytest.fixture
def controller_fixture(
    db: DatabaseTransactionFixture,
) -> IndividualAdminControllerFixture:
    return IndividualAdminControllerFixture(db)


class TestIndividualAdmins:
    def test_individual_admins_get(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller_fixture: IndividualAdminControllerFixture,
    ):
        for admin in db.session.query(Admin):
            db.session.delete(admin)

        # There are two admins that can sign in with passwords, with different roles.
        admin1, ignore = create(db.session, Admin, email="admin1@nypl.org")
        admin1.password = "pass1"
        admin1.add_role(AdminRole.SYSTEM_ADMIN)
        admin2, ignore = create(db.session, Admin, email="admin2@nypl.org")
        admin2.password = "pass2"
        admin2.add_role(AdminRole.LIBRARY_MANAGER, db.default_library())
        admin2.add_role(AdminRole.SITEWIDE_LIBRARIAN)

        # These admins don't have passwords.
        admin3, ignore = create(db.session, Admin, email="admin3@nypl.org")
        admin3.add_role(AdminRole.LIBRARIAN, db.default_library())
        library2 = db.library()
        admin4, ignore = create(db.session, Admin, email="admin4@l2.org")
        admin4.add_role(AdminRole.LIBRARY_MANAGER, library2)
        admin5, ignore = create(db.session, Admin, email="admin5@l2.org")
        admin5.add_role(AdminRole.LIBRARIAN, library2)

        admin6, ignore = create(db.session, Admin, email="admin6@l2.org")
        admin6.add_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)

        with flask_app_fixture.test_request_context("/", admin=admin1):
            # A system admin can see all other admins' roles.
            response = controller_fixture.controller.process_get()
            admins = response.get("individualAdmins", [])

            expected = {
                "admin1@nypl.org": [{"role": AdminRole.SYSTEM_ADMIN}],
                "admin2@nypl.org": [
                    {
                        "role": AdminRole.LIBRARY_MANAGER,
                        "library": str(db.default_library().short_name),
                    },
                    {"role": AdminRole.SITEWIDE_LIBRARIAN},
                ],
                "admin3@nypl.org": [
                    {
                        "role": AdminRole.LIBRARIAN,
                        "library": str(db.default_library().short_name),
                    }
                ],
                "admin4@l2.org": [
                    {
                        "role": AdminRole.LIBRARY_MANAGER,
                        "library": str(library2.short_name),
                    }
                ],
                "admin5@l2.org": [
                    {
                        "role": AdminRole.LIBRARIAN,
                        "library": str(library2.short_name),
                    }
                ],
                "admin6@l2.org": [
                    {
                        "role": AdminRole.SITEWIDE_LIBRARY_MANAGER,
                    }
                ],
            }

            assert len(admins) == len(expected)
            for admin in admins:
                assert admin["email"] in expected
                assert sorted(admin["roles"], key=lambda x: x["role"]) == sorted(
                    expected[admin["email"]], key=lambda x: x["role"]
                )

        with flask_app_fixture.test_request_context("/", admin=admin2):
            # A sitewide librarian or library manager can also see all admins' roles.
            response = controller_fixture.controller.process_get()
            admins = response.get("individualAdmins")
            expected_admins: list[dict[str, str | list[dict[str, str]]]] = [
                {
                    "email": "admin2@nypl.org",
                    "roles": [
                        {
                            "role": AdminRole.LIBRARY_MANAGER,
                            "library": str(db.default_library().short_name),
                        },
                        {"role": AdminRole.SITEWIDE_LIBRARIAN},
                    ],
                },
                {
                    "email": "admin3@nypl.org",
                    "roles": [
                        {
                            "role": AdminRole.LIBRARIAN,
                            "library": str(db.default_library().short_name),
                        }
                    ],
                },
                {
                    "email": "admin6@l2.org",
                    "roles": [
                        {
                            "role": AdminRole.SITEWIDE_LIBRARY_MANAGER,
                        }
                    ],
                },
            ]
            assert sorted(
                expected_admins,
                key=lambda x: x["email"],
            ) == sorted(admins, key=lambda x: x["email"])

        with flask_app_fixture.test_request_context("/", admin=admin3):
            # A librarian cannot view this API anymore
            pytest.raises(
                AdminNotAuthorized,
                controller_fixture.controller.process_get,
            )

        with flask_app_fixture.test_request_context("/", admin=admin4):
            response = controller_fixture.controller.process_get()
            admins = response.get("individualAdmins")
            expected_admins = [
                {
                    "email": "admin2@nypl.org",
                    "roles": [{"role": AdminRole.SITEWIDE_LIBRARIAN}],
                },
                {
                    "email": "admin4@l2.org",
                    "roles": [
                        {
                            "role": AdminRole.LIBRARY_MANAGER,
                            "library": str(library2.short_name),
                        }
                    ],
                },
                {
                    "email": "admin5@l2.org",
                    "roles": [
                        {
                            "role": AdminRole.LIBRARIAN,
                            "library": str(library2.short_name),
                        }
                    ],
                },
                {
                    "email": "admin6@l2.org",
                    "roles": [
                        {
                            "role": AdminRole.SITEWIDE_LIBRARY_MANAGER,
                        }
                    ],
                },
            ]
            assert sorted(
                expected_admins,
                key=lambda x: x["email"],
            ) == sorted(admins, key=lambda x: x["email"])

        with flask_app_fixture.test_request_context("/", admin=admin5):
            pytest.raises(
                AdminNotAuthorized,
                controller_fixture.controller.process_get,
            )

        with flask_app_fixture.test_request_context("/", admin=admin6):
            response = controller_fixture.controller.process_get()
            admins = response.get("individualAdmins")
            expected_admins = [
                {
                    "email": "admin2@nypl.org",
                    "roles": [
                        {
                            "role": AdminRole.LIBRARY_MANAGER,
                            "library": str(db.default_library().short_name),
                        },
                        {"role": AdminRole.SITEWIDE_LIBRARIAN},
                    ],
                },
                {
                    "email": "admin3@nypl.org",
                    "roles": [
                        {
                            "role": AdminRole.LIBRARIAN,
                            "library": str(db.default_library().short_name),
                        }
                    ],
                },
                {
                    "email": "admin4@l2.org",
                    "roles": [
                        {
                            "role": AdminRole.LIBRARY_MANAGER,
                            "library": str(library2.short_name),
                        }
                    ],
                },
                {
                    "email": "admin5@l2.org",
                    "roles": [
                        {
                            "role": AdminRole.LIBRARIAN,
                            "library": str(library2.short_name),
                        }
                    ],
                },
                {
                    "email": "admin6@l2.org",
                    "roles": [
                        {
                            "role": AdminRole.SITEWIDE_LIBRARY_MANAGER,
                        }
                    ],
                },
            ]
            assert sorted(
                expected_admins,
                key=lambda x: x["email"],
            ) == sorted(admins, key=lambda x: x["email"])

    def test_individual_admins_get_no_admin(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller_fixture: IndividualAdminControllerFixture,
    ):
        # When the application is first started, there is no admin user. In that
        # case, we return a problem detail.

        with flask_app_fixture.test_request_context("/", method="GET"):
            response = controller_fixture.controller.process_get()
            assert response == ADMIN_AUTH_NOT_CONFIGURED

    def test_individual_admins_post_error_incomplete_config(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller_fixture: IndividualAdminControllerFixture,
    ):
        with flask_app_fixture.test_request_context(
            "/", method="POST", admin=controller_fixture.system
        ):
            flask.request.form = ImmutableMultiDict([])
            response = controller_fixture.controller.process_post()
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

    def test_individual_admins_post_error_library_not_found(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller_fixture: IndividualAdminControllerFixture,
    ):
        with flask_app_fixture.test_request_context(
            "/", method="POST", admin=controller_fixture.system
        ):
            flask.request.form = ImmutableMultiDict(
                [
                    ("email", "test@library.org"),
                    ("password", "334df3f70bfe1979"),
                    (
                        "roles",
                        json.dumps(
                            [{"role": AdminRole.LIBRARIAN, "library": "notalibrary"}]
                        ),
                    ),
                ]
            )
            response = controller_fixture.controller.process_post()
            assert response.uri == LIBRARY_NOT_FOUND.uri

    def test_individual_admins_post_error_invalid_email(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller_fixture: IndividualAdminControllerFixture,
    ):
        with flask_app_fixture.test_request_context(
            "/", method="POST", admin=controller_fixture.system
        ):
            flask.request.form = ImmutableMultiDict(
                [
                    ("email", "not-a-email"),
                    ("password", "334df3f70bfe1979"),
                ]
            )
            response = controller_fixture.controller.process_post()
            assert response.uri == INVALID_EMAIL.uri
            assert '"not-a-email" is not a valid email address' in response.detail

    def test_individual_admins_post_error_unknown_role(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller_fixture: IndividualAdminControllerFixture,
    ):
        with flask_app_fixture.test_request_context(
            "/", method="POST", admin=controller_fixture.system
        ):
            flask.request.form = ImmutableMultiDict(
                [
                    ("email", "test@library.org"),
                    ("password", "334df3f70bfe1979"),
                    (
                        "roles",
                        json.dumps(
                            [
                                {
                                    "role": "notarole",
                                    "library": controller_fixture.l1.short_name,
                                }
                            ]
                        ),
                    ),
                ]
            )
            response = controller_fixture.controller.process_post()
            assert response.uri == UNKNOWN_ROLE.uri

    @pytest.mark.parametrize(
        "admin_making_request_str,target_admin_str,roles,allowed",
        [
            # Various types of user trying to change a system admin's roles
            ("system", "system", [], True),
            ("sitewide_manager", "system", [], False),
            ("sitewide_librarian", "system", [], False),
            ("manager1", "system", [], False),
            ("librarian1", "system", [], False),
            ("manager2", "system", [], False),
            ("librarian2", "system", [], False),
            # Various types of user trying to change a sitewide manager's roles
            ("system", "sitewide_manager", [], True),
            ("sitewide_manager", "sitewide_manager", [], True),
            ("sitewide_librarian", "sitewide_manager", [], False),
            ("manager1", "sitewide_manager", [], False),
            ("librarian1", "sitewide_manager", [], False),
            ("manager2", "sitewide_manager", [], False),
            ("librarian2", "sitewide_manager", [], False),
            # Various types of user trying to change a sitewide librarian's roles
            ("system", "sitewide_librarian", [], True),
            ("sitewide_manager", "sitewide_librarian", [], True),
            ("sitewide_librarian", "sitewide_librarian", [], False),
            ("manager1", "sitewide_librarian", [], False),
            ("librarian1", "sitewide_librarian", [], False),
            ("manager2", "sitewide_librarian", [], False),
            ("librarian2", "sitewide_librarian", [], False),
            # Various other role changing tests
            ("manager1", "manager1", [], True),
            (
                "manager1",
                "sitewide_librarian",
                [
                    {"role": AdminRole.SITEWIDE_LIBRARIAN},
                    {"role": AdminRole.LIBRARY_MANAGER, "library": "l1"},
                ],
                True,
            ),
            ("manager1", "librarian1", [], True),
            (
                "manager2",
                "librarian2",
                [{"role": AdminRole.LIBRARIAN, "library": "l1"}],
                False,
            ),
            (
                "manager2",
                "librarian1",
                [{"role": AdminRole.LIBRARY_MANAGER, "library": "l1"}],
                False,
            ),
            ("sitewide_librarian", "librarian1", [], False),
            (
                "sitewide_manager",
                "sitewide_manager",
                [{"role": AdminRole.SYSTEM_ADMIN}],
                False,
            ),
            (
                "sitewide_librarian",
                "manager1",
                [{"role": AdminRole.SITEWIDE_LIBRARY_MANAGER}],
                False,
            ),
        ],
    )
    def test_individual_admins_post_permissions_changing_roles(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller_fixture: IndividualAdminControllerFixture,
        admin_making_request_str: str,
        target_admin_str: str,
        roles: list[dict[str, str]],
        allowed: bool,
    ) -> None:
        admin_making_request = getattr(controller_fixture, admin_making_request_str)
        target_admin = getattr(controller_fixture, target_admin_str)

        with flask_app_fixture.test_request_context(
            "/", method="POST", admin=admin_making_request
        ):
            flask.request.form = ImmutableMultiDict(
                [
                    ("email", target_admin.email),
                    ("roles", json.dumps(roles)),
                ]
            )
            context_manager = (
                nullcontext() if allowed else pytest.raises(AdminNotAuthorized)
            )
            with context_manager:
                controller_fixture.controller.process_post()

    @pytest.mark.parametrize(
        "admin_making_request_str,target_admin_str,allowed",
        [
            # Various types of user trying to change a system admin's password
            ("system", "system", True),
            ("sitewide_manager", "system", False),
            ("sitewide_librarian", "system", False),
            ("manager1", "system", False),
            ("librarian1", "system", False),
            ("manager2", "system", False),
            ("librarian2", "system", False),
            # Various types of user trying to change a sitewide manager's password
            ("system", "sitewide_manager", True),
            ("sitewide_manager", "sitewide_manager", True),
            ("sitewide_librarian", "sitewide_manager", False),
            ("manager1", "sitewide_manager", False),
            ("librarian1", "sitewide_manager", False),
            ("manager2", "sitewide_manager", False),
            ("librarian2", "sitewide_manager", False),
            # Various types of user trying to change a sitewide librarian's password
            ("system", "sitewide_librarian", True),
            ("sitewide_manager", "sitewide_librarian", True),
            ("manager1", "sitewide_librarian", False),
            ("manager2", "sitewide_librarian", False),
            ("sitewide_librarian", "sitewide_librarian", False),
            ("librarian1", "sitewide_librarian", False),
            ("librarian2", "sitewide_librarian", False),
            # Various types of user trying to change a manager's password
            # Manager 1
            ("system", "manager1", True),
            ("sitewide_manager", "manager1", True),
            ("manager1", "manager1", True),
            ("sitewide_librarian", "manager1", False),
            ("manager2", "manager1", False),
            ("librarian2", "manager1", False),
            # Manager 2
            ("system", "manager2", True),
            ("sitewide_manager", "manager2", True),
            ("manager2", "manager2", True),
            ("sitewide_librarian", "manager2", False),
            ("manager1", "manager2", False),
            ("librarian1", "manager2", False),
            # Various types of user trying to change a librarian's password
            # Librarian 1
            ("system", "librarian1", True),
            ("sitewide_manager", "librarian1", True),
            ("manager1", "librarian1", True),
            ("sitewide_librarian", "librarian1", False),
            ("manager2", "librarian1", False),
            ("librarian2", "librarian1", False),
            # Librarian 2
            ("system", "librarian2", True),
            ("sitewide_manager", "librarian2", True),
            ("manager2", "librarian2", True),
            ("sitewide_librarian", "librarian2", False),
            ("manager1", "librarian2", False),
            ("librarian1", "librarian2", False),
            # A manager of library1 should not be allowed for a manager of library1 and library2
            ("system", "manager1_2", True),
            ("sitewide_manager", "manager1_2", True),
            ("manager1_2", "manager1_2", True),
            ("sitewide_librarian", "manager1_2", False),
            ("manager1", "manager1_2", False),
            ("manager2", "manager1_2", False),
            ("librarian1", "manager1_2", False),
            # A manager of both libraries should be able to change the passwords of both
            ("manager1_2", "manager1", True),
            ("manager1_2", "manager2", True),
            ("manager1_2", "librarian1", True),
            ("manager1_2", "librarian2", True),
            ("manager1_2", "system", False),
            ("manager1_2", "sitewide_manager", False),
            ("manager1_2", "sitewide_librarian", False),
        ],
    )
    def test_individual_admins_post_permissions_changing_password(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller_fixture: IndividualAdminControllerFixture,
        admin_making_request_str: str,
        target_admin_str: str,
        allowed: bool,
    ) -> None:
        admin_making_request = getattr(controller_fixture, admin_making_request_str)
        target_admin = getattr(controller_fixture, target_admin_str)

        with flask_app_fixture.test_request_context(
            "/", method="POST", admin=admin_making_request
        ):
            flask.request.form = ImmutableMultiDict(
                [
                    ("email", target_admin.email),
                    ("password", "new password"),
                    (
                        "roles",
                        json.dumps([role.to_dict() for role in target_admin.roles]),
                    ),
                ]
            )
            context_manager = (
                nullcontext() if allowed else pytest.raises(AdminNotAuthorized)
            )

            with context_manager:
                controller_fixture.controller.process_post()

    def test_individual_admins_post_create(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller_fixture: IndividualAdminControllerFixture,
    ):
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("email", "admin@nypl.org"),
                    ("password", "pass"),
                    (
                        "roles",
                        json.dumps(
                            [
                                {
                                    "role": AdminRole.LIBRARY_MANAGER,
                                    "library": db.default_library().short_name,
                                }
                            ]
                        ),
                    ),
                ]
            )
            response = controller_fixture.controller.process_post()
            assert response.status_code == 201

        # The admin was created.
        admin_match = Admin.authenticate(db.session, "admin@nypl.org", "pass")
        assert admin_match is not None
        assert admin_match.email == response.get_data(as_text=True)
        assert admin_match
        assert admin_match.has_password("pass")

        [role] = admin_match.roles
        assert AdminRole.LIBRARY_MANAGER == role.role
        assert db.default_library() == role.library

        # The new admin is a library manager, so they can create librarians.
        with flask_app_fixture.test_request_context(
            "/", method="POST", admin=admin_match
        ):
            flask.request.form = ImmutableMultiDict(
                [
                    ("email", "admin2@nypl.org"),
                    ("password", "pass"),
                    (
                        "roles",
                        json.dumps(
                            [
                                {
                                    "role": AdminRole.LIBRARIAN,
                                    "library": db.default_library().short_name,
                                }
                            ]
                        ),
                    ),
                ]
            )
            response = controller_fixture.controller.process_post()
            assert response.status_code == 201

        admin_match = Admin.authenticate(db.session, "admin2@nypl.org", "pass")
        assert admin_match is not None
        assert admin_match.email == response.get_data(as_text=True)
        assert admin_match
        assert admin_match.has_password("pass")

        [role] = admin_match.roles
        assert AdminRole.LIBRARIAN == role.role
        assert db.default_library() == role.library

    def test_individual_admins_post_edit(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller_fixture: IndividualAdminControllerFixture,
    ):
        # An admin exists.
        admin, ignore = create(
            db.session,
            Admin,
            email="admin@nypl.org",
        )
        admin.password = "password"
        admin.add_role(AdminRole.SYSTEM_ADMIN)

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("email", "admin@nypl.org"),
                    ("password", "new password"),
                    (
                        "roles",
                        json.dumps(
                            [
                                {"role": AdminRole.SITEWIDE_LIBRARIAN},
                                {
                                    "role": AdminRole.LIBRARY_MANAGER,
                                    "library": db.default_library().short_name,
                                },
                            ]
                        ),
                    ),
                ]
            )
            response = controller_fixture.controller.process_post()
            assert response.status_code == 200

        assert admin.email == response.get_data(as_text=True)

        # The password was changed.
        old_password_match = Admin.authenticate(
            db.session, "admin@nypl.org", "password"
        )
        assert None == old_password_match

        new_password_match = Admin.authenticate(
            db.session, "admin@nypl.org", "new password"
        )
        assert admin == new_password_match

        # The roles were changed.
        assert False == admin.is_system_admin()
        [librarian_all, manager] = sorted(admin.roles, key=lambda x: str(x.role))
        assert AdminRole.SITEWIDE_LIBRARIAN == librarian_all.role
        assert None == librarian_all.library
        assert AdminRole.LIBRARY_MANAGER == manager.role
        assert db.default_library() == manager.library

    def test_individual_admin_delete(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller_fixture: IndividualAdminControllerFixture,
    ):
        librarian, ignore = create(db.session, Admin, email=db.fresh_str())
        librarian.password = "password"
        librarian.add_role(AdminRole.LIBRARIAN, db.default_library())

        sitewide_manager, ignore = create(db.session, Admin, email=db.fresh_str())
        sitewide_manager.add_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)

        system_admin, ignore = create(db.session, Admin, email=db.fresh_str())
        system_admin.add_role(AdminRole.SYSTEM_ADMIN)

        with flask_app_fixture.test_request_context(
            "/", method="DELETE", admin=librarian
        ):
            pytest.raises(
                AdminNotAuthorized,
                controller_fixture.controller.process_delete,
                librarian.email,
            )

        with flask_app_fixture.test_request_context(
            "/", method="DELETE", admin=sitewide_manager
        ):
            response = controller_fixture.controller.process_delete(librarian.email)
            assert response.status_code == 200

            pytest.raises(
                AdminNotAuthorized,
                controller_fixture.controller.process_delete,
                system_admin.email,
            )

        with flask_app_fixture.test_request_context(
            "/", method="DELETE", admin=system_admin
        ):
            response = controller_fixture.controller.process_delete(system_admin.email)
            assert response.status_code == 200

        admin = get_one(db.session, Admin, id=librarian.id)
        assert None == admin

        admin = get_one(db.session, Admin, id=system_admin.id)
        assert None == admin

    def test_individual_admins_post_create_not_system(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller_fixture: IndividualAdminControllerFixture,
    ):
        """Creating an admin that's not a system admin will fail."""

        for admin in db.session.query(Admin):
            db.session.delete(admin)

        with flask_app_fixture.test_request_context("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("email", "first_admin@nypl.org"),
                    ("password", "pass"),
                    (
                        "roles",
                        json.dumps(
                            [
                                {
                                    "role": AdminRole.LIBRARY_MANAGER,
                                    "library": db.default_library().short_name,
                                }
                            ]
                        ),
                    ),
                ]
            )
            flask.request.files = ImmutableMultiDict()
            pytest.raises(
                AdminNotAuthorized,
                controller_fixture.controller.process_post,
            )

    def test_individual_admins_post_create_requires_password(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller_fixture: IndividualAdminControllerFixture,
    ):
        """The password is required."""

        for admin in db.session.query(Admin):
            db.session.delete(admin)

        with flask_app_fixture.test_request_context("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("email", "first_admin@nypl.org"),
                    ("roles", json.dumps([{"role": AdminRole.SYSTEM_ADMIN}])),
                ]
            )
            flask.request.files = ImmutableMultiDict()
            response = controller_fixture.controller.process_post()
            assert 400 == response.status_code
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

    def test_individual_admins_post_create_requires_non_empty_password(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller_fixture: IndividualAdminControllerFixture,
    ):
        """The password is required."""

        for admin in db.session.query(Admin):
            db.session.delete(admin)

        with flask_app_fixture.test_request_context("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("email", "first_admin@nypl.org"),
                    ("password", ""),
                    ("roles", json.dumps([{"role": AdminRole.SYSTEM_ADMIN}])),
                ]
            )
            flask.request.files = ImmutableMultiDict()
            response = controller_fixture.controller.process_post()
            assert 400 == response.status_code
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

    def test_individual_admins_post_create_on_setup(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller_fixture: IndividualAdminControllerFixture,
    ):
        """Creating a system admin with a password works."""

        for admin in db.session.query(Admin):
            db.session.delete(admin)

        with flask_app_fixture.test_request_context("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("email", "first_admin@nypl.org"),
                    ("password", "pass"),
                    ("roles", json.dumps([{"role": AdminRole.SYSTEM_ADMIN}])),
                ]
            )
            flask.request.files = ImmutableMultiDict()
            response = controller_fixture.controller.process_post()
            assert 201 == response.status_code

        # The admin was created.
        admin_match = Admin.authenticate(db.session, "first_admin@nypl.org", "pass")
        assert admin_match is not None
        assert admin_match.email == response.get_data(as_text=True)
        assert admin_match
        assert admin_match.has_password("pass")

        [role] = admin_match.roles
        assert AdminRole.SYSTEM_ADMIN == role.role

    def test_individual_admins_post_create_second_admin(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller_fixture: IndividualAdminControllerFixture,
    ):
        """Creating a second admin with a password works."""

        for admin in db.session.query(Admin):
            db.session.delete(admin)

        system_admin, ignore = create(db.session, Admin, email=db.fresh_str())
        system_admin.add_role(AdminRole.SYSTEM_ADMIN)

        with flask_app_fixture.test_request_context(
            "/", method="POST", admin=system_admin
        ):
            flask.request.form = ImmutableMultiDict(
                [
                    ("email", "second_admin@nypl.org"),
                    ("password", "pass"),
                    ("roles", json.dumps([])),
                ]
            )
            flask.request.files = ImmutableMultiDict()
            response = controller_fixture.controller.process_post()
            assert 201 == response.status_code

    def test_individual_admins_post_create_second_admin_no_roles(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller_fixture: IndividualAdminControllerFixture,
    ):
        """Creating a second admin with a password works."""

        for admin in db.session.query(Admin):
            db.session.delete(admin)

        system_admin, ignore = create(db.session, Admin, email=db.fresh_str())
        system_admin.add_role(AdminRole.SYSTEM_ADMIN)

        with flask_app_fixture.test_request_context(
            "/", method="POST", admin=system_admin
        ):
            flask.request.form = ImmutableMultiDict(
                [("email", "second_admin@nypl.org"), ("password", "pass")]
            )
            flask.request.files = ImmutableMultiDict()
            response = controller_fixture.controller.process_post()
            assert 201 == response.status_code

    def test_individual_admins_post_create_second_admin_no_password(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller_fixture: IndividualAdminControllerFixture,
    ):
        """Creating a second admin without a password fails."""

        for admin in db.session.query(Admin):
            db.session.delete(admin)

        system_admin, ignore = create(db.session, Admin, email=db.fresh_str())
        system_admin.add_role(AdminRole.SYSTEM_ADMIN)

        with flask_app_fixture.test_request_context(
            "/", method="POST", admin=system_admin
        ):
            flask.request.form = ImmutableMultiDict(
                [
                    ("email", "second_admin@nypl.org"),
                    ("roles", json.dumps([])),
                ]
            )
            flask.request.files = ImmutableMultiDict()
            response = controller_fixture.controller.process_post()
            assert 400 == response.status_code

    def test_individual_admins_post_create_second_admin_empty_password(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller_fixture: IndividualAdminControllerFixture,
    ):
        """Creating a second admin without a password fails."""

        for admin in db.session.query(Admin):
            db.session.delete(admin)

        system_admin, ignore = create(db.session, Admin, email=db.fresh_str())
        system_admin.add_role(AdminRole.SYSTEM_ADMIN)

        with flask_app_fixture.test_request_context(
            "/", method="POST", admin=system_admin
        ):
            flask.request.form = ImmutableMultiDict(
                [
                    ("email", "second_admin@nypl.org"),
                    ("password", ""),
                    ("roles", json.dumps([])),
                ]
            )
            flask.request.files = ImmutableMultiDict()
            response = controller_fixture.controller.process_post()
            assert 400 == response.status_code

    def test_individual_admins_post_create_second_admin_blank_password(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller_fixture: IndividualAdminControllerFixture,
    ):
        """Creating a second admin without a password fails."""

        for admin in db.session.query(Admin):
            db.session.delete(admin)

        system_admin, ignore = create(db.session, Admin, email=db.fresh_str())
        system_admin.add_role(AdminRole.SYSTEM_ADMIN)

        with flask_app_fixture.test_request_context(
            "/", method="POST", admin=system_admin
        ):
            flask.request.form = ImmutableMultiDict(
                [
                    ("email", "second_admin@nypl.org"),
                    ("password", "            "),
                    ("roles", json.dumps([])),
                ]
            )
            flask.request.files = ImmutableMultiDict()
            response = controller_fixture.controller.process_post()
            assert 400 == response.status_code
