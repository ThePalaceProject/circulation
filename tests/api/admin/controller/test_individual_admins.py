import json

import flask
import pytest
from werkzeug.datastructures import ImmutableMultiDict

from api.admin.controller.individual_admin_settings import (
    IndividualAdminSettingsController,
)
from api.admin.exceptions import AdminNotAuthorized
from api.admin.problem_details import (
    ADMIN_AUTH_NOT_CONFIGURED,
    INCOMPLETE_CONFIGURATION,
    INVALID_EMAIL,
    UNKNOWN_ROLE,
)
from api.problem_details import LIBRARY_NOT_FOUND
from core.model import Admin, AdminRole, create, get_one
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.flask import FlaskAppFixture


@pytest.fixture
def controller(db: DatabaseTransactionFixture) -> IndividualAdminSettingsController:
    return IndividualAdminSettingsController(db.session)


class TestIndividualAdmins:
    def test_individual_admins_get(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller: IndividualAdminSettingsController,
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
            response = controller.process_get()
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
            response = controller.process_get()
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
                controller.process_get,
            )

        with flask_app_fixture.test_request_context("/", admin=admin4):
            response = controller.process_get()
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
                controller.process_get,
            )

        with flask_app_fixture.test_request_context("/", admin=admin6):
            response = controller.process_get()
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
        controller: IndividualAdminSettingsController,
    ):
        # When the application is first started, there is no admin user. In that
        # case, we return a problem detail.

        with flask_app_fixture.test_request_context("/", method="GET"):
            response = controller.process_get()
            assert response == ADMIN_AUTH_NOT_CONFIGURED

    def test_individual_admins_post_errors(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller: IndividualAdminSettingsController,
    ):
        with flask_app_fixture.test_request_context("/", method="POST"):
            flask.request.form = ImmutableMultiDict([])
            response = controller.process_post()
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

        with flask_app_fixture.test_request_context("/", method="POST"):
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
            response = controller.process_post()
            assert response.uri == LIBRARY_NOT_FOUND.uri

        with flask_app_fixture.test_request_context("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("email", "not-a-email"),
                    ("password", "334df3f70bfe1979"),
                ]
            )
            response = controller.process_post()
            assert response.uri == INVALID_EMAIL.uri
            assert '"not-a-email" is not a valid email address' in response.detail

        library = db.library()
        with flask_app_fixture.test_request_context("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("email", "test@library.org"),
                    ("password", "334df3f70bfe1979"),
                    (
                        "roles",
                        json.dumps(
                            [{"role": "notarole", "library": library.short_name}]
                        ),
                    ),
                ]
            )
            response = controller.process_post()
            assert response.uri == UNKNOWN_ROLE.uri

    def test_individual_admins_post_permissions(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller: IndividualAdminSettingsController,
    ):
        l1 = db.library()
        l2 = db.library()
        system, ignore = create(db.session, Admin, email="system@example.com")
        system.add_role(AdminRole.SYSTEM_ADMIN)
        assert system.is_system_admin()

        sitewide_manager, ignore = create(
            db.session, Admin, email="sitewide_manager@example.com"
        )
        sitewide_manager.add_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
        assert sitewide_manager.is_sitewide_library_manager()

        sitewide_librarian, ignore = create(
            db.session, Admin, email="sitewide_librarian@example.com"
        )
        sitewide_librarian.add_role(AdminRole.SITEWIDE_LIBRARIAN)
        assert sitewide_manager.is_sitewide_librarian()

        manager1, ignore = create(
            db.session, Admin, email="library_manager_l1@example.com"
        )
        manager1.add_role(AdminRole.LIBRARY_MANAGER, l1)
        assert manager1.is_library_manager(l1)

        librarian1, ignore = create(db.session, Admin, email="librarian_l1@example.com")
        librarian1.add_role(AdminRole.LIBRARIAN, l1)
        assert librarian1.is_librarian(l1)

        l2 = db.library()
        manager2, ignore = create(
            db.session, Admin, email="library_manager_l2@example.com"
        )
        manager2.add_role(AdminRole.LIBRARY_MANAGER, l2)
        assert manager2.is_library_manager(l2)

        librarian2, ignore = create(db.session, Admin, email="librarian_l2@example.com")
        librarian2.add_role(AdminRole.LIBRARIAN, l2)
        assert librarian2.is_librarian(l2)

        def test_changing_roles(
            admin_making_request, target_admin, roles=None, allowed=False
        ):
            with flask_app_fixture.test_request_context(
                "/", method="POST", admin=admin_making_request
            ):
                flask.request.form = ImmutableMultiDict(
                    [
                        ("email", target_admin.email),
                        ("roles", json.dumps(roles or [])),
                    ]
                )
                if allowed:
                    controller.process_post()
                    db.session.rollback()
                else:
                    pytest.raises(
                        AdminNotAuthorized,
                        controller.process_post,
                    )

        # Various types of user trying to change a system admin's roles
        test_changing_roles(system, system, allowed=True)
        test_changing_roles(sitewide_manager, system)
        test_changing_roles(sitewide_librarian, system)
        test_changing_roles(manager1, system)
        test_changing_roles(librarian1, system)
        test_changing_roles(manager2, system)
        test_changing_roles(librarian2, system)

        # Various types of user trying to change a sitewide manager's roles
        test_changing_roles(system, sitewide_manager, allowed=True)
        test_changing_roles(sitewide_manager, sitewide_manager, allowed=True)
        test_changing_roles(sitewide_librarian, sitewide_manager)
        test_changing_roles(manager1, sitewide_manager)
        test_changing_roles(librarian1, sitewide_manager)
        test_changing_roles(manager2, sitewide_manager)
        test_changing_roles(librarian2, sitewide_manager)

        # Various types of user trying to change a sitewide librarian's roles
        test_changing_roles(system, sitewide_librarian, allowed=True)
        test_changing_roles(sitewide_manager, sitewide_librarian, allowed=True)
        test_changing_roles(sitewide_librarian, sitewide_librarian)
        test_changing_roles(manager1, sitewide_librarian)
        test_changing_roles(librarian1, sitewide_librarian)
        test_changing_roles(manager2, sitewide_librarian)
        test_changing_roles(librarian2, sitewide_librarian)

        test_changing_roles(manager1, manager1, allowed=True)
        test_changing_roles(
            manager1,
            sitewide_librarian,
            roles=[
                {"role": AdminRole.SITEWIDE_LIBRARIAN},
                {"role": AdminRole.LIBRARY_MANAGER, "library": l1.short_name},
            ],
            allowed=True,
        )
        test_changing_roles(manager1, librarian1, allowed=True)
        test_changing_roles(
            manager2,
            librarian2,
            roles=[{"role": AdminRole.LIBRARIAN, "library": l1.short_name}],
        )
        test_changing_roles(
            manager2,
            librarian1,
            roles=[{"role": AdminRole.LIBRARY_MANAGER, "library": l1.short_name}],
        )

        test_changing_roles(sitewide_librarian, librarian1)

        test_changing_roles(
            sitewide_manager, sitewide_manager, roles=[{"role": AdminRole.SYSTEM_ADMIN}]
        )
        test_changing_roles(
            sitewide_librarian,
            manager1,
            roles=[{"role": AdminRole.SITEWIDE_LIBRARY_MANAGER}],
        )

        def test_changing_password(admin_making_request, target_admin, allowed=False):
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
                if allowed:
                    controller.process_post()
                    db.session.rollback()
                else:
                    pytest.raises(
                        AdminNotAuthorized,
                        controller.process_post,
                    )

        # Various types of user trying to change a system admin's password
        test_changing_password(system, system, allowed=True)
        test_changing_password(sitewide_manager, system)
        test_changing_password(sitewide_librarian, system)
        test_changing_password(manager1, system)
        test_changing_password(librarian1, system)
        test_changing_password(manager2, system)
        test_changing_password(librarian2, system)

        # Various types of user trying to change a sitewide manager's password
        test_changing_password(system, sitewide_manager, allowed=True)
        test_changing_password(sitewide_manager, sitewide_manager, allowed=True)
        test_changing_password(sitewide_librarian, sitewide_manager)
        test_changing_password(manager1, sitewide_manager)
        test_changing_password(librarian1, sitewide_manager)
        test_changing_password(manager2, sitewide_manager)
        test_changing_password(librarian2, sitewide_manager)

        # Various types of user trying to change a sitewide librarian's password
        test_changing_password(system, sitewide_librarian, allowed=True)
        test_changing_password(sitewide_manager, sitewide_librarian, allowed=True)
        test_changing_password(manager1, sitewide_librarian)
        test_changing_password(manager2, sitewide_librarian)
        test_changing_password(sitewide_librarian, sitewide_librarian)
        test_changing_password(librarian1, sitewide_librarian)
        test_changing_password(librarian2, sitewide_librarian)

        # Various types of user trying to change a manager's password
        # Manager 1
        test_changing_password(system, manager1, allowed=True)
        test_changing_password(sitewide_manager, manager1, allowed=True)
        test_changing_password(manager1, manager1, allowed=True)
        test_changing_password(sitewide_librarian, manager1)
        test_changing_password(manager2, manager1)
        test_changing_password(librarian2, manager1)
        # Manager 2
        test_changing_password(system, manager2, allowed=True)
        test_changing_password(sitewide_manager, manager2, allowed=True)
        test_changing_password(manager2, manager2, allowed=True)
        test_changing_password(sitewide_librarian, manager2)
        test_changing_password(manager1, manager2)
        test_changing_password(librarian1, manager2)

        # Various types of user trying to change a librarian's password
        # Librarian 1
        test_changing_password(system, librarian1, allowed=True)
        test_changing_password(sitewide_manager, librarian1, allowed=True)
        test_changing_password(manager1, librarian1, allowed=True)
        test_changing_password(sitewide_librarian, librarian1)
        test_changing_password(manager2, librarian1)
        test_changing_password(librarian2, librarian1)
        # Librarian 2
        test_changing_password(system, librarian2, allowed=True)
        test_changing_password(sitewide_manager, librarian2, allowed=True)
        test_changing_password(manager2, librarian2, allowed=True)
        test_changing_password(sitewide_librarian, librarian2)
        test_changing_password(manager1, librarian2)
        test_changing_password(librarian1, librarian2)

        # Library crossover tests
        manager1_2, ignore = create(
            db.session, Admin, email="library_manager_l1_l2@example.com"
        )
        manager1_2.add_role(AdminRole.LIBRARY_MANAGER, l1)
        manager1_2.add_role(AdminRole.LIBRARY_MANAGER, l2)
        # A manager of library1 should not be allowed for a manager of library1 and library2
        test_changing_password(system, manager1_2, allowed=True)
        test_changing_password(sitewide_manager, manager1_2, allowed=True)
        test_changing_password(manager1_2, manager1_2, allowed=True)
        test_changing_password(sitewide_librarian, manager1_2)
        test_changing_password(manager1, manager1_2)
        test_changing_password(manager2, manager1_2)
        test_changing_password(librarian1, manager1_2)
        # A manager of both libraries should be able to change the passwords of both
        test_changing_password(manager1_2, manager1, allowed=True)
        test_changing_password(manager1_2, manager2, allowed=True)
        test_changing_password(manager1_2, librarian1, allowed=True)
        test_changing_password(manager1_2, librarian2, allowed=True)
        test_changing_password(manager1_2, system)
        test_changing_password(manager1_2, sitewide_manager)
        test_changing_password(manager1_2, sitewide_librarian)

    def test_individual_admins_post_create(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller: IndividualAdminSettingsController,
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
            response = controller.process_post()
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
            response = controller.process_post()
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
        controller: IndividualAdminSettingsController,
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
            response = controller.process_post()
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
        controller: IndividualAdminSettingsController,
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
                controller.process_delete,
                librarian.email,
            )

        with flask_app_fixture.test_request_context(
            "/", method="DELETE", admin=sitewide_manager
        ):
            response = controller.process_delete(librarian.email)
            assert response.status_code == 200

            pytest.raises(
                AdminNotAuthorized,
                controller.process_delete,
                system_admin.email,
            )

        with flask_app_fixture.test_request_context(
            "/", method="DELETE", admin=system_admin
        ):
            response = controller.process_delete(system_admin.email)
            assert response.status_code == 200

        admin = get_one(db.session, Admin, id=librarian.id)
        assert None == admin

        admin = get_one(db.session, Admin, id=system_admin.id)
        assert None == admin

    def test_individual_admins_post_create_not_system(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller: IndividualAdminSettingsController,
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
                controller.process_post,
            )

    def test_individual_admins_post_create_requires_password(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller: IndividualAdminSettingsController,
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
            response = controller.process_post()
            assert 400 == response.status_code
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

    def test_individual_admins_post_create_requires_non_empty_password(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller: IndividualAdminSettingsController,
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
            response = controller.process_post()
            assert 400 == response.status_code
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

    def test_individual_admins_post_create_on_setup(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller: IndividualAdminSettingsController,
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
            response = controller.process_post()
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
        controller: IndividualAdminSettingsController,
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
            response = controller.process_post()
            assert 201 == response.status_code

    def test_individual_admins_post_create_second_admin_no_roles(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller: IndividualAdminSettingsController,
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
            response = controller.process_post()
            assert 201 == response.status_code

    def test_individual_admins_post_create_second_admin_no_password(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller: IndividualAdminSettingsController,
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
            response = controller.process_post()
            assert 400 == response.status_code

    def test_individual_admins_post_create_second_admin_empty_password(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller: IndividualAdminSettingsController,
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
            response = controller.process_post()
            assert 400 == response.status_code

    def test_individual_admins_post_create_second_admin_blank_password(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        controller: IndividualAdminSettingsController,
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
            response = controller.process_post()
            assert 400 == response.status_code
