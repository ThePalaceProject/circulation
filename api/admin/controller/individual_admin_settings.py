import json
from typing import Optional

import flask
from flask import Response
from flask_babel import lazy_gettext as _
from sqlalchemy.exc import ProgrammingError

from api.admin.controller.settings import SettingsController
from api.admin.exceptions import AdminNotAuthorized
from api.admin.problem_details import (
    ADMIN_AUTH_NOT_CONFIGURED,
    INCOMPLETE_CONFIGURATION,
    MISSING_ADMIN,
    MISSING_PGCRYPTO_EXTENSION,
    UNKNOWN_ROLE,
)
from api.problem_details import LIBRARY_NOT_FOUND
from core.model import Admin, AdminRole, Library, get_one, get_one_or_create
from core.util.problem_detail import ProblemDetail


class IndividualAdminSettingsController(SettingsController):
    def process_individual_admins(self):
        if flask.request.method == "GET":
            return self.process_get()
        else:
            return self.process_post()

    def _highest_authorized_role(self) -> Optional[AdminRole]:
        highest_role: Optional[AdminRole] = None
        has_auth = False

        admin = getattr(flask.request, "admin", None)

        if not admin:
            return None

        for role in admin.roles:
            if role.role in (
                AdminRole.SYSTEM_ADMIN,
                AdminRole.SITEWIDE_LIBRARY_MANAGER,
                AdminRole.LIBRARY_MANAGER,
            ):
                # Admin has the authority to view this API
                has_auth = True

            if (
                not highest_role
                or highest_role.compare_role(role) == AdminRole.LESS_THAN
            ):
                # What is the highest role this admin possesses (via AdminRole.ROLES)
                highest_role = role
        return highest_role if has_auth else None

    def process_get(self):
        logged_in_admin: Optional[Admin] = getattr(flask.request, "admin", None)
        if not logged_in_admin:
            return ADMIN_AUTH_NOT_CONFIGURED

        highest_role: AdminRole = self._highest_authorized_role()

        if not highest_role:
            raise AdminNotAuthorized()

        def append_role(roles, role):
            role_dict = dict(role=role.role)
            if role.library:
                role_dict["library"] = role.library.short_name
            roles.append(role_dict)

        admins = []
        for admin in self._db.query(Admin).order_by(Admin.email):
            roles = []
            show_admin = True
            for role in admin.roles:
                # System admin sees all
                if highest_role.role == AdminRole.SYSTEM_ADMIN:
                    append_role(roles, role)

                # Sitewide managers see each other and lower
                elif (
                    highest_role.role == AdminRole.SITEWIDE_LIBRARY_MANAGER
                    and highest_role.compare_role(role) is not AdminRole.LESS_THAN
                ):
                    append_role(roles, role)

                # Managers
                elif highest_role.role == AdminRole.LIBRARY_MANAGER:
                    # See same library admins
                    if role.library and logged_in_admin.is_library_manager(
                        role.library
                    ):
                        append_role(roles, role)
                    # and sitewide managers and librarians
                    elif role.role in {
                        AdminRole.SITEWIDE_LIBRARY_MANAGER,
                        AdminRole.SITEWIDE_LIBRARIAN,
                    }:
                        append_role(roles, role)

            if len(roles):
                admins.append(dict(email=admin.email, roles=roles))

        return dict(
            individualAdmins=admins,
        )

    def process_post_create_first_admin(self, email: str):
        """Create the first admin in the system."""

        # Passwords are always required, so check presence and validity up front.
        password: Optional[str] = flask.request.form.get("password")
        if not self.is_acceptable_password(password):
            return self.unacceptable_password()

        success = False
        try:
            admin, _ = get_one_or_create(self._db, Admin, email=email)
            self.check_permissions(admin, settingUp=True)

            # Update the roles, if requested.
            roles_json = flask.request.form.get("roles")
            if roles_json:
                roles = json.loads(roles_json)
            else:
                roles = []

            roles_error = self.handle_roles(admin, roles, settingUp=True)
            if roles_error:
                return roles_error

            # Update the password, if requested.
            self.handle_password(password, admin, is_new=True, settingUp=True)

            success = True
            return self.response(admin, is_new=True)
        finally:
            if not success:
                self._db.rollback()

    def process_post_create_new_admin(self, email: str):
        """Create a new admin (not the first admin in the system)."""

        # Passwords are always required, so check presence and validity up front.
        password: Optional[str] = flask.request.form.get("password")
        if not self.is_acceptable_password(password):
            return self.unacceptable_password()

        success = False
        try:
            admin, _ = get_one_or_create(self._db, Admin, email=email)
            self.check_permissions(admin, settingUp=False)

            # Update the roles, if requested.
            roles_json = flask.request.form.get("roles")
            if roles_json:
                roles = json.loads(roles_json)
            else:
                roles = []

            roles_error = self.handle_roles(admin, roles, settingUp=False)
            if roles_error:
                return roles_error

            # Update the password, if requested.
            self.handle_password(password, admin, is_new=True, settingUp=False)
            success = True
            return self.response(admin, is_new=True)
        finally:
            if not success:
                self._db.rollback()

    def process_post_update_existing_admin(self, admin: Admin):
        """Update an existing admin."""
        password: Optional[str] = flask.request.form.get("password")

        success = False
        try:
            self.check_permissions(admin, settingUp=False)

            # If a password is provided, it must be valid.
            if password:
                if not self.is_acceptable_password(password):
                    return self.unacceptable_password()

            # Update the roles, if requested.
            roles_json = flask.request.form.get("roles")
            if roles_json:
                roles = json.loads(roles_json)
            else:
                roles = []

            roles_error = self.handle_roles(admin, roles, settingUp=False)
            if roles_error:
                return roles_error

            # Update the password, if requested.
            self.handle_password(password, admin, is_new=False, settingUp=False)

            success = True
            return self.response(admin, is_new=False)
        finally:
            if not success:
                self._db.rollback()

    def process_post(self):
        # There are three possible paths through this method:
        #
        # 1. The admin being edited is the first admin to be created. In this case,
        #    a password is required and pretty much everything is permitted.
        # 2. The admin being edited doesn't exist, but is not the first admin in
        #    the system. In this case, a password is required.
        # 3. The admin being edited exists. In this case, a password is only required
        #    if the intention is to change the password of the admin.

        email = flask.request.form.get("email")

        error = self.validate_form_fields(email)
        if error:
            return error

        # If there are no admins yet, anyone can create the first system admin.
        creating_first_admin = self._db.query(Admin).count() == 0
        if creating_first_admin:
            return self.process_post_create_first_admin(email)

        highest_role = self._highest_authorized_role()
        if not highest_role:
            raise AdminNotAuthorized()

        # Otherwise, check to see if the admin exists.
        existing_admin = get_one(self._db, Admin, email=email)
        if existing_admin is None:
            return self.process_post_create_new_admin(email)

        # The admin exists. We might just be updating the password or roles.
        return self.process_post_update_existing_admin(existing_admin)

    @staticmethod
    def unacceptable_password():
        return INCOMPLETE_CONFIGURATION.detailed(
            _("The password field cannot be blank.")
        )

    @staticmethod
    def is_acceptable_password(password: Optional[str]) -> bool:
        # Forbid missing passwords.
        if not password:
            return False

        # Forbid passwords that are empty after leading/trailing whitespace stripping.
        if len(password.strip()) == 0:
            return False

        return True

    def check_permissions(self, admin, settingUp):
        """Before going any further, check that the user actually has permission
        to create/edit this type of admin"""

        # For readability: the person who is submitting the form is referred to as "user"
        # rather than as something that could be confused with "admin" (the admin
        # which the user is submitting the form in order to create/edit.)

        if not settingUp:
            user = flask.request.admin

            # System admin has all permissions.
            if user.is_system_admin():
                return

            # If we've hit this point, then the user isn't a system admin.  If the
            # admin is a system admin, the user won't be able to do anything.
            if admin.is_system_admin():
                raise AdminNotAuthorized()

            # By this point, we know no one is a system admin.
            if user.is_sitewide_library_manager():
                return

            # The user isn't a system admin or a sitewide manager.
            if admin.is_sitewide_library_manager():
                raise AdminNotAuthorized()

    def validate_form_fields(self, email):
        """Check that 1) the user has entered something into the required fields,
        and 2) if so, the input is formatted as a valid email address."""
        if not email:
            return INCOMPLETE_CONFIGURATION.detailed(
                _("The email field cannot be blank.")
            )

        email_error = self.validate_formats(email)
        if email_error:
            return email_error

    def validate_role_exists(self, role):
        if role.get("role") not in AdminRole.ROLES:
            return UNKNOWN_ROLE

    def look_up_library_for_role(self, role):
        """If the role is affiliated with a particular library, as opposed to being
        sitewide, find the library (and check that it actually exists)."""
        library = None
        library_short_name = role.get("library")
        if library_short_name:
            library = Library.lookup(self._db, library_short_name)
            if not library:
                return LIBRARY_NOT_FOUND.detailed(
                    _(
                        'Library "%(short_name)s" does not exist.',
                        short_name=library_short_name,
                    )
                )
        return library

    def handle_roles(self, admin, roles, settingUp):
        """Compare the admin's existing set of roles against the roles submitted in the form, and,
        unless there's a problem with the roles or the permissions, modify the admin's roles accordingly
        """

        # User = person submitting the form; admin = person who the form is about

        if settingUp:
            # There are no admins yet; the user and the new system admin are the same person.
            user = admin
        else:
            user = flask.request.admin

        old_roles = admin.roles
        old_roles_set = {(role.role, role.library) for role in old_roles}

        for role in roles:
            error = self.validate_role_exists(role)
            if error:
                return error

            library = self.look_up_library_for_role(role)
            if isinstance(library, ProblemDetail):
                return library

            if (role.get("role"), library) in old_roles_set:
                # The admin already has this role.
                continue

            if library:
                self.require_library_manager(library)
            elif role.get("role") == AdminRole.SYSTEM_ADMIN and not settingUp:
                self.require_system_admin()
            elif not settingUp:
                self.require_sitewide_library_manager()
            admin.add_role(role.get("role"), library)

        new_roles = {(role.get("role"), role.get("library")) for role in roles}
        for role in old_roles:
            library = None
            if role.library:
                library = role.library.short_name
            if not (role.role, library) in new_roles:
                if not library:
                    self.require_sitewide_library_manager()
                if user and user.is_librarian(role.library):
                    # A librarian can see roles for the library, but only a library manager
                    # can delete them.
                    self.require_library_manager(role.library)
                    admin.remove_role(role.role, role.library)
                else:
                    # An admin who isn't a librarian for the library won't be able to see
                    # its roles, so might make requests that change other roles without
                    # including this library's roles. Leave the non-visible roles alone.
                    continue

    def handle_password(self, password, admin: Admin, is_new, settingUp):
        """Check that the user has permission to change this type of admin's password"""

        # User = person submitting the form; admin = person who the form is about
        if settingUp:
            # There are no admins yet; the user and the new system admin are the same person.
            user = admin
        else:
            user: Admin = flask.request.admin  # type: ignore

        if password:
            # If the admin we're editing has a sitewide manager role, we've already verified
            # the current admin's role in check_permissions. Otherwise, an admin can only change that
            # admin's password if they are a library manager of one of that admin's
            # libraries, or if they are editing a new admin or an admin who has no
            # roles yet.
            # TODO: set up password reset emails instead.
            # NOTE: librarians can change their own passwords via SignInController.change_password(),
            # but not via this controller; this is because they don't have access to the
            # IndividualAdmins create/edit form.
            message = None
            if not is_new and not admin.is_sitewide_library_manager():
                can_change_pw = False
                if not admin.roles:
                    can_change_pw = True
                if admin.is_sitewide_librarian():
                    # Only a manager of the same or higher level can change the password
                    if user.is_sitewide_librarian():
                        can_change_pw = True
                    else:
                        message = "Only an administrator can change another administrators password."
                else:
                    # If any of the target users libraries are outside of logged-in users
                    # libraries then they should not be able to change the password
                    for role in admin.roles:
                        if not user.is_library_manager(role.library):
                            message = f"User is part of '{role.library.name}', you are not authorized to change their password."
                            break
                    else:
                        can_change_pw = True
                if not can_change_pw:
                    raise AdminNotAuthorized(message)
            admin.password = password
        try:
            self._db.flush()
        except ProgrammingError as e:
            self._db.rollback()
            return MISSING_PGCRYPTO_EXTENSION

    def response(self, admin, is_new):
        if is_new:
            return Response(str(admin.email), 201)
        else:
            return Response(str(admin.email), 200)

    def process_delete(self, email):
        self.require_sitewide_library_manager()
        admin = get_one(self._db, Admin, email=email)
        if admin.is_system_admin():
            self.require_system_admin()
        if not admin:
            return MISSING_ADMIN
        self._db.delete(admin)
        return Response(str(_("Deleted")), 200)
