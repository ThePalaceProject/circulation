# Admin, AdminRole

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import bcrypt
from flask_babel import lazy_gettext as _
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from sqlalchemy import (
    Column,
    ForeignKey,
    Index,
    Integer,
    Unicode,
    UniqueConstraint,
    func,
    or_,
    select,
)
from sqlalchemy.orm import Mapped, Query, relationship, validates
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.session import Session

from palace.manager.core.problem_details import INVALID_RESET_PASSWORD_TOKEN
from palace.manager.sqlalchemy.hassessioncache import HasSessionCache
from palace.manager.sqlalchemy.hybrid import hybrid_property
from palace.manager.sqlalchemy.model.base import Base
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.util import get_one, get_one_or_create
from palace.manager.util.problem_detail import ProblemDetail

if TYPE_CHECKING:
    from palace.manager.sqlalchemy.model.collection import Collection


class Admin(Base, HasSessionCache):
    __tablename__ = "admins"

    id: Mapped[int] = Column(Integer, primary_key=True)
    email: Mapped[str] = Column(Unicode, unique=True, nullable=False)

    # Admins can also log in with a local password.
    password_hashed = Column(Unicode, index=True)

    # An Admin may have many roles.
    roles: Mapped[list[AdminRole]] = relationship(
        "AdminRole", back_populates="admin", cascade="all, delete-orphan", uselist=True
    )

    # Token age is max 30 minutes, in seconds
    RESET_PASSWORD_TOKEN_MAX_AGE = 1800

    def cache_key(self) -> str:
        return self.email

    @validates("email")  # type: ignore[untyped-decorator]
    def validate_email(self, key: str, address: str) -> str:
        # strip any whitespace from email address
        return address.strip()

    @hybrid_property
    def password(self) -> Any:
        raise NotImplementedError("Password comparison is only with Admin.authenticate")

    @password.setter
    def password(self, value: str) -> None:
        self.password_hashed = bcrypt.hashpw(value.encode(), bcrypt.gensalt()).decode()

    def has_password(self, password: str) -> bool:
        if self.password_hashed is None:
            return False
        return bcrypt.checkpw(password.encode(), self.password_hashed.encode())

    @classmethod
    def authenticate(cls, _db: Session, email: str, password: str) -> Admin | None:
        """Finds an authenticated Admin by email and password
        :return: Admin or None
        """

        def lookup_hook() -> tuple[Admin | None, bool]:
            try:
                return (
                    _db.query(Admin)
                    .filter(func.upper(Admin.email) == email.upper())
                    .limit(1)
                    .one(),
                    False,
                )
            except NoResultFound:
                return None, False

        match, _ = Admin.by_cache_key(_db, str(email), lookup_hook)
        if match and not match.has_password(password):
            # Admin with this email was found, but password is invalid.
            match = None
        return match

    @classmethod
    def with_password(cls, _db: Session) -> Query[Admin]:
        """Get Admins that have a password."""
        return _db.query(Admin).filter(Admin.password_hashed != None)  # noqa: E711

    def is_system_admin(self) -> bool:
        _db = Session.object_session(self)

        def lookup_hook() -> tuple[AdminRole | None, bool]:
            return (
                get_one(_db, AdminRole, admin=self, role=AdminRole.SYSTEM_ADMIN),
                False,
            )

        role, _ = AdminRole.by_cache_key(
            _db, (self.id, None, AdminRole.SYSTEM_ADMIN), lookup_hook
        )
        if role:
            return True
        return False

    def is_sitewide_library_manager(self) -> bool:
        _db = Session.object_session(self)
        if self.is_system_admin():
            return True

        def lookup_hook() -> tuple[AdminRole | None, bool]:
            return (
                get_one(
                    _db, AdminRole, admin=self, role=AdminRole.SITEWIDE_LIBRARY_MANAGER
                ),
                False,
            )

        role, _ = AdminRole.by_cache_key(
            _db, (self.id, None, AdminRole.SITEWIDE_LIBRARY_MANAGER), lookup_hook
        )
        if role:
            return True
        return False

    def is_sitewide_librarian(self) -> bool:
        _db = Session.object_session(self)
        if self.is_sitewide_library_manager():
            return True

        def lookup_hook() -> tuple[AdminRole | None, bool]:
            return (
                get_one(_db, AdminRole, admin=self, role=AdminRole.SITEWIDE_LIBRARIAN),
                False,
            )

        role, _ = AdminRole.by_cache_key(
            _db, (self.id, None, AdminRole.SITEWIDE_LIBRARIAN), lookup_hook
        )
        if role:
            return True
        return False

    def is_library_manager(self, library: Library | None) -> bool:
        _db = Session.object_session(self)
        # First check if the admin is a manager of _all_ libraries.
        if self.is_sitewide_library_manager():
            return True

        # If library is None (sitewide role), only sitewide managers can manage it
        if library is None:
            return False

        # If not, they could still be a manager of _this_ library.
        def lookup_hook() -> tuple[AdminRole | None, bool]:
            return (
                get_one(
                    _db,
                    AdminRole,
                    admin=self,
                    library=library,
                    role=AdminRole.LIBRARY_MANAGER,
                ),
                False,
            )

        role, _ = AdminRole.by_cache_key(
            _db, (self.id, library.id, AdminRole.LIBRARY_MANAGER), lookup_hook
        )
        if role:
            return True
        return False

    def is_librarian(self, library: Library | None) -> bool:
        _db = Session.object_session(self)
        # If the admin is a library manager, they can do everything a librarian can do.
        if self.is_library_manager(library):
            return True
        # Check if the admin is a librarian for _all_ libraries.
        if self.is_sitewide_librarian():
            return True

        # If library is None (sitewide role), only sitewide librarians can access it
        if library is None:
            return False

        # If not, they might be a librarian of _this_ library.
        def lookup_hook() -> tuple[AdminRole | None, bool]:
            return (
                get_one(
                    _db,
                    AdminRole,
                    admin=self,
                    library=library,
                    role=AdminRole.LIBRARIAN,
                ),
                False,
            )

        role, _ = AdminRole.by_cache_key(
            _db, (self.id, library.id, AdminRole.LIBRARIAN), lookup_hook
        )
        if role:
            return True
        return False

    def can_see_collection(self, collection: Collection) -> bool:
        if self.is_system_admin():
            return True
        for library in collection.associated_libraries:
            if self.is_librarian(library):
                return True
        return False

    def authorized_libraries(self) -> list[Library]:
        query = select(Library).order_by(Library.id)
        session = Session.object_session(self)
        if self.is_sitewide_librarian():
            return session.scalars(query).all()
        return session.scalars(
            query.join(AdminRole).where(
                AdminRole.admin_id == self.id,
                or_(
                    AdminRole.role == AdminRole.LIBRARY_MANAGER,
                    AdminRole.role == AdminRole.LIBRARIAN,
                ),
            )
        ).all()

    def add_role(self, role: str, library: Library | None = None) -> AdminRole:
        _db = Session.object_session(self)
        admin_role, _ = get_one_or_create(
            _db, AdminRole, admin=self, role=role, library=library
        )
        return admin_role

    def remove_role(self, role: str, library: Library | None = None) -> None:
        _db = Session.object_session(self)
        admin_role = get_one(_db, AdminRole, admin=self, role=role, library=library)
        if admin_role:
            _db.delete(admin_role)

    def generate_reset_password_token(self, secret_key: str) -> str:
        serializer = URLSafeTimedSerializer(secret_key)

        return serializer.dumps(self.email, salt=self.password_hashed)

    @staticmethod
    def validate_reset_password_token_and_fetch_admin(
        token: str, admin_id: int, _db: Session, secret_key: str
    ) -> ProblemDetail | Admin:
        serializer = URLSafeTimedSerializer(secret_key)

        # We first load admin using admin_id sent in the request.
        possible_admin = get_one(_db, Admin, id=admin_id)

        if possible_admin is None:
            return INVALID_RESET_PASSWORD_TOKEN

        # There exists an admin that matches the admin_id sent in the request. Now we can check the validity of the
        # sent token.
        # We use the existing password hash as a salt to invalidate token if the user has already used the same
        # token and already changed the password
        try:
            admin_email = serializer.loads(
                token,
                max_age=Admin.RESET_PASSWORD_TOKEN_MAX_AGE,
                salt=possible_admin.password_hashed,
            )
        except SignatureExpired:
            return INVALID_RESET_PASSWORD_TOKEN.detailed(
                _("Reset password token has expired.")
            )
        except BadSignature:
            return INVALID_RESET_PASSWORD_TOKEN

        # We also check that deserialized admin email from the token matches the admin email from the database.
        if possible_admin.email != admin_email:
            return INVALID_RESET_PASSWORD_TOKEN

        return possible_admin

    def __repr__(self) -> str:
        return "<Admin: email=%s>" % self.email


class AdminRole(Base, HasSessionCache):
    __tablename__ = "adminroles"

    id: Mapped[int] = Column(Integer, primary_key=True)
    admin_id: Mapped[int] = Column(
        Integer, ForeignKey("admins.id"), nullable=False, index=True
    )
    admin: Mapped[Admin] = relationship("Admin", back_populates="roles")
    library_id = Column(Integer, ForeignKey("libraries.id"), nullable=True, index=True)
    library: Mapped[Library | None] = relationship(
        "Library", back_populates="adminroles"
    )
    role: Mapped[str] = Column(Unicode, nullable=False, index=True)

    __table_args__ = (UniqueConstraint("admin_id", "library_id", "role"),)

    SYSTEM_ADMIN = "system"
    SITEWIDE_LIBRARY_MANAGER = "manager-all"
    LIBRARY_MANAGER = "manager"
    SITEWIDE_LIBRARIAN = "librarian-all"
    LIBRARIAN = "librarian"

    ROLES = [
        SYSTEM_ADMIN,
        SITEWIDE_LIBRARY_MANAGER,
        LIBRARY_MANAGER,
        SITEWIDE_LIBRARIAN,
        LIBRARIAN,
    ]
    LESS_THAN = -1
    EQUAL = 0
    GREATER_THAN = 1

    def cache_key(self) -> tuple[int, int | None, str]:
        return (self.admin_id, self.library_id, self.role)

    def to_dict(self) -> dict[str, str]:
        if self.library:
            return dict(role=self.role, library=self.library.short_name)
        return dict(role=self.role)

    def __repr__(self) -> str:
        return "<AdminRole: role={} library={} admin={}>".format(
            self.role,
            self.library and self.library.short_name,
            self.admin.email,
        )

    def compare_role(self, other: AdminRole) -> int:
        """Compare one role to the other for hierarchy"""
        if not self.role or not other.role:
            raise ValueError("Cannot compare role to None")

        self_ix = self.ROLES.index(self.role)
        other_ix = self.ROLES.index(other.role)
        if self_ix == other_ix:
            return self.EQUAL
        elif self_ix > other_ix:  # Lower priority role is later in the array
            return self.LESS_THAN
        else:
            return self.GREATER_THAN


Index(
    "ix_adminroles_admin_id_library_id_role",
    AdminRole.admin_id,
    AdminRole.library_id,
    AdminRole.role,
)
