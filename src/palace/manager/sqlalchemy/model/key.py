import datetime
import uuid
from collections.abc import Callable
from enum import Enum
from typing import Literal, Self, overload

from sqlalchemy import Column, DateTime, Enum as SaEnum, Unicode, delete, select
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, Session

from palace.manager.sqlalchemy.model.base import Base
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.string_helpers import random_key
from palace.manager.util.uuid import uuid_decode


class KeyType(Enum):
    AUTH_TOKEN_JWE = "auth_token"
    BEARER_TOKEN_SIGNING = "bearer_token"
    ADMIN_SECRET_KEY = "admin_auth"


class Key(Base):
    __tablename__ = "keys"

    id: Mapped[uuid.UUID] = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    created: Mapped[datetime.datetime] = Column(
        DateTime(timezone=True), index=True, nullable=False, default=utc_now
    )
    value: Mapped[str] = Column(Unicode, nullable=False)
    type: Mapped[KeyType] = Column(SaEnum(KeyType), nullable=False, index=True)

    def __repr__(self) -> str:
        return f"<Key id={self.id} created={self.created} type={self.type}>"

    @classmethod
    @overload
    def get_key(
        cls,
        db: Session,
        key_type: KeyType,
        key_id: str | uuid.UUID | None = None,
        *,
        raise_exception: Literal[True] = True,
    ) -> Self: ...

    @classmethod
    @overload
    def get_key(
        cls,
        db: Session,
        key_type: KeyType,
        key_id: str | uuid.UUID | None = None,
        *,
        raise_exception: bool = False,
    ) -> Self | None: ...

    @classmethod
    def get_key(
        cls,
        db: Session,
        key_type: KeyType,
        key_id: str | uuid.UUID | None = None,
        *,
        raise_exception: bool = False,
    ) -> Self | None:
        """Get a key from the DB"""
        key_query = select(Key).where(Key.type == key_type).order_by(Key.created.desc())

        if key_id is not None:
            decoded_kid = uuid_decode(key_id) if isinstance(key_id, str) else key_id
            key_query = key_query.where(Key.id == decoded_kid)

        result_key = db.scalars(key_query).first()
        if result_key is None and raise_exception:
            raise ValueError(f"No key found in the database with type {key_type}")

        return result_key

    @classmethod
    def create_key(
        cls, db: Session, key_type: KeyType, create: Callable[[uuid.UUID], str]
    ) -> Self:
        """Create a new key in the DB"""
        key_id = uuid.uuid4()
        value = create(key_id)

        key = cls(id=key_id, value=value, type=key_type)
        db.add(key)
        db.flush()
        return key

    @classmethod
    def create_admin_secret_key(cls, db: Session) -> Self:
        """Create a new admin secret key in the DB"""
        # If we already have an admin secret key, we should not create a new one
        existing_key = cls.get_key(db, KeyType.ADMIN_SECRET_KEY)
        if existing_key:
            return existing_key

        return cls.create_key(db, KeyType.ADMIN_SECRET_KEY, lambda _: random_key(48))

    @classmethod
    def create_bearer_token_signing_key(cls, db: Session) -> Self:
        """Create a new admin secret key in the DB"""
        # If we already have a bearer token signing key, we should not create a new one
        existing_key = cls.get_key(db, KeyType.BEARER_TOKEN_SIGNING)
        if existing_key:
            return existing_key

        return cls.create_key(
            db, KeyType.BEARER_TOKEN_SIGNING, lambda _: random_key(48)
        )

    @classmethod
    def delete_old_keys(
        cls, db: Session, key_type: KeyType, keep: int, older_than: datetime.datetime
    ) -> int:
        """
        Delete old keys from the DB
        """
        if keep < 0:
            raise ValueError("keep must be a non-negative integer")

        if keep > 0:
            ids_to_keep = [
                row.id
                for row in db.execute(
                    select(cls.id)
                    .where(cls.type == key_type)
                    .order_by(cls.created.desc())
                    .limit(keep)
                )
            ]
        else:
            ids_to_keep = []

        delete_query = (
            delete(cls).where(cls.type == key_type).where(cls.created < older_than)
        )

        if ids_to_keep:
            delete_query = delete_query.where(cls.id.notin_(ids_to_keep))

        result = db.execute(delete_query)
        # mypy doesn't recognize the rowcount attribute on the CursorResult
        # since db.execute doesn't always return a CursorResult.
        # The sqlalchemy docs say that the rowcount attribute is always present
        # when doing a DELETE statement, so we can safely ignore this error.
        # https://docs.sqlalchemy.org/en/20/tutorial/data_update.html#getting-affected-row-count-from-update-delete
        return result.rowcount  # type: ignore[attr-defined]
