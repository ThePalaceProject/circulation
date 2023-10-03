from __future__ import annotations

import datetime
import uuid
from typing import TYPE_CHECKING

from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.orm import Mapped, relationship
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import and_

from core.model import Base, get_one, get_one_or_create
from core.util import is_session
from core.util.datetime_helpers import utc_now

if TYPE_CHECKING:
    from core.model import Collection, DataSource, Patron


class Credential(Base):
    """A place to store credentials for external services."""

    __tablename__ = "credentials"
    id = Column(Integer, primary_key=True)
    data_source_id = Column(Integer, ForeignKey("datasources.id"), index=True)
    data_source: Mapped[DataSource] = relationship(
        "DataSource", back_populates="credentials"
    )
    patron_id = Column(Integer, ForeignKey("patrons.id"), index=True)
    patron: Mapped[Patron] = relationship("Patron", back_populates="credentials")
    collection_id = Column(Integer, ForeignKey("collections.id"), index=True)
    collection: Mapped[Collection] = relationship(
        "Collection", back_populates="credentials"
    )
    type = Column(String(255), index=True)
    credential = Column(String)
    expires = Column(DateTime(timezone=True), index=True)

    __table_args__ = (
        # Unique indexes to prevent the creation of redundant credentials.
        # If both patron_id and collection_id are null, then (data_source_id,
        # type, credential) must be unique.
        Index(
            "ix_credentials_data_source_id_type_token",
            data_source_id,
            type,
            credential,
            unique=True,
            postgresql_where=and_(patron_id == None, collection_id == None),
        ),
        # If patron_id is null but collection_id is not, then
        # (data_source, type, collection_id) must be unique.
        Index(
            "ix_credentials_data_source_id_type_collection_id",
            data_source_id,
            type,
            collection_id,
            unique=True,
            postgresql_where=(patron_id == None),
        ),
        # If collection_id is null but patron_id is not, then
        # (data_source, type, patron_id) must be unique.
        # (At the moment this never happens.)
        Index(
            "ix_credentials_data_source_id_type_patron_id",
            data_source_id,
            type,
            patron_id,
            unique=True,
            postgresql_where=(collection_id == None),
        ),
        # If neither collection_id nor patron_id is null, then
        # (data_source, type, patron_id, collection_id)
        # must be unique.
        Index(
            "ix_credentials_data_source_id_type_patron_id_collection_id",
            data_source_id,
            type,
            patron_id,
            collection_id,
            unique=True,
        ),
    )

    # A meaningless identifier used to identify this patron (and no other)
    # to a remote service.
    IDENTIFIER_TO_REMOTE_SERVICE = "Identifier Sent To Remote Service"

    # An identifier used by a remote service to identify this patron.
    IDENTIFIER_FROM_REMOTE_SERVICE = "Identifier Received From Remote Service"

    @classmethod
    def _filter_invalid_credential(
        cls, credential: Credential, allow_persistent_token: bool
    ) -> Credential | None:
        """Filter out invalid credentials based on their expiration time and persistence.

        :param credential: Credential object
        :param allow_persistent_token: Boolean value indicating whether persistent tokens are allowed
        """
        if not credential:
            # No matching token.
            return None

        if not credential.expires:
            if allow_persistent_token:
                return credential
            else:
                # It's an error that this token never expires. It's invalid.
                return None
        elif credential.expires > utc_now():
            return credential
        else:
            # Token has expired.
            return None

    @classmethod
    def lookup(
        cls,
        _db,
        data_source,
        token_type,
        patron,
        refresher_method,
        allow_persistent_token=False,
        allow_empty_token=False,
        collection=None,
        force_refresh=False,
    ) -> Credential:
        from core.model.datasource import DataSource

        if isinstance(data_source, str):
            data_source = DataSource.lookup(_db, data_source)
        credential, is_new = get_one_or_create(
            _db,
            Credential,
            data_source=data_source,
            type=token_type,
            patron=patron,
            collection=collection,
        )
        if (
            is_new
            or force_refresh
            or (not credential.expires and not allow_persistent_token)
            or (not credential.credential and not allow_empty_token)
            or (credential.expires and credential.expires <= utc_now())
        ):
            if refresher_method:
                refresher_method(credential)
        return credential

    @classmethod
    def lookup_by_token(
        cls, _db, data_source, token_type, token, allow_persistent_token=False
    ):
        """Look up a unique token.
        Lookup will fail on expired tokens. Unless persistent tokens
        are specifically allowed, lookup will fail on persistent tokens.
        """

        credential = get_one(
            _db, Credential, data_source=data_source, type=token_type, credential=token
        )

        return cls._filter_invalid_credential(credential, allow_persistent_token)

    @classmethod
    def lookup_by_patron(
        cls,
        _db: Session,
        data_source_name: str,
        token_type: str,
        patron: Patron,
        allow_persistent_token: bool = False,
        auto_create_datasource: bool = True,
    ) -> Credential | None:
        """Look up a unique token.
        Lookup will fail on expired tokens. Unless persistent tokens
        are specifically allowed, lookup will fail on persistent tokens.

        :param _db: Database session
        :param data_source_name: Name of the data source
        :param token_type: Token type
        :param patron: Patron object
        :param allow_persistent_token: Boolean value indicating whether persistent tokens are allowed or not
        :param auto_create_datasource: Boolean value indicating whether
            a data source should be created in the case it doesn't
        """
        from core.model.patron import Patron

        if not is_session(_db):
            raise ValueError('"_db" argument must be a valid SQLAlchemy session')
        if not isinstance(data_source_name, str) or not data_source_name:
            raise ValueError('"data_source_name" argument must be a non-empty string')
        if not isinstance(token_type, str) or not token_type:
            raise ValueError('"token_type" argument must be a non-empty string')
        if not isinstance(patron, Patron):
            raise ValueError('"patron" argument must be an instance of Patron class')
        if not isinstance(allow_persistent_token, bool):
            raise ValueError('"allow_persistent_token" argument must be boolean')
        if not isinstance(auto_create_datasource, bool):
            raise ValueError('"auto_create_datasource" argument must be boolean')

        from core.model.datasource import DataSource

        data_source = DataSource.lookup(
            _db, data_source_name, autocreate=auto_create_datasource
        )
        credential = get_one(
            _db, Credential, data_source=data_source, type=token_type, patron=patron
        )

        return (
            cls._filter_invalid_credential(credential, allow_persistent_token)
            if credential
            else None
        )

    @classmethod
    def lookup_and_expire_temporary_token(cls, _db, data_source, type, token):
        """Look up a temporary token and expire it immediately."""
        credential = cls.lookup_by_token(_db, data_source, type, token)
        if not credential:
            return None
        credential.expires = utc_now() - datetime.timedelta(seconds=5)
        return credential

    @classmethod
    def temporary_token_create(
        cls, _db, data_source, token_type, patron, duration, value=None
    ):
        """Create a temporary token for the given data_source/type/patron.
        The token will be good for the specified `duration`.
        """
        expires = utc_now() + duration
        token_string = value or str(uuid.uuid1())
        credential, is_new = get_one_or_create(
            _db, Credential, data_source=data_source, type=token_type, patron=patron
        )
        # If there was already a token of this type for this patron,
        # the new one overwrites the old one.
        credential.credential = token_string
        credential.expires = expires
        return credential, is_new

    @classmethod
    def persistent_token_create(
        self, _db, data_source, type, patron, token_string=None
    ):
        """Create or retrieve a persistent token for the given
        data_source/type/patron.
        """
        if token_string is None:
            token_string = str(uuid.uuid1())
        credential, is_new = get_one_or_create(
            _db,
            Credential,
            data_source=data_source,
            type=type,
            patron=patron,
            create_method_kwargs=dict(credential=token_string),
        )
        credential.expires = None
        return credential, is_new

    def __repr__(self):
        return (
            "<Credential("
            "data_source_id={}, "
            "patron_id={}, "
            "collection_id={}, "
            "type={}, "
            "credential={}, "
            "expires={}>)".format(
                self.data_source_id,
                self.patron_id,
                self.collection_id,
                self.type,
                self.credential,
                self.expires,
            )
        )
