import sys

from sqlalchemy import Column, Enum, ForeignKey, Index, Integer, Unicode
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapped, backref, relationship

from core.model import Base
from core.model.patron import Patron

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class DeviceTokenTypes:
    FCM_ANDROID = "FCMAndroid"
    FCM_IOS = "FCMiOS"


class DeviceToken(Base):
    """Meant to store patron device tokens
    Currently the only use case is mobile FCM tokens"""

    __tablename__ = "devicetokens"

    id = Column("id", Integer, primary_key=True)
    patron_id = Column(
        Integer,
        ForeignKey("patrons.id", ondelete="CASCADE", name="devicetokens_patron_fkey"),
        index=True,
        nullable=False,
    )
    patron: Mapped[Patron] = relationship(
        "Patron", backref=backref("device_tokens", passive_deletes=True)
    )

    token_type_enum = Enum(
        DeviceTokenTypes.FCM_ANDROID, DeviceTokenTypes.FCM_IOS, name="token_types"
    )
    token_type = Column(token_type_enum, nullable=False)

    device_token = Column(Unicode, nullable=False, index=True)

    __table_args__ = (
        Index(
            "ix_devicetokens_device_token_patron", device_token, patron_id, unique=True
        ),
    )

    @classmethod
    def create(
        cls,
        db,
        token_type: str,
        device_token: str,
        patron: Patron | int,
    ) -> Self:
        """Create a DeviceToken while ensuring sql issues are managed.
        Raises InvalidTokenTypeError, DuplicateDeviceTokenError"""

        if token_type not in [DeviceTokenTypes.FCM_ANDROID, DeviceTokenTypes.FCM_IOS]:
            raise InvalidTokenTypeError(token_type)

        kwargs: dict = dict(device_token=device_token, token_type=token_type)
        if type(patron) is int:
            kwargs["patron_id"] = patron
        elif type(patron) is Patron:
            kwargs["patron_id"] = patron.id

        device = cls(**kwargs)
        try:
            db.add(device)
            db.commit()
        except IntegrityError as e:
            db.rollback()
            if "device_token" in e.args[0]:
                raise DuplicateDeviceTokenError() from e
            else:
                raise

        return device


class InvalidTokenTypeError(Exception):
    pass


class DuplicateDeviceTokenError(Exception):
    pass
