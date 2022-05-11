from typing import TYPE_CHECKING, TypeVar

from sqlalchemy import Column, Enum, ForeignKey, Integer, Unicode
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import relationship

from . import Base

if TYPE_CHECKING:
    from core.model.patron import Patron


class DeviceTokenTypes:
    FCM_ANDROID = "FCMAndroid"
    FCM_IOS = "FCMiOS"


T = TypeVar("T")


class DeviceToken(Base):
    """Meant to store patron device tokens
    Currently the only use case is mobile FCM tokens"""

    __tablename__ = "devicetokens"

    id = Column("id", Integer, primary_key=True)
    patron_id = Column(Integer, ForeignKey("patrons.id"), index=True, nullable=False)
    patron = relationship("Patron", backref="device_tokens", cascade="delete")

    token_type_enum = Enum(
        DeviceTokenTypes.FCM_ANDROID, DeviceTokenTypes.FCM_IOS, name="token_types"
    )
    token_type = Column(token_type_enum, nullable=False)

    device_token = Column(Unicode, nullable=False, unique=True, index=True)

    @classmethod
    def create(
        cls: type[T], db, token_type: str, device_token: str, patron: "Patron|int"
    ) -> T:
        """Create a DeviceToken while ensuring sql issues are managed.
        Raises InvalidTokenTypeError, DuplicateDeviceTokenError"""

        if token_type not in DeviceToken.token_type_enum.enums:
            raise InvalidTokenTypeError(token_type)

        kwargs = dict(device_token=device_token, token_type=token_type)
        if type(patron) is int:
            kwargs["patron_id"] = patron
        else:
            kwargs["patron_id"] = patron.id

        device = DeviceToken(**kwargs)
        try:
            db.add(device)
            db.commit()
        except IntegrityError as e:
            if "device_token" in e.args[0]:
                raise DuplicateDeviceTokenError()
            else:
                raise

        return device


class InvalidTokenTypeError(Exception):
    pass


class DuplicateDeviceTokenError(Exception):
    pass
