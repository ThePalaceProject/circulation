from sqlalchemy import Column, DateTime, ForeignKey, Integer, Unicode

from core.util.datetime_helpers import utc_now

from . import Base


class ExternalIntegrationError(Base):
    __tablename__ = "externalintegrationerrors"

    id = Column(Integer, primary_key=True)
    time = Column(DateTime, default=utc_now)
    error = Column(Unicode)
    external_integration_id = Column(
        Integer,
        ForeignKey("externalintegrations.id", name="fk_error_externalintegrations_id"),
    )


class ExternalIntegrationStatus(Base):
    __tablename__ = "externalintegrationstatuses"

    id = Column(Integer, primary_key=True)
    last_updated = Column(DateTime)
    status = Column(Integer)
    external_integration_id = Column(
        Integer,
        ForeignKey("externalintegrations.id", name="fk_status_externalintegrations_id"),
    )
