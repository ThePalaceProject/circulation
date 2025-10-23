from __future__ import annotations

import datetime

from pydantic import NonNegativeInt
from sqlalchemy.orm import Session

from palace.manager.data_layer.base.frozen import BaseFrozenData
from palace.manager.opds.odl.info import LicenseStatus
from palace.manager.sqlalchemy.model.licensing import (
    License,
    LicenseFunctions,
    LicensePool,
)
from palace.manager.sqlalchemy.util import get_one_or_create


class LicenseData(BaseFrozenData, LicenseFunctions):
    identifier: str
    checkout_url: str | None
    status_url: str
    status: LicenseStatus
    checkouts_available: NonNegativeInt
    expires: datetime.datetime | None = None
    checkouts_left: int | None = None
    terms_concurrency: int | None = None
    content_types: tuple[str, ...] = tuple()

    def add_to_pool(self, db: Session, pool: LicensePool) -> License:
        kwargs = {
            key: value
            for key, value in vars(self).items()
            if key not in ["content_types", "identifier"]
        }

        license_obj, _ = get_one_or_create(
            db,
            License,
            identifier=self.identifier,
            license_pool=pool,
            create_method_kwargs=kwargs,
        )
        for key, value in kwargs.items():
            if getattr(license_obj, key) != value:
                setattr(license_obj, key, value)
        return license_obj
