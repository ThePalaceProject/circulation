from __future__ import annotations

import datetime

from sqlalchemy.orm import Session

from palace.manager.metadata_layer.frozen_data import BaseFrozenData
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
    checkouts_available: int
    expires: datetime.datetime | None = None
    checkouts_left: int | None = None
    terms_concurrency: int | None = None
    content_types: list[str] | None = None

    def add_to_pool(self, db: Session, pool: LicensePool) -> License:
        license_obj, _ = get_one_or_create(
            db,
            License,
            identifier=self.identifier,
            license_pool=pool,
        )
        for key, value in vars(self).items():
            if key != "content_types":
                setattr(license_obj, key, value)
        return license_obj
