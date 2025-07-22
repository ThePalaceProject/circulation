from __future__ import annotations

import sys
from typing import NamedTuple

from bidict import frozenbidict
from frozendict import frozendict

from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism
from palace.manager.sqlalchemy.model.resource import Representation

# TODO: Remove this when we drop support for Python 3.10
if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from backports.strenum import StrEnum


class BoundlessFormat(StrEnum):
    axis_now = "AxisNow"
    # Legacy format, handled the same way as AxisNow
    blio = "Blio"
    epub = "ePub"
    pdf = "PDF"
    acoustik = "Acoustik"


class ServerNickname(StrEnum):
    production = "Production"
    qa = "QA"


API_BASE_URLS = frozendict(
    {
        ServerNickname.production: "https://axis360api.baker-taylor.com/Services/VendorAPI/",
        ServerNickname.qa: "https://axis360apiqa.baker-taylor.com/Services/VendorAPI/",
    }
)

LICENSE_SERVER_BASE_URLS = frozendict(
    {
        ServerNickname.production: "https://frontdoor.axisnow.com/",
        ServerNickname.qa: "https://qa-frontdoor.axisnow.com/",
    }
)


class DeliveryMechanismTuple(NamedTuple):
    content_type: str | None
    drm_scheme: str | None


DELIVERY_MECHANISM_TO_INTERNAL_FORMAT: frozenbidict[DeliveryMechanismTuple, str] = (
    frozenbidict(
        {
            DeliveryMechanismTuple(
                Representation.EPUB_MEDIA_TYPE,
                DeliveryMechanism.ADOBE_DRM,
            ): BoundlessFormat.epub,
            DeliveryMechanismTuple(
                Representation.PDF_MEDIA_TYPE,
                DeliveryMechanism.ADOBE_DRM,
            ): BoundlessFormat.pdf,
            DeliveryMechanismTuple(
                None,
                DeliveryMechanism.FINDAWAY_DRM,
            ): BoundlessFormat.acoustik,
            DeliveryMechanismTuple(
                Representation.EPUB_MEDIA_TYPE,
                DeliveryMechanism.BAKER_TAYLOR_KDRM_DRM,
            ): BoundlessFormat.axis_now,
        }
    )
)

INTERNAL_FORMAT_TO_DELIVERY_MECHANISM = DELIVERY_MECHANISM_TO_INTERNAL_FORMAT.inverse

BAKER_TAYLOR_KDRM_PARAMS = ("modulus", "exponent", "device_id")
