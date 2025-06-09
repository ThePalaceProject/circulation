from __future__ import annotations


class Axis360APIConstants:
    VERIFY_SSL = "verify_certificate"
    PRODUCTION_BASE_URL = "https://axis360api.baker-taylor.com/Services/VendorAPI/"
    QA_BASE_URL = "http://axis360apiqa.baker-taylor.com/Services/VendorAPI/"
    SERVER_NICKNAMES = {
        "production": PRODUCTION_BASE_URL,
        "qa": QA_BASE_URL,
    }

    # The name Axis 360 gives to its web interface. We use it as the
    # name for the underlying access control system.
    AXISNOW = "AxisNow"
    BLIO = "Blio"
