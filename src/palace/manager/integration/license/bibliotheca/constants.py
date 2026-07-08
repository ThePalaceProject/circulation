from __future__ import annotations

from palace.manager.sqlalchemy.constants import DataSourceConstants

BIBLIOTHECA_LABEL = DataSourceConstants.BIBLIOTHECA

# The name we use to identify Bibliotheca as the remote service, in self-test
# output and in the RemoteInitiatedServerErrors we raise against it.
BIBLIOTHECA_SERVICE_NAME = "Bibliotheca"

# The format Bibliotheca uses for datetimes, both for the arguments it expects
# on requests and for the timestamps in the documents it returns.
BIBLIOTHECA_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
