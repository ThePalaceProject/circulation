# autoflake: skip_file
"""
We rely on all of our sqlalchemy models being listed here, so that we can
make sure they are all registered with the declarative base.
This is necessary to make sure that all of our models are properly reflected in
the database when we run migrations or create a new database.
"""

import palace.manager.sqlalchemy.model.admin
import palace.manager.sqlalchemy.model.announcements
import palace.manager.sqlalchemy.model.base
import palace.manager.sqlalchemy.model.circulationevent
import palace.manager.sqlalchemy.model.classification
import palace.manager.sqlalchemy.model.collection
import palace.manager.sqlalchemy.model.contributor
import palace.manager.sqlalchemy.model.coverage
import palace.manager.sqlalchemy.model.credential
import palace.manager.sqlalchemy.model.customlist
import palace.manager.sqlalchemy.model.datasource
import palace.manager.sqlalchemy.model.devicetokens
import palace.manager.sqlalchemy.model.discovery_service_registration
import palace.manager.sqlalchemy.model.edition
import palace.manager.sqlalchemy.model.identifier
import palace.manager.sqlalchemy.model.integration
import palace.manager.sqlalchemy.model.key
import palace.manager.sqlalchemy.model.lane
import palace.manager.sqlalchemy.model.library
import palace.manager.sqlalchemy.model.licensing
import palace.manager.sqlalchemy.model.marcfile
import palace.manager.sqlalchemy.model.measurement
import palace.manager.sqlalchemy.model.patron
import palace.manager.sqlalchemy.model.resource
import palace.manager.sqlalchemy.model.saml
import palace.manager.sqlalchemy.model.startup_task
import palace.manager.sqlalchemy.model.time_tracking
import palace.manager.sqlalchemy.model.work
