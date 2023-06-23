#!/bin/bash
# Manages the Circulation Manager database (either initializing it, migrating
# it, or ignoring it) when the container starts and before the app launches.

set -e

WORKDIR=/var/www/circulation
BINDIR=$WORKDIR/bin
CORE_BINDIR=$WORKDIR/core/bin

initialization_task="${BINDIR}/util/initialize_instance"
migration_logfile="/var/log/migrate.log"

su simplified <<EOF 2>&1 | tee -a ${migration_logfile}
# Default value 'ignore' does nothing.

echo "-- Begin Migrate --"
date

# Enter the virtual environment for the application.
source $WORKDIR/env/bin/activate;

${initialization_task};

EOF
