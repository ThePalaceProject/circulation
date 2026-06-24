#!/bin/bash
# Stores Circulation Manager environment variables in the virtualenv environment.
# Subprocesses (Celery workers, CLI scripts) don't inherit the container's
# runtime environment, so variables set at runtime need to be stored.

set -e

SIMPLIFIED_ENVIRONMENT=/var/www/circulation/environment.sh

# Make sure there's a file to put environment variables into
touch $SIMPLIFIED_ENVIRONMENT

# Move all of the environment variables with Library Simplified prefixes
# into an environment file. The virtualenv's activate script sources this file,
# so the environment is loaded whenever a Celery worker or CLI script runs.
# The values of the variables are escaped as needed for the shell.
for var in $(printenv | grep -e SIMPLIFIED -e LIBSIMPLE -e PALACE | sed -e 's/^\([^=]*\)=.*$/\1/g'); do {
  printf "export ${var}=%q\n" "$(printenv "${var}")"
} done > $SIMPLIFIED_ENVIRONMENT

# Give it to the appropriate user.
chown palace:palace $SIMPLIFIED_ENVIRONMENT
