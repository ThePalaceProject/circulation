#!/bin/bash

set -ex

# Container is passed as arg
container="$1"

# Source check command
dir=$(dirname "${BASH_SOURCE[0]}")
source "${dir}/check_service_status.sh"

# Wait for container to start
wait_for_runit "$container"

# Wait for us to be able to connect to the webapp.
docker exec "$container" curl --retry 15 --retry-delay 5 --retry-connrefused -4 --output /dev/null http://localhost/

# In a webapp container, check that uwsgi is running.
check_service_status "$container" /etc/service/uwsgi

# Make sure the web server is running.
healthcheck=$(docker exec "$container" curl --write-out "%{http_code}" --silent --output /dev/null http://localhost/healthcheck.html)
if ! [[ ${healthcheck} == "200" ]]; then
  exit 1
else
  echo "  OK"
fi

# Also make sure the app server is running.
feed_type=$(docker exec "$container" curl --write-out "%{content_type}" --silent --output /dev/null http://localhost/version.json)
if ! [[ ${feed_type} == "application/json" ]]; then
  exit 1
else
  echo "  OK"
fi

exit 0
