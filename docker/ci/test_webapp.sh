#!/bin/bash

set -ex

# Container is passed as arg
container="$1"

# Source check command
dir=$(dirname "${BASH_SOURCE[0]}")
source "${dir}/check_service_status.sh"

# Wait for container to start
wait_for_runit "$container"

# In a webapp container, check that nginx and uwsgi are running.
check_service_status "$container" /etc/service/nginx
check_service_status "$container" /etc/service/uwsgi

# Wait for UWSGI to be ready to accept connections.
timeout 240s grep -q 'WSGI app .* ready in [0-9]* seconds' <(docker compose logs "$container" -f 2>&1)

# Make sure the web server is running.
healthcheck=$(docker compose exec "$container" curl --write-out "%{http_code}" --silent --output /dev/null http://localhost/healthcheck.html)
if ! [[ ${healthcheck} == "200" ]]; then
  exit 1
else
  echo "  OK"
fi

# Also make sure the app server is running.
feed_type=$(docker compose exec "$container" curl --write-out "%{content_type}" --silent --output /dev/null http://localhost/version.json)
if ! [[ ${feed_type} == "application/json" ]]; then
  exit 1
else
  echo "  OK"
fi

exit 0
