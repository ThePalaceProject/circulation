#!/bin/bash

set -ex

# Wait for the container to start services before running tests
sleep 30;

dir=$(dirname "${BASH_SOURCE[0]}")
source "${dir}/check_service_status.sh"

# In a webapp container, check that nginx and uwsgi are running.
check_service_status /etc/service/nginx
check_service_status /home/simplified/service/uwsgi

# Make sure the web server is running.
healthcheck=$(curl --write-out "%{http_code}" --silent --output /dev/null http://localhost:8000/healthcheck.html)
if ! [[ ${healthcheck} == "200" ]]; then
  exit 1
else
  echo "  OK"
fi

# Also make sure the app server is running.
feed_type=$(curl --write-out "%{content_type}" --silent --output /dev/null http://localhost:8000/heartbeat)
if ! [[ ${feed_type} == "application/vnd.health+json" ]]; then
  exit 1
else
  echo "  OK"
fi

exit 0
