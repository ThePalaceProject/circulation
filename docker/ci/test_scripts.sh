#!/bin/bash

set -ex

# Container is passed as arg
container="$1"

# Source check command
dir=$(dirname "${BASH_SOURCE[0]}")
source "${dir}/check_service_status.sh"

# Output the version file for debugging
docker compose exec "$container" cat /var/www/circulation/src/palace/manager/_version.py

# Wait for container to start
wait_for_runit "$container"

# Make sure database initialization completed successfully
timeout 240s grep -q -e 'Initialization complete' -e "Migrations complete" <(docker compose logs "$container" -f 2>&1)

# Make sure that cron is running in the scripts container
check_service_status "$container" /etc/service/cron

# Make sure that the celery worker is running in the scripts container
check_service_status "$container" /etc/service/worker-default
check_service_status "$container" /etc/service/worker-high
check_service_status "$container" /etc/service/beat
check_service_status "$container" /etc/service/celery-cloudwatch

# Ensure the installed crontab has no problems
check_crontab "$container"

# Run a single script to ensure basic settings are correct
# The opds2 import script will only test the DB configuration
run_script "$container" "source ../env/bin/activate && ./opds2_import_monitor"
exit 0
