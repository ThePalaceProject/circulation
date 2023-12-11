#!/bin/bash

set -ex

# Container is passed as arg
container="$1"

# Source check command
dir=$(dirname "${BASH_SOURCE[0]}")
source "${dir}/check_service_status.sh"

# Wait for container to start
wait_for_runit "$container"

# Make sure database initialization completed successfully
timeout 240s grep -q 'Initialization complete' <(docker compose logs "$container" -f 2>&1)

# Make sure that cron is running in the scripts container
check_service_status "$container" /etc/service/cron

# Ensure the installed crontab has no problems
check_crontab "$container"

# Run a single script to ensure basic settings are correct
# The opds2 import script will only test the DB configuration
run_script "$container" "source ../env/bin/activate && ./opds2_import_monitor"
exit 0
