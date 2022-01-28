#!/bin/bash

set -ex

# Container is passed as arg
container="$1"

# Source check command
dir=$(dirname "${BASH_SOURCE[0]}")
source "${dir}/check_service_status.sh"

# Wait for container to start
wait_for_runit "$container"

# Make sure that cron is running in the scripts container
check_service_status "$container" /etc/service/cron
exit 0
