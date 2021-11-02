#!/bin/bash

set -ex

# Wait for the container to start services before running tests
sleep 30;

dir=$(dirname "${BASH_SOURCE[0]}")
source "${dir}/check_service_status.sh"

# Make sure that cron is running in the scripts container
check_service_status /etc/service/cron
exit 0
