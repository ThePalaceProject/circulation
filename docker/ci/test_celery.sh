#!/bin/bash

# Smoke test for the circ-celery image. Unlike the scripts image, circ-celery
# runs a single Celery process per container (no runit), so we can't use the
# `sv check` helpers. Instead we confirm the worker answers a ping over the
# broker and that the beat and cloudwatch containers come up and stay running.

set -ex

CELERY="/var/www/circulation/env/bin/celery"
APP="palace.manager.celery.app"

# Assert a compose service's container is running (has not exited).
function assert_running() {
  service="$1"
  cid=$(docker compose ps -q "$service")
  if [[ -z "$cid" ]]; then
    echo "  FAIL: $service has no container"
    exit 1
  fi
  running=$(docker inspect -f '{{.State.Running}}' "$cid")
  if [[ "$running" != "true" ]]; then
    echo "  FAIL: $service is not running (State.Running=$running)"
    docker compose logs "$service"
    exit 1
  fi
  echo "  OK: $service is running"
}

# Output the version file for debugging.
docker compose exec -T celery-worker cat /var/www/circulation/src/palace/manager/_version.py

# Wait for the worker to come up and answer a ping over the broker. `inspect
# ping` only returns successfully once a worker has connected and is ready, so
# this exercises the whole path: image -> entrypoint -> celery -> broker.
timeout 120s bash -c "
  until docker compose exec -T celery-worker $CELERY -A $APP inspect ping --timeout 5; do
    sleep 5
  done
"

# Beat and the cloudwatch camera have no ping; confirm they started and are
# still running (i.e. the entrypoint launched the right process and it did not
# immediately exit).
assert_running celery-beat
assert_running celery-cloudwatch
assert_running celery-worker

# Confirm beat has actually started its scheduler loop.
timeout 60s grep -q -e 'beat: Starting' -e 'Scheduler:' <(docker compose logs celery-beat -f 2>&1)

exit 0
