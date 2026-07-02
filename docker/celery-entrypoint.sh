#!/bin/bash
#
# Entrypoint for the circ-celery image.
#
# A single image backs every Celery process we run; the role is chosen by the
# first argument, so the same image can be deployed as the beat scheduler or any
# number of autoscaled worker pools:
#
#   celery-entrypoint.sh beat
#   celery-entrypoint.sh worker        (PALACE_CELERY_QUEUES required)
#
# Exactly one Celery process runs per container (no runit supervision); that is
# what lets the worker pools be scaled horizontally, one replica per unit of
# queue depth. Logs are written to stdout/stderr (as JSON, via the application's
# logging configuration) rather than to files, so nothing is lost when an
# autoscaled worker is scaled away.
#
# Queue-depth metrics (which drive worker autoscaling) are published by the
# periodic publish_queue_stats task on the beat schedule, so there is no separate
# always-on metrics process/role here.

set -euo pipefail

APP="palace.manager.celery.app"
CELERY="/var/www/circulation/env/bin/celery"

cd /var/www/circulation

role="${1:-}"
if [[ -z "$role" ]]; then
  echo "Usage: $(basename "$0") <beat|worker> [extra celery args]" >&2
  exit 64
fi
shift

case "$role" in
  beat)
    # Beat MUST run as a singleton -- a second replica would double-fire every
    # scheduled task. Never autoscale this role; pin it to a single instance.
    schedule_dir="/var/run/celery"
    mkdir -p "$schedule_dir"
    chown palace:palace "$schedule_dir"
    exec "$CELERY" -A "$APP" beat \
      --uid palace --gid palace \
      --schedule "$schedule_dir/beat-schedule" \
      "$@"
    ;;
  worker)
    # The queue set and concurrency are supplied per-deployment so one image can
    # back every worker pool. Concurrency is a single pool of child processes
    # shared across ALL queues this worker consumes -- to give a queue its own
    # concurrency, run a separate deployment with its own PALACE_CELERY_QUEUES
    # and PALACE_CELERY_CONCURRENCY.
    queues="${PALACE_CELERY_QUEUES:-}"
    if [[ -z "$queues" ]]; then
      echo "PALACE_CELERY_QUEUES is required for the worker role (comma-separated queue names)." >&2
      exit 64
    fi
    concurrency="${PALACE_CELERY_CONCURRENCY:-1}"
    hostname="${PALACE_CELERY_WORKER_HOSTNAME:-worker@%h}"
    exec "$CELERY" -A "$APP" worker \
      --uid palace --gid palace \
      --queues "$queues" \
      --concurrency "$concurrency" \
      --hostname "$hostname" \
      "$@"
    ;;
  *)
    echo "Unknown role '$role' (expected: beat or worker)." >&2
    exit 64
    ;;
esac
