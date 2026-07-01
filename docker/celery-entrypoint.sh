#!/bin/bash
#
# Entrypoint for the circ-celery image.
#
# A single image backs every Celery process we run; the role is chosen by the
# first argument, so the same image can be deployed as the beat scheduler, any
# number of autoscaled worker pools, or the CloudWatch metrics camera:
#
#   celery-entrypoint.sh beat
#   celery-entrypoint.sh worker        (PALACE_CELERY_QUEUES required)
#   celery-entrypoint.sh cloudwatch
#
# Exactly one Celery process runs per container (no runit supervision); that is
# what lets the worker pools be scaled horizontally, one replica per unit of
# queue depth. Logs are written to stdout/stderr (as JSON, via the application's
# logging configuration) rather than to files, so nothing is lost when an
# autoscaled worker is scaled away.

set -euo pipefail

APP="palace.manager.celery.app"
CELERY="/var/www/circulation/env/bin/celery"

cd /var/www/circulation

role="${1:-}"
if [[ -z "$role" ]]; then
  echo "Usage: $(basename "$0") <beat|worker|cloudwatch> [extra celery args]" >&2
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
  cloudwatch)
    # Publishes the per-queue depth (QueueWaiting) and oldest-age
    # (QueueOldestAge) metrics that drive worker autoscaling. A single replica
    # snapshots the whole broker, so this is a singleton as well.
    flush="${PALACE_CELERY_CLOUDWATCH_FLUSH_INTERVAL:-60}"
    exec "$CELERY" -A "$APP" events \
      -c "palace.manager.celery.monitoring.Cloudwatch" \
      -F "$flush" \
      --uid palace --gid palace \
      "$@"
    ;;
  *)
    echo "Unknown role '$role' (expected: beat, worker, or cloudwatch)." >&2
    exit 64
    ;;
esac
