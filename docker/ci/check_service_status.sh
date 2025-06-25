# Wait for runit to start in container
function wait_for_runit()
{
  # The container to run the command in
  container="$1"

  timeout 120s grep -q 'Runit started' <(docker compose logs "$container" -f 2>&1)
}

# A method to check that runit services are running inside the container
function check_service_status()
{
  # The container to run the command in
  container="$1"

  # The location of the runit service should be passed.
  service="$2"

  # Wait up to 15 seconds for the service file to exist
  timeout --foreground 15s docker compose exec "$container" bash -c "until test -e $service; do sleep 1; done"
  file_status=$?
  if [[ "$file_status" != 0 ]]; then
    echo "  FAIL: $service file does not exist after waiting"
    exit 1
  fi

  # Check the status of the service.
  service_status=$(docker compose exec "$container" /bin/bash -c "sv check $service")

  # Get the exit code for the sv call.
  sv_status=$?

  if [[ "$sv_status" != 0 || "$service_status" =~ down ]]; then
    echo "  FAIL: $service is not running"
    exit 1
  else
    echo "  OK"
  fi
}

function check_crontab() {
  container="$1"

  # Installing the crontab will reveal any errors and exit with an error code
  $(docker compose exec "$container" /bin/bash -c "crontab /etc/cron.d/circulation")
  validate_status=$?
  if [[ "$validate_status" != 0 ]]; then
    echo "  FAIL: crontab is incorrect"
    exit 1
  else
    echo "  OK"
  fi
}

function run_script() {
  container="$1"
  script="$2"

  output=$(docker compose exec "$container" /bin/bash -c "$script")
  script_status=$?
  if [[ "$script_status" != 0 ]]; then
    echo "  FAIL: script run failed"
    exit 1
  else
    echo "  OK"
  fi
}
