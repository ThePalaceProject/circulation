# Wait for runit to start in container
function wait_for_runit()
{
  # The container to run the command in
  container="$1"

  timeout 120s grep -q 'Runit started' <(docker logs "$container" -f 2>&1)
}

# A method to check that runit services are running inside the container
function check_service_status()
{
  # The container to run the command in
  container="$1"

  # The location of the runit service should be passed.
  service="$2"

  # Check the status of the service.
  service_status=$(docker exec "$container" /bin/bash -c "sv check $service")

  # Get the exit code for the sv call.
  sv_status=$?

  if [[ "$sv_status" != 0 || "$service_status" =~ down ]]; then
    echo "  FAIL: $service is not running"
    exit 1
  else
    echo "  OK"
  fi
}
