#!/bin/bash

# This script makes sure that our database migrations bring the database up to date
# so that the resulting database is the same as if we had initialized a new instance.
#
# This is done by:
# (1) Finding the id of the first DB migration.
# (2) Initializing the database with an old version of the app. This old version is
#     the version of the app that was current when the first migration was created.
#     This version is started in a separate container called `webapp-old`. This
#     container is defined in the `test_migrations.yml` file.
# (3) Then the current version of the app is started in a container called `webapp`.
# (4) We run the migrations in the `webapp` container to bring the database up to date.
#     and then check that the database schema matches the model.
# (5) We then run the downgrade migrations in the `webapp` container to bring the database
#     back to the state it was in when the first migration was created.
# (6) We then check that the database schema matches the model in the `webapp-old` container.
# (7) Finally, we repeat step (4) to make sure that the up migrations stay in sync.
#
# After the migrations are complete in step (4) and (6) we use `alembic check` [1] to
# make sure that the database model matches the migrated database. If the model matches,
# then the database is in sync and the migrations are up to date. If the database doesn't
# match then there is a problem with the migrations and the script will fail.
#
# Note: This test cannot be added to the normal migration test suite since it requires
# us to have access to an older version of our code base. To facilitate this we use the
# `test_migrations.yml` file to define a container that runs an older version of the app.
# And run all the commands in this script in a docker-compose environment.
#
# [1] https://alembic.sqlalchemy.org/en/latest/autogenerate.html#running-alembic-check-to-test-for-new-upgrade-operations

# Text colors
RESET='\033[0m'       # Text Reset
GREEN='\033[1;32m'       # Green

# Keeps track of whether we are in a group or not
IN_GROUP=0

# Allow a command to run without echoing its output
DEBUG_ECHO_ENABLED=1

# Functions to interact with GitHub Actions
# https://docs.github.com/en/actions/reference/workflow-commands-for-github-actions
gh_command() {
  local COMMAND=$1
  local MESSAGE=${2:-""}
  echo "::${COMMAND}::${MESSAGE}"
}

# Create a group of log lines
# https://docs.github.com/en/actions/using-workflows/workflow-commands-for-github-actions#grouping-log-lines
gh_group() {
  local MESSAGE=$1
  gh_command group "$MESSAGE"
  IN_GROUP=1
}

# End a group of log lines
gh_endgroup() {
  if [[ $IN_GROUP -eq 1 ]]; then
    gh_command endgroup
    IN_GROUP=0
  fi
}

# Log an error message
# Note: if this is called in a group, the group will be closed before the error message is logged.
gh_error() {
  gh_endgroup
  local MESSAGE=$1
  gh_command error "$MESSAGE"
}

# Log a success message
# Note: if this is called in a group, the group will be closed before the success message is logged.
success() {
  gh_endgroup
  local MESSAGE=$1
  echo -e "${GREEN}Success:${RESET} ${MESSAGE}"
}

debug_echo() {
  if [[ $DEBUG_ECHO_ENABLED -eq 1 ]]; then
    printf "%q " "$@"
    printf "\n"
  fi
}

# Run a docker-compose command
compose_cmd() {
  args=(docker compose -f docker-compose.yml -f docker/ci/test_migrations.yml --progress quiet)
  debug_echo "++" "${args[@]}" "$@"
  "${args[@]}" "$@"
}

# Run a command in a particular container using docker-compose
# The command is run in a bash shell with the palace virtualenv activated
run_in_container()
{
  local CONTAINER_NAME=$1
  shift 1
  debug_echo "+" "$@"
  compose_cmd run --build --rm --no-deps "${CONTAINER_NAME}" /bin/bash -c "source env/bin/activate && $*"
}

# Cleanup any running containers
cleanup() {
  compose_cmd down
}

# Cleanup any running containers and exit with an error message
error_and_cleanup() {
  local MESSAGE=$1
  local EXIT_CODE=$2
  cleanup
  gh_error "$MESSAGE"
  exit "$EXIT_CODE"
}

# Run an alembic migration command in a container
run_migrations() {
  local CONTAINER_NAME=$1
  shift 1
  run_in_container "${CONTAINER_NAME}" "alembic" "$@"
  exit_code=$?
  if [[ $exit_code -ne 0 ]]; then
    error_and_cleanup "Running database migrations failed." $exit_code
  fi
}

# Check if the database is in sync with the model
check_db() {
  local CONTAINER_NAME=$1
  local DETAILED_ERROR=$2
  run_in_container "${CONTAINER_NAME}" alembic check
  local exit_code=$?
  if [[ $exit_code -ne 0 ]]; then
    error_and_cleanup "Database is out of sync! ${DETAILED_ERROR}" $exit_code
  fi
  success "Database is in sync."
}

# Find all the info we need about the first migration in the git history.
gh_group "Finding first migration"
run_in_container "webapp" alembic history -r'base:base+1' -v
# Debug echo is disabled since we are capturing the output of the command
DEBUG_ECHO_ENABLED=0
first_migration_id=$(run_in_container "webapp" alembic history -r'base:base+1' -v | head -n 1 | cut -d ' ' -f2)
DEBUG_ECHO_ENABLED=1
if [[ -z $first_migration_id ]]; then
  error_and_cleanup "Could not find first migration id." 1
fi

first_migration_file=$(find alembic/versions -name "*${first_migration_id}*.py")
if [[ -z $first_migration_file ]]; then
  error_and_cleanup "Could not find first migration file." 1
fi

first_migration_commit=$(git log --follow --format=%H --reverse "${first_migration_file}" | head -n 1)
if [[ -z $first_migration_commit ]]; then
  error_and_cleanup "Could not find first migration commit hash." 1
fi
first_migration_container="ghcr.io/thepalaceproject/circ-webapp:sha-${first_migration_commit:0:7}"
echo "First migration info:"
echo "  id: ${first_migration_id}"
echo "  file: ${first_migration_file}"
echo "  commit: ${first_migration_commit}"
echo "  container: ${first_migration_container}"

container_image=$(sed -n 's/^ *image: "\(.*\)"/\1/p' docker/ci/test_migrations.yml)
if [[ -z $container_image ]]; then
  error_and_cleanup "Could not find container image in test_migrations.yml" 1
fi

if [[ "$container_image" != "$first_migration_container" ]]; then
  error_and_cleanup "Incorrect container image in test_migrations.yml. Please update." 1
fi
gh_endgroup

gh_group "Starting service containers"
compose_cmd down
compose_cmd up -d pg os
gh_endgroup

gh_group "Initializing database"
run_in_container "webapp-old" "./bin/util/initialize_instance"
initialize_exit_code=$?
if [[ $initialize_exit_code -ne 0 ]]; then
  error_and_cleanup "Failed to initialize instance." $initialize_exit_code
fi
gh_endgroup

# Migrate up to the current commit and check if the database is in sync
gh_group "Testing upgrade migrations"
run_migrations "webapp" upgrade head
check_db "webapp" "A new migration is required or an up migration is broken."
gh_endgroup

# Migrate down to the first migration and check if the database is in sync
gh_group "Testing downgrade migrations"
run_migrations "webapp" downgrade "${first_migration_id}"
check_db "webapp-old" "A down migration is broken."
gh_endgroup

# Migrate back up once more to make sure that the database is still in sync
gh_group "Testing upgrade migrations a second time"
run_migrations "webapp" upgrade head
check_db "webapp" "An up migration is likely broken."
gh_endgroup

echo ""
success "All migrations are up to date 🎉"

gh_group "Shutting down service containers"
cleanup
gh_endgroup
