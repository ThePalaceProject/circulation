#!/bin/bash

# This script makes sure that our database migrations bring the database up to date
# so that the resulting database is the same as if we had initialized a new instance.
#
# This is done by checking out the an older version of our codebase. The commit when
# the first migration was added and initializing a new instance. Then we check out
# the current version of our codebase and run the migrations. If the database is in
# sync, then the migrations are up to date. If the database is out of sync, then
# a new migration is required.
#
# This test is cannot be added to the normal migration test suite since it requires
# manipulating the git history and checking out older versions of the codebase.
#
# All of the commands in this script are run inside a docker-compose environment.


compose_cmd() {
  docker --log-level ERROR compose --progress quiet "$@"
}

run_in_container()
{
  compose_cmd run --build --rm webapp /bin/bash -c "source env/bin/activate && $*"
}

if ! git diff --quiet; then
  echo "ERROR: You have uncommitted changes. These changes will be lost if you run this script."
  echo "  Please commit or stash your changes and try again."
  exit 1
fi

# Find the currently checked out branch
current_branch=$(git symbolic-ref -q --short HEAD)
current_branch_exit_code=$?

# If we are not on a branch, then we are in a detached HEAD state, so
# we use the commit hash instead. This happens in CI when being run
# against a PR instead of a branch.
# See: https://stackoverflow.com/questions/69935511/how-do-i-save-the-current-head-so-i-can-check-it-back-out-in-the-same-way-later
if [[ $current_branch_exit_code -ne 0 ]]; then
  current_branch=$(git rev-parse HEAD)
  echo "WARNING: You are in a detached HEAD state. This is normal when running in CI."
  echo "  The current commit hash will be used instead of a branch name."
fi

echo "Current branch: ${current_branch}"

# Find the first migration file
first_migration_id=$(run_in_container alembic history -r'base:base+1' -v | head -n 1 | cut -d ' ' -f2)
if [[ -z $first_migration_id ]]; then
  echo "ERROR: Could not find first migration."
  exit 1
fi

first_migration_file=$(find alembic/versions -name "*${first_migration_id}*.py")
if [[ -z $first_migration_file ]]; then
  echo "ERROR: Could not find first migration file."
  exit 1
fi

echo "First migration file: ${first_migration_file}"
echo ""

# Find the git commit where the first migration file was added
first_migration_commit=$(git log --follow --format=%H --reverse "${first_migration_file}" | head -n 1)

echo "Starting containers and initializing database at commit ${first_migration_commit}"
git checkout -q "${first_migration_commit}"
compose_cmd down
compose_cmd up -d pg
run_in_container "./bin/util/initialize_instance"
echo ""

# Migrate up to the current commit and check if the database is in sync
git checkout -q "${current_branch}"
echo "Running database migrations on branch ${current_branch}"
run_in_container "alembic upgrade head"
exit_code=$?
if [[ $exit_code -ne 0 ]]; then
  echo "ERROR: Database migration failed."
  exit $exit_code
fi
echo ""

echo "Checking database status"
run_in_container "alembic check"
exit_code=$?
echo ""

if [[ $exit_code -eq 0 ]]; then
  echo "SUCCESS: Database is in sync."
else
  echo "ERROR: Database is out of sync. A new migration is required."
fi

# Stop containers
compose_cmd down

exit $exit_code
