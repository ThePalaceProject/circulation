#!/bin/bash

# This script checks that the PREVIOUS released version of the application still works
# against the database schema produced by the CURRENT code. It exercises our online
# migration requirement: a release's database must keep working with version N-1 of the
# code, because during a deploy the new migrations run while the previous version's
# webservers are still serving traffic.
#
# It works by:
#  (1) Finding the previous release (the latest GitHub release) and its published
#      circ-webapp image.
#  (2) Confirming that image contains the external-schema test seam. The seam ships in a
#      later release than this check itself, so until a release that has it becomes the
#      previous release there is nothing to test and we skip.
#  (3) Initializing a fresh database with the CURRENT image, which builds the new schema.
#  (4) Running the previous release's database tests (pytest -m db) against that schema in
#      external-schema mode (PALACE_TEST_DATABASE_EXTERNAL_SCHEMA), so the older code
#      exercises the new schema. If those tests fail, the migration is not backwards
#      compatible.
#
# The current image is provided via the WEBAPP_IMAGE environment variable (as in the other
# docker CI jobs). The previous release image can be overridden via PREV_RELEASE_IMAGE
# (useful for running this script locally).

set -uo pipefail

COMPOSE_FILES=(-f docker-compose.yml -f docker/ci/test_backwards_compatibility.yml)

# Run a docker compose command for this check.
compose_cmd() {
  docker compose "${COMPOSE_FILES[@]}" --progress quiet "$@"
}

# Run a command in a container with the palace virtualenv activated.
run_in_container() {
  local container="$1"
  shift
  compose_cmd run --rm --no-deps "${container}" /bin/bash -c "source env/bin/activate && $*"
}

cleanup() {
  compose_cmd down --remove-orphans >/dev/null 2>&1
}

fail() {
  echo "::error::$1"
  exit "${2:-1}"
}

# (1) Resolve the previous release image.
if [[ -z "${PREV_RELEASE_IMAGE:-}" ]]; then
  # In CI gh infers the repo from $GITHUB_REPOSITORY; locally it infers it from the git
  # remote of the current directory.
  if [[ -n "${GITHUB_REPOSITORY:-}" ]]; then
    prev_version="$(gh release view --repo "${GITHUB_REPOSITORY}" --json tagName --jq '.tagName' 2>/dev/null | sed 's/^v//')"
  else
    prev_version="$(gh release view --json tagName --jq '.tagName' 2>/dev/null | sed 's/^v//')"
  fi
  if [[ -z "${prev_version}" ]]; then
    echo "No previous GitHub release found; nothing to check. Skipping."
    exit 0
  fi
  PREV_RELEASE_IMAGE="ghcr.io/thepalaceproject/circ-webapp:${prev_version}"
fi
export PREV_RELEASE_IMAGE
echo "Previous release image: ${PREV_RELEASE_IMAGE}"

if [[ -z "${WEBAPP_IMAGE:-}" ]]; then
  fail "WEBAPP_IMAGE is not set; it must point at the current build's webapp image."
fi

trap cleanup EXIT

# (2) Make sure the previous release actually includes the external-schema test seam. It
# ships in a later release than this check, so older images do not support it yet and there
# is nothing meaningful to test.
compose_cmd pull --quiet webapp-prev || fail "Could not pull ${PREV_RELEASE_IMAGE}."
if ! run_in_container webapp-prev "grep -q 'external_schema' tests/fixtures/database.py"; then
  echo "Previous release image does not include the external-schema test seam; skipping."
  exit 0
fi

# (3) Build the current schema by initializing a fresh database with the current image.
compose_cmd up -d pg os minio redis || fail "Could not start service containers."
run_in_container webapp "./bin/util/initialize_instance" \
  || fail "Failed to initialize the database with the current image."

# (4) Run the previous release's database tests against the new schema. -n0 forces serial
# execution, which external-schema mode requires (all tests share the one database).
echo "Running the previous release's database tests against the current schema ..."
if ! run_in_container webapp-prev \
  "uv sync --frozen --active && pytest --no-cov -n0 -m db --ignore=tests/migration tests"; then
  fail "Previous release tests failed against the current schema: the migration is not backwards compatible."
fi

echo "The previous release works against the current schema 🎉"
