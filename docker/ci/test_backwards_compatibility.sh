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
#  (2) Initializing a fresh database with the CURRENT image, which builds the new schema.
#  (3) Running the previous release's database tests (pytest -m db) against that schema in
#      external-schema mode (PALACE_TEST_DATABASE_EXTERNAL_SCHEMA), so the older code
#      exercises the new schema. If those tests fail, the migration is not backwards
#      compatible.
#
# The current image is provided via the WEBAPP_IMAGE environment variable (as in the other
# docker CI jobs). The previous release image can be overridden via PREV_RELEASE_IMAGE (useful
# for running this script locally); it is ignored in local mode, where the previous image is
# always built from the ref.
#
# Local mode (PREV_RELEASE_REF): pass a git ref as the first argument, or set
# PREV_RELEASE_REF, to test against that ref instead of the latest published release. Both
# images are then built from git -- the previous side from the ref's committed tree, the
# current side from the working tree (WEBAPP_IMAGE, built if unset) -- so you can prove a
# "stop using" change is backwards compatible locally, before it is released. Example:
#
#   ./docker/ci/test_backwards_compatibility.sh HEAD~1
#
# Requires that the build base image is pullable (docker login ghcr.io if needed). CI leaves
# PREV_RELEASE_REF unset and keeps testing against the real previous release.

set -uo pipefail

# Optional git ref for local mode (see header): accept it as $1, falling back to
# PREV_RELEASE_REF.
PREV_RELEASE_REF="${1:-${PREV_RELEASE_REF:-}}"

COMPOSE_FILES=(-f docker-compose.yml -f docker/ci/test_backwards_compatibility.yml)

# Run a docker compose command for this check.
compose_cmd() {
  docker compose "${COMPOSE_FILES[@]}" --progress quiet "$@"
}

# Run a command in a container with the palace virtualenv activated. compose run builds a missing
# image from its build: context, so webapp-prev -- which inherits webapp's build: via extends --
# must have its previous image ensured upstream (its --load/pull steps are || fail-guarded), or a
# missing one would silently test the working tree against itself instead of failing.
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

# Reject bad arguments early: only one git ref is meaningful. Too many (e.g. mistaking it for a
# ref range) or an empty one (e.g. ./...sh "$UNSET_VAR") would otherwise silently test the wrong
# thing -- the first ref, or a fall-through to the latest release -- and report a confident result.
[[ $# -gt 1 ]] && fail "Too many arguments; expected at most one git ref, got $#."
[[ $# -eq 1 && -z "${1:-}" ]] && fail "Empty git ref argument; pass a ref, or no argument to test against the latest release."

# (0) Local mode: when a git ref is given, build both sides from git instead of pulling the
# latest release, so a "stop using" change can be validated before it ships. Skipped in CI,
# which leaves PREV_RELEASE_REF unset and tests against the published previous release.
if [[ -n "${PREV_RELEASE_REF}" ]]; then
  git rev-parse --verify --quiet "${PREV_RELEASE_REF}^{commit}" >/dev/null \
    || fail "PREV_RELEASE_REF='${PREV_RELEASE_REF}' is not a valid git ref."
  prev_sha="$(git rev-parse --short "${PREV_RELEASE_REF}^{commit}")"
  base_image="${BUILD_BASE_IMAGE-ghcr.io/thepalaceproject/circ-baseimage:latest}"

  # Previous side: build the ref's committed tree, streamed as the build context via git archive
  # so the working tree is untouched -- no checkout or worktree needed. Export (not just set) so
  # `compose_cmd build webapp` below sees it, else compose warns webapp-prev's image is unset.
  export PREV_RELEASE_IMAGE="circ-webapp:backcompat-prev-${prev_sha}"
  echo "Building previous image from ${PREV_RELEASE_REF} (${prev_sha}) ..."
  # --load: else a container-driver buildx builder leaves the tag only in its cache, not the image store.
  prev_build=(docker buildx build -f docker/Dockerfile --target webapp --load
    --build-arg "BASE_IMAGE=${base_image}" -t "${PREV_RELEASE_IMAGE}")
  # cache_from / --platform: mirror docker-compose.yml so both sides build alike (empty cache_from is skipped;
  # a BUILD_PLATFORM mismatch would otherwise surface as a bogus "not backwards compatible" on webapp-prev).
  cache_from="${BUILD_CACHE_FROM-ghcr.io/thepalaceproject/circ-webapp:main}"
  [[ -n "${cache_from}" ]] && prev_build+=(--cache-from "${cache_from}")
  [[ -n "${BUILD_PLATFORM:-}" ]] && prev_build+=(--platform "${BUILD_PLATFORM}")
  prev_build+=(-)
  git archive --format=tar "${PREV_RELEASE_REF}" | "${prev_build[@]}" \
    || fail "Could not build the previous image from ${PREV_RELEASE_REF}."

  # Current side: build from the working tree (including uncommitted changes) so the schema
  # under test is whatever you are working on right now. Use plain progress (not compose_cmd's
  # --progress quiet) so this build -- the longest step in local mode -- streams output.
  if [[ -z "${WEBAPP_IMAGE:-}" ]]; then
    export WEBAPP_IMAGE="circ-webapp:backcompat-current-local"
    echo "Building current image from the working tree ..."
    docker compose "${COMPOSE_FILES[@]}" --progress plain build webapp \
      || fail "Could not build the current image from the working tree."
  fi
fi

# (1) Resolve the previous release image.
if [[ -z "${PREV_RELEASE_IMAGE:-}" ]]; then
  # In CI gh infers the repo from $GITHUB_REPOSITORY; locally it infers it from the git
  # remote of the current directory.
  gh_release_args=(release view --json tagName --jq '.tagName')
  if [[ -n "${GITHUB_REPOSITORY:-}" ]]; then
    gh_release_args+=(--repo "${GITHUB_REPOSITORY}")
  fi

  # Resolve the previous release's tag. We deliberately do NOT fail the job when this lookup
  # fails: `gh release view` exits non-zero both when there is genuinely no prior release (a
  # legitimate skip) and on transient errors (auth hiccup, rate limit, network blip). But we
  # don't fail *open silently*, we let gh's error flow to the job log and check its exit status, and on
  # any failure we emit a GitHub Actions ::warning:: before skipping. That makes the gate no-op
  # visible at the PR/checks level instead of quietly passing.
  prev_tag="$(gh "${gh_release_args[@]}")"
  gh_status=$?
  if [[ ${gh_status} -ne 0 || -z "${prev_tag}" ]]; then
    echo "::warning::Backwards-compatibility check skipped: could not resolve the previous release (gh exit ${gh_status}). The gate did NOT run for this build -- see gh's error above. A transient gh/auth/network failure or a genuine absence of releases both land here."
    exit 0
  fi

  PREV_RELEASE_IMAGE="ghcr.io/thepalaceproject/circ-webapp:${prev_tag#v}"
fi
export PREV_RELEASE_IMAGE
echo "Previous release image: ${PREV_RELEASE_IMAGE}"

if [[ -z "${WEBAPP_IMAGE:-}" ]]; then
  fail "WEBAPP_IMAGE is not set; it must point at the current build's webapp image."
fi
# Echo the current image too, so a stale/exported WEBAPP_IMAGE (which skips the working-tree
# build in local mode) is visible rather than producing a confidently wrong result.
echo "Current image: ${WEBAPP_IMAGE}"

trap cleanup EXIT

# (2) Build the current schema by initializing a fresh database with the current image.
# --wait blocks until the services report healthy: initialize_instance is run with
# `compose run --no-deps` (below), which bypasses the webapp service's depends_on health
# gating, so without --wait it can race a still-starting OpenSearch/Postgres and fail.
services=(pg os minio valkey)
echo "Starting service containers and waiting for them to be healthy ..."
compose_cmd up -d --wait "${services[@]}" || {
  # cleanup (trap EXIT) tears these down on exit, so capture state and logs before failing.
  compose_cmd ps -a
  compose_cmd logs --no-color "${services[@]}"
  fail "Could not start service containers."
}
run_in_container webapp "./bin/util/initialize_instance" \
  || fail "Failed to initialize the database with the current image."

# (3) Run the previous release's database tests against the new schema. In external-schema
# mode each xdist worker clones the freshly-built schema into its own database, so the suite
# runs in parallel (-n auto). In local mode the previous image was built above, not pulled.
if [[ -z "${PREV_RELEASE_REF}" ]]; then
  compose_cmd pull --quiet webapp-prev || fail "Could not pull ${PREV_RELEASE_IMAGE}."
fi
# Assert the previous image is actually present before running it. compose pull does not reliably
# fail for a service with a build: section (webapp-prev inherits webapp's via extends; it can fall
# back to "must build"), and compose run would then rebuild the missing image from the working
# tree -- a vacuous current-vs-current pass. This inspect fails loudly in both the pull (CI) and
# --load (local) paths, so a missing previous image can never be silently rebuilt.
docker image inspect "${PREV_RELEASE_IMAGE}" >/dev/null 2>&1 \
  || fail "${PREV_RELEASE_IMAGE} is not present locally; the check would rebuild it from the working tree and pass vacuously."
echo "Running the previous release's database tests against the current schema ..."
if ! run_in_container webapp-prev \
  "uv sync --frozen --active && pytest --no-cov -n auto -m db --ignore=tests/migration tests"; then
  fail "Previous release tests failed against the current schema: the migration is not backwards compatible."
fi

echo "The previous release works against the current schema 🎉"
