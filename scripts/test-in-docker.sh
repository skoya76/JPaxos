#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
IMAGE_NAME=${JPAXOS_JAVA_TEST_IMAGE:-jpaxos-java-test}
REBUILD_IMAGE=${JPAXOS_JAVA_TEST_REBUILD:-0}
DOCKERFILE="${REPO_ROOT}/docker/java-test.Dockerfile"
BUILD_CONTEXT="${REPO_ROOT}/docker"

if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required but was not found." >&2
    exit 1
fi

if [ "${REBUILD_IMAGE}" = "1" ] || ! docker image inspect "${IMAGE_NAME}" >/dev/null 2>&1; then
    echo "Building Docker image: ${IMAGE_NAME}"
    docker build -t "${IMAGE_NAME}" -f "${DOCKERFILE}" "${BUILD_CONTEXT}"
fi

if [ "$#" -eq 0 ]; then
    set -- test
fi

docker run --rm \
    -v "${REPO_ROOT}:/work:rw" \
    -w /work \
    -e TMPDIR=/tmp \
    "${IMAGE_NAME}" \
    ant -Dclasses=/tmp/jpaxos-bin -DnativeHeaders.dir=/tmp/jpaxos-headers \
    -Dtest.excludes="**/Nvm*Test.java" "$@"
