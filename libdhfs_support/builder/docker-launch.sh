#!/usr/bin/env bash

set -exo pipefail
export SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
cd "$SCRIPT_DIR"

PLATFORM_ARG=""
if [[ ! -z "${DOCKER_PLATFORM}" ]]; then
  PLATFORM_ARG="--platform $DOCKER_PLATFORM"
fi

if [[ -z "${DOCKER_BUILDER_IMAGE}" ]]; then
  DOCKER_IMG_FILE=$(mktemp)
  docker build $PLATFORM_ARG --iidfile "$DOCKER_IMG_FILE" .
  DOCKER_BUILDER_IMAGE="$(cat "$DOCKER_IMG_FILE")"
fi

ROOT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"

docker run $PLATFORM_ARG --rm -v "$ROOT_DIR:$ROOT_DIR" -e DO_LOCAL_BUILD=TRUE "$DOCKER_BUILDER_IMAGE" \
  "$SCRIPT_DIR/cross-build.sh" "$@"
