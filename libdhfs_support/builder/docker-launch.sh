#!/usr/bin/env bash

PLATFORM_ARG=""
if [[ ! -z "${DOCKER_PLATFORM}" ]]; then
  PLATFORM_ARG="--platform $DOCKER_PLATFORM"
fi

set -euxo pipefail
export SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
cd "$SCRIPT_DIR"

DOCKER_IMG_FILE=$(mktemp)
docker build $PLATFORM_ARG --iidfile "$DOCKER_IMG_FILE" .

ROOT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"

docker run $PLATFORM_ARG --rm -v "$ROOT_DIR:$ROOT_DIR" -e DO_LOCAL_BUILD=TRUE "$(cat "$DOCKER_IMG_FILE")" \
  "$SCRIPT_DIR/cross-build.sh" "$@"
