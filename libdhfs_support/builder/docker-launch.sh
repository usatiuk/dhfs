#!/usr/bin/env bash

set -euxo pipefail
export SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
cd "$SCRIPT_DIR"

DOCKER_IMG_FILE=$(mktemp)
docker build --iidfile "$DOCKER_IMG_FILE" .

ROOT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"

docker run --rm -v "$ROOT_DIR:$ROOT_DIR" -e INSIDE_DOCKER_ALREADY=TRUE "$(cat "$DOCKER_IMG_FILE")"  \
  "$SCRIPT_DIR/cross-build.sh" "$@"