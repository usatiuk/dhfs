#!/usr/bin/env bash

if [[ "$(uname)" == "*Linux*" ]]
then
  echo "Already on linux"
  exit 1
fi

export SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
cd "$SCRIPT_DIR"

if [[ -z "${INSIDE_DOCKER_ALREADY}" ]]; then
  exec "$SCRIPT_DIR"/docker-launch.sh "$@"
fi

set -euxo pipefail

if [ $# -lt 3 ]
  then
    echo "Not enough arguments supplied: (build/configure) (build dir) (output dir)"
    exit 1
fi

PROJECT_DIR="$SCRIPT_DIR/.."

CONFIGURE_DIR="$2"
INSTALL_DIR="$3"

function configure() {
  cmake -B"$CONFIGURE_DIR" -S"$PROJECT_DIR" -DDHFS_LIB_INSTALL="$INSTALL_DIR"
}

function build() {
  cmake --build "$CONFIGURE_DIR" --target install
}

mkdir -p "$2"
mkdir -p "$3"

case "$1" in
  "configure")
    configure
    ;;
  "build")
    build
    ;;
  *)
    echo "Unknown command"
    exit 1
    ;;
esac
