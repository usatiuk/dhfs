#!/usr/bin/env bash

CMAKE_ARGS="${CMAKE_ARGS:--DCMAKE_BUILD_TYPE=Debug}"

export SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
cd "$SCRIPT_DIR"

if [[ -z "${DO_LOCAL_BUILD}" ]]; then
  if [[ "$(uname)" == "Linux" ]]; then
    if [[ -z "${CROSS_PLATFORM}" ]]; then
      echo "Already on linux"
      exit 0
    fi
  fi
  exec "$SCRIPT_DIR"/docker-launch.sh "$@"
fi

set -euxo pipefail

if [ $# -lt 3 ]; then
  echo "Not enough arguments supplied: (build/configure) (build dir) (output dir)"
  exit 1
fi

PROJECT_DIR="$SCRIPT_DIR/.."

CONFIGURE_DIR="$2"
INSTALL_DIR="$3"

function configure() {
  cmake -B"$CONFIGURE_DIR" -S"$PROJECT_DIR" -DDHFS_LIB_INSTALL="$INSTALL_DIR" $CMAKE_ARGS
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
"both")
  configure
  build
  ;;
*)
  echo "Unknown command"
  exit 1
  ;;
esac
