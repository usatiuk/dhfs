#!/usr/bin/env bash

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

PIDFILE="$SCRIPT_DIR"/.pid

if [ ! -f "$PIDFILE" ]; then
    echo "Not running"
    exit 2
fi

if ! kill -0 $(cat "$PIDFILE") >/dev/null; then
    echo "Not running"
    rm .pid
    exit 2
fi

PID=$(cat "$PIDFILE")

echo "Killing $PID"
kill "$PID"

rm .pid
