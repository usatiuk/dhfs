#!/usr/bin/env bash
set -e
set -u
set -o pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

PIDFILE="$SCRIPT_DIR"/.pid
EXTRAOPTS="$SCRIPT_DIR"/extra-opts

if [ ! -f "$EXTRAOPTS" ]; then
    touch "$EXTRAOPTS"
fi

if [ -f "$PIDFILE" ]; then
    if kill -0 $(cat "$PIDFILE") >/dev/null; then
        echo "Already running: "$(cat "$PIDFILE")
        exit 2
    fi
fi

EXTRAOPTS_PARSED="$(tr '\n\r' ' ' <"$EXTRAOPTS")"

echo "Extra options: $EXTRAOPTS_PARSED"

java \
    -Xmx256M -Ddhfs.objects.writeback.limit=134217728 \
    --add-exports java.base/sun.nio.ch=ALL-UNNAMED \
    -Ddhfs.objects.persistence.files.root="$SCRIPT_DIR"/../data/objects \
    -Ddhfs.objects.root="$SCRIPT_DIR"/../data/configs \
    -Ddhfs.fuse.root="$SCRIPT_DIR"/../fuse \
    -Dquarkus.http.host=0.0.0.0 \
    -Ddhfs.webui.root="$SCRIPT_DIR"/Webui $EXTRAOPTS_PARSED \
    -jar "$SCRIPT_DIR"/"DHFS Package"/quarkus-run.jar >quarkus.log 2>&1 &

echo "Started $!"

echo $! >"$PIDFILE"

disown
