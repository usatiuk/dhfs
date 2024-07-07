#!/usr/bin/env bash

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

exec java \
    -Xmx256M -Ddhfs.objects.writeback.limit=134217728 \
    --add-exports java.base/sun.nio.ch=ALL-UNNAMED \
    -Ddhfs.objects.persistence.files.root="$SCRIPT_DIR"/../data/objects \
    -Ddhfs.objects.root="$SCRIPT_DIR"/../data/configs \
    -Ddhfs.fuse.root="$SCRIPT_DIR"/../fuse \
    -Dquarkus.http.host=0.0.0.0 \
    -Ddhfs.webui.root="$SCRIPT_DIR"/Webui \
    -jar "$SCRIPT_DIR"/"DHFS Package"/quarkus-run.jar
