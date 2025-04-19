#!/bin/bash

set -e || true
set -u || true
set -o pipefail || true
set -x || true

exec java \
    -Xmx512M \
    --add-exports java.base/sun.nio.ch=ALL-UNNAMED \
    --add-exports java.base/jdk.internal.access=ALL-UNNAMED \
    -Ddhfs.objects.persistence.files.root=/dhfs_root/p \
    -Ddhfs.objects.root=/dhfs_root/d \
    -Ddhfs.fuse.root=/dhfs_root_fuse \
    -Dquarkus.http.host=0.0.0.0 \
    -Ddhfs.objects.ref_verification=false \
    -Dquarkus.log.category.\"com.usatiuk.dhfs\".level=$DHFS_LOGLEVEL \
    "$@" \
    -jar quarkus-run.jar
