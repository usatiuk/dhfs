#!/bin/sh

exec java \
    --add-exports java.base/sun.nio.ch=ALL-UNNAMED \
    -Ddhfs.objects.persistence.files.root=/dhfs_root/p \
    -Ddhfs.objects.root=/dhfs_root/d \
    -Ddhfs.fuse.root=/dhfs_root_fuse \
    -Dquarkus.http.host=0.0.0.0 \
    "$@" \
    -jar quarkus-run.jar
