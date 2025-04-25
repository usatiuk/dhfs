package com.usatiuk.dhfsfs.service;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import com.usatiuk.objects.JObjectKey;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Optional;

public interface DhfsFileService {
    Optional<JObjectKey> open(String name);

    Optional<JObjectKey> create(String name, long mode);

    Pair<String, JObjectKey> inoToParent(JObjectKey ino);

    void mkdir(String name, long mode);

    Optional<GetattrRes> getattr(JObjectKey name);

    Boolean chmod(JObjectKey name, long mode);

    void unlink(String name);

    Boolean rename(String from, String to);

    Boolean setTimes(JObjectKey fileUuid, long atimeMs, long mtimeMs);

    Iterable<String> readDir(String name);

    long size(JObjectKey fileUuid);

    ByteString read(JObjectKey fileUuid, long offset, int length);

    Long write(JObjectKey fileUuid, long offset, ByteString data);

    default Long write(JObjectKey fileUuid, long offset, byte[] data) {
        return write(fileUuid, offset, UnsafeByteOperations.unsafeWrap(data));
    }

    Boolean truncate(JObjectKey fileUuid, long length);

    String readlink(JObjectKey uuid);

    ByteString readlinkBS(JObjectKey uuid);

    JObjectKey symlink(String oldpath, String newpath);
}
