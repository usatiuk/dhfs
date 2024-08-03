package com.usatiuk.dhfs.files.service;

import com.google.protobuf.ByteString;

import java.util.Optional;

public interface DhfsFileService {
    Optional<String> open(String name);

    Optional<String> create(String name, long mode);

    void mkdir(String name, long mode);

    Optional<GetattrRes> getattr(String name);

    Boolean chmod(String name, long mode);

    void unlink(String name);

    Boolean rename(String from, String to);

    Boolean setTimes(String fileUuid, long atimeMs, long mtimeMs);

    Iterable<String> readDir(String name);

    Long size(String f);

    Optional<ByteString> read(String fileUuid, long offset, int length);

    Long write(String fileUuid, long offset, byte[] data);

    Boolean truncate(String fileUuid, long length);

    String readlink(String uuid);

    ByteString readlinkBS(String uuid);

    String symlink(String oldpath, String newpath);
}
