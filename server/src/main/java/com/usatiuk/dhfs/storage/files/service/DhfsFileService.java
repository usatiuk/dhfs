package com.usatiuk.dhfs.storage.files.service;

import com.usatiuk.dhfs.storage.files.objects.FsNode;

import java.util.Optional;

public interface DhfsFileService {
    Optional<String> open(String name);
    Optional<String> create(String name, long mode);
    Optional<String> mkdir(String name, long mode);
    Optional<FsNode> getattr(String name);
    Boolean chmod(String name, long mode);
    Boolean rmdir(String name);
    Boolean unlink(String name);
    Boolean rename(String from, String to);
    Boolean setTimes(String fileUuid, long atimeMs, long mtimeMs);
    Iterable<String> readDir(String name);

    Long size(String f);

    Optional<byte[]> read(String fileUuid, long offset, int length);
    Long write(String fileUuid, long offset, byte[] data);
    Boolean truncate(String fileUuid, long length);
}
