package com.usatiuk.dhfs.storage.files.service;

import com.usatiuk.dhfs.storage.files.objects.Directory;
import com.usatiuk.dhfs.storage.files.objects.File;
import com.usatiuk.dhfs.storage.files.objects.FsNode;

import java.util.Optional;

public interface DhfsFileService {
    Optional<FsNode> getDirEntry(String name);
    Optional<File> open(String name);
    Optional<File> create(String name, long mode);
    Optional<Directory> mkdir(String name, long mode);
    Boolean chmod(String name, long mode);
    Boolean rmdir(String name);
    Boolean unlink(String name);
    Boolean rename(String from, String to);
    Boolean setTimes(String fileUuid, long atimeMs, long mtimeMs);
    Iterable<String> readDir(String name);

    Long size(File f);

    Optional<byte[]> read(String fileUuid, long offset, int length);
    Long write(String fileUuid, long offset, byte[] data);
    Boolean truncate(String fileUuid, long length);

    Directory getRoot();
}
