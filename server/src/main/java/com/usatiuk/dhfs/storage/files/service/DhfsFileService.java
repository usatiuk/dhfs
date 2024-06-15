package com.usatiuk.dhfs.storage.files.service;

import com.usatiuk.dhfs.storage.files.objects.Directory;
import com.usatiuk.dhfs.storage.files.objects.File;
import com.usatiuk.dhfs.storage.files.objects.FsNode;
import io.smallrye.mutiny.Uni;

import java.util.Optional;

public interface DhfsFileService {
    Uni<Optional<FsNode>> getDirEntry(String name);
    Uni<Optional<File>> open(String name);
    Uni<Optional<File>> create(String name, long mode);
    Uni<Optional<Directory>> mkdir(String name, long mode);
    Uni<Boolean> chmod(String name, long mode);
    Uni<Boolean> rmdir(String name);
    Uni<Boolean> unlink(String name);
    Uni<Boolean> rename(String from, String to);
    Uni<Boolean> setTimes(String fileUuid, long atimeMs, long mtimeMs);
    Uni<Iterable<String>> readDir(String name);

    Uni<Long> size(File f);

    Uni<Optional<byte[]>> read(String fileUuid, long offset, int length);
    Uni<Long> write(String fileUuid, long offset, byte[] data);
    Uni<Boolean> truncate(String fileUuid, long length);

    Uni<Directory> getRoot();
}
