package com.usatiuk.dhfs.storage.files.service;

import com.usatiuk.dhfs.storage.files.objects.DirEntry;
import com.usatiuk.dhfs.storage.files.objects.Directory;
import com.usatiuk.dhfs.storage.files.objects.File;
import io.smallrye.mutiny.Uni;

import java.util.Optional;

public interface DhfsFileService {
    Uni<Optional<DirEntry>> getDirEntry(String name);
    Uni<Optional<File>> open(String name);
    Uni<Optional<File>> create(String name);
    Uni<Iterable<String>> readDir(String name);

    Uni<Long> size(File f);

    Uni<Optional<byte[]>> read(String fileUuid, long offset, int length);
    Uni<Long> write(String fileUuid, long offset, byte[] data);
    Uni<Boolean> truncate(String fileUuid, long length);

    Uni<Directory> getRoot();
}
