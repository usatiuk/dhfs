package com.usatiuk.dhfs.storage.files.service;

import com.usatiuk.dhfs.storage.files.objects.DirEntry;
import com.usatiuk.dhfs.storage.files.objects.Directory;
import com.usatiuk.dhfs.storage.files.objects.File;
import io.smallrye.mutiny.Uni;

import java.util.Optional;

public interface DhfsFileService {
    public Uni<Optional<DirEntry>> getDirEntry(String name);
    public Uni<Optional<File>> open(String name);
    public Uni<Iterable<String>> readDir(String name);

    public Uni<Directory> getRoot();
}
