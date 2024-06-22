package com.usatiuk.dhfs.storage.files.objects;

import com.usatiuk.dhfs.storage.files.conflicts.FileConflictResolver;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import lombok.Getter;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;

public class File extends FsNode {
    public File(UUID uuid, long mode, UUID parent) {
        super(uuid, mode);
        _parent = parent;
    }

    @Override
    public Class<? extends ConflictResolver> getConflictResolver() {
        return FileConflictResolver.class;
    }

    @Getter
    private final NavigableMap<Long, String> _chunks = new TreeMap<>();

    @Getter
    private final UUID _parent;
}
