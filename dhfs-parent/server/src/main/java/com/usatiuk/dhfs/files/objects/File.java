package com.usatiuk.dhfs.files.objects;

import com.usatiuk.dhfs.files.conflicts.FileConflictResolver;
import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.repository.ConflictResolver;
import lombok.Getter;

import java.io.Serial;
import java.util.Collection;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;

public class File extends FsNode {
    @Serial
    private static final long serialVersionUID = 1;
    @Getter
    private final NavigableMap<Long, String> _chunks = new TreeMap<>();
    @Getter
    private final UUID _parent;
    @Getter
    private final boolean _symlink;

    public File(UUID uuid, long mode, UUID parent, boolean symlink) {
        super(uuid, mode);
        if (parent == null)
            throw new IllegalArgumentException("Parent UUID cannot be null");
        _parent = parent;
        _symlink = symlink;
    }

    @Override
    public Class<? extends ConflictResolver> getConflictResolver() {
        return FileConflictResolver.class;
    }

    @Override
    public Class<? extends JObjectData> getRefType() {
        return ChunkInfo.class;
    }

    @Override
    public Collection<String> extractRefs() {
        return _chunks.values().stream().map(ChunkInfo::getNameFromHash).toList();
    }

    @Override
    public long estimateSize() {
        return _chunks.size() * 192L;
    }
}
