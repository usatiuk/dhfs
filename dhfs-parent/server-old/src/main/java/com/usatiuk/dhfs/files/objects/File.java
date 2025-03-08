package com.usatiuk.dhfs.files.objects;

import com.usatiuk.dhfs.files.conflicts.FileConflictResolver;
import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.repository.ConflictResolver;
import lombok.Getter;
import lombok.Setter;

import java.util.*;

public class File extends FsNode {
    @Getter
    private final NavigableMap<Long, String> _chunks;
    @Getter
    private final boolean _symlink;
    @Getter
    @Setter
    private long _size = 0;

    public File(UUID uuid, long mode, boolean symlink) {
        super(uuid, mode);
        _symlink = symlink;
        _chunks = new TreeMap<>();
    }

    public File(UUID uuid, long mode, boolean symlink, NavigableMap<Long, String> chunks) {
        super(uuid, mode);
        _symlink = symlink;
        _chunks = chunks;
    }

    @Override
    public Class<? extends ConflictResolver> getConflictResolver() {
        return FileConflictResolver.class;
    }

    @Override
    public Class<? extends JObjectData> getRefType() {
        return ChunkData.class;
    }

    @Override
    public Collection<String> extractRefs() {
        return Collections.unmodifiableCollection(_chunks.values());
    }

    @Override
    public int estimateSize() {
        return _chunks.size() * 192;
    }
}
