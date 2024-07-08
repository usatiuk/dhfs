package com.usatiuk.dhfs.files.objects;

import com.usatiuk.dhfs.files.conflicts.NoOpConflictResolver;
import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.jrepository.Movable;
import com.usatiuk.dhfs.objects.repository.ConflictResolver;
import lombok.Getter;

import java.io.Serial;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

@Getter
@Movable
public class ChunkInfo extends JObjectData {
    @Serial
    private static final long serialVersionUID = 1;

    final String _hash;
    final Integer _size;

    public ChunkInfo(String hash, Integer size) {
        super();
        this._hash = hash;
        this._size = size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChunkInfo chunkInfo = (ChunkInfo) o;
        return Objects.equals(_hash, chunkInfo._hash) && Objects.equals(_size, chunkInfo._size);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_hash, _size);
    }

    @Override
    public String getName() {
        return getNameFromHash(_hash);
    }

    @Override
    public Class<? extends ConflictResolver> getConflictResolver() {
        return NoOpConflictResolver.class;
    }

    public static String getNameFromHash(String hash) {
        return "info_" + hash;
    }

    @Override
    public Class<? extends JObjectData> getRefType() {
        return ChunkData.class;
    }

    @Override
    public Collection<String> extractRefs() {
        return List.of(ChunkData.getNameFromHash(_hash));
    }

    @Override
    public boolean assumeUnique() {
        return true;
    }

    @Override
    public long estimateSize() {
        return _hash.length() * 2L;
    }
}
