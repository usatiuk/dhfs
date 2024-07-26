package com.usatiuk.dhfs.files.objects;

import com.usatiuk.dhfs.files.conflicts.NoOpConflictResolver;
import com.usatiuk.dhfs.objects.jrepository.AssumedUnique;
import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.jrepository.Movable;
import com.usatiuk.dhfs.objects.repository.ConflictResolver;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serial;
import java.util.Collection;
import java.util.List;

@Getter
@EqualsAndHashCode(callSuper = false)
@Movable
@AssumedUnique
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

    public static String getNameFromHash(String hash) {
        return "info_" + hash;
    }

    @Override
    public String getName() {
        return getNameFromHash(_hash);
    }

    @Override
    public Class<? extends ConflictResolver> getConflictResolver() {
        return NoOpConflictResolver.class;
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
    public long estimateSize() {
        return _hash.length() * 2L;
    }
}
