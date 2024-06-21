package com.usatiuk.dhfs.storage.files.objects;

import com.usatiuk.dhfs.storage.objects.jrepository.JObject;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import lombok.Getter;

import java.util.Objects;

@Getter
public class ChunkInfo extends JObject {
    final String _hash;
    final Integer _size;

    public ChunkInfo(String hash, Integer size) {
        this._hash = hash;
        this._size = size;
    }

    @Override
    public Class<? extends ConflictResolver> getConflictResolver() {
        return NoOpConflictResolver.class;
    }

    @Override
    public String getName() {
        return getNameFromHash(_hash);
    }

    public static String getNameFromHash(String hash) {
        return hash + "_info";
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
}
