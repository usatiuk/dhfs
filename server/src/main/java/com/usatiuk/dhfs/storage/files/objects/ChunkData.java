package com.usatiuk.dhfs.storage.files.objects;

import com.usatiuk.dhfs.storage.objects.jrepository.JObject;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import lombok.Getter;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.Arrays;
import java.util.Objects;

@Getter
public class ChunkData extends JObject {
    final String _hash;
    final byte[] _bytes;

    public ChunkData(byte[] bytes) {
        this._bytes = Arrays.copyOf(bytes, bytes.length);
        this._hash = DigestUtils.sha512Hex(bytes);
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
        return hash + "_data";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChunkData chunkData = (ChunkData) o;
        return Objects.equals(_hash, chunkData._hash);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_hash);
    }
}
