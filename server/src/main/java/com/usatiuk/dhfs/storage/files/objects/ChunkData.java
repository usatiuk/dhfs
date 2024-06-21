package com.usatiuk.dhfs.storage.files.objects;

import com.usatiuk.dhfs.storage.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import lombok.Getter;
import net.openhft.hashing.LongTupleHashFunction;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

@Getter
public class ChunkData extends JObjectData {
    final String _hash;
    final byte[] _bytes;

    public ChunkData(byte[] bytes) {
        super();
        this._bytes = Arrays.copyOf(bytes, bytes.length);
        this._hash = Arrays.stream(LongTupleHashFunction.xx128().hashBytes(bytes))
                .mapToObj(Long::toHexString).collect(Collectors.joining());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChunkData chunkData = (ChunkData) o;
        return Objects.equals(_hash, chunkData._hash);
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
        return "data_" + hash;
    }

    @Override
    public boolean assumeUnique() {
        return true;
    }
}
