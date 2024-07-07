package com.usatiuk.dhfs.files.objects;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.files.conflicts.NoOpConflictResolver;
import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.repository.ConflictResolver;
import lombok.Getter;
import net.openhft.hashing.LongTupleHashFunction;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Getter
public class ChunkData extends JObjectData {
    final String _hash;
    final ByteString _bytes;

    public ChunkData(ByteString bytes) {
        super();
        this._bytes = bytes;
        // TODO: There might be (most definitely) a copy there
        this._hash = Arrays.stream(LongTupleHashFunction.xx128().hashBytes(_bytes.asReadOnlyByteBuffer()))
                .mapToObj(Long::toHexString).collect(Collectors.joining());
    }

    public ChunkData(ByteString bytes, String name) {
        super();
        this._bytes = bytes;
        this._hash = name;
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
    public Collection<String> extractRefs() {
        return List.of();
    }

    @Override
    public boolean assumeUnique() {
        return true;
    }

    @Override
    public long estimateSize() {
        return _bytes.size();
    }
}