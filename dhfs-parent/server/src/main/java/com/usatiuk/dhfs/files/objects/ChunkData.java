package com.usatiuk.dhfs.files.objects;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.files.conflicts.NoOpConflictResolver;
import com.usatiuk.dhfs.objects.jrepository.AssumedUnique;
import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.jrepository.Leaf;
import com.usatiuk.dhfs.objects.persistence.ChunkDataP;
import com.usatiuk.dhfs.objects.repository.ConflictResolver;
import lombok.Getter;
import net.openhft.hashing.LongTupleHashFunction;

import java.io.Serial;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Getter
@AssumedUnique
@Leaf
public class ChunkData extends JObjectData {
    @Serial
    private static final long serialVersionUID = 1;

    final ChunkDataP _data;

    public ChunkData(ByteString bytes) {
        super();
        _data = ChunkDataP.newBuilder()
                .setData(bytes)
                // TODO: There might be (most definitely) a copy there
                .setName(Arrays.stream(LongTupleHashFunction.xx128().hashBytes(bytes.asReadOnlyByteBuffer()))
                        .mapToObj(Long::toHexString).collect(Collectors.joining()))
                .build();
    }

    public ChunkData(ByteString bytes, String name) {
        super();
        _data = ChunkDataP.newBuilder()
                .setData(bytes)
                .setName(name)
                .build();
    }

    public ChunkData(ChunkDataP chunkDataP) {
        super();
        _data = chunkDataP;
    }

    public static String getNameFromHash(String hash) {
        return "data_" + hash;
    }

    public String getHash() {
        return _data.getName();
    }

    public ByteString getBytes() {
        return _data.getData();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChunkData chunkData = (ChunkData) o;
        return Objects.equals(_data.getName(), chunkData.getData().getName());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_data.getName());
    }

    @Override
    public String getName() {
        return getNameFromHash(_data.getName());
    }

    @Override
    public Class<? extends ConflictResolver> getConflictResolver() {
        return NoOpConflictResolver.class;
    }

    @Override
    public Collection<String> extractRefs() {
        return List.of();
    }

    @Override
    public long estimateSize() {
        return _data.getData().size();
    }
}
