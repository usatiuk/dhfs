package com.usatiuk.dhfs.files.objects;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.files.conflicts.NoOpConflictResolver;
import com.usatiuk.dhfs.objects.jrepository.AssumedUnique;
import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.jrepository.Leaf;
import com.usatiuk.dhfs.objects.jrepository.NoTransaction;
import com.usatiuk.dhfs.objects.persistence.ChunkDataP;
import com.usatiuk.dhfs.objects.repository.ConflictResolver;
import net.openhft.hashing.LongTupleHashFunction;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@AssumedUnique
@Leaf
@NoTransaction
public class ChunkData extends JObjectData {
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

    ChunkDataP getData() {
        return _data;
    }

    public ByteString getBytes() {
        return _data.getData();
    }

    public int getSize() {
        return _data.getData().size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChunkData chunkData = (ChunkData) o;
        return Objects.equals(getName(), chunkData.getName());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getName());
    }

    @Override
    public String getName() {
        return _data.getName();
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
    public int estimateSize() {
        return _data.getData().size();
    }
}
