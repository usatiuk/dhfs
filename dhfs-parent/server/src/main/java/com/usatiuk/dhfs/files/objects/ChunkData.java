package com.usatiuk.dhfs.files.objects;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.JDataRefcounted;
import com.usatiuk.dhfs.objects.JObjectKey;
import org.pcollections.PCollection;
import org.pcollections.TreePSet;

public record ChunkData(JObjectKey key, PCollection<JObjectKey> refsFrom, boolean frozen,
                        ByteString data) implements JDataRefcounted {
    public ChunkData(JObjectKey key, ByteString data) {
        this(key, TreePSet.empty(), false, data);
    }

    @Override
    public ChunkData withRefsFrom(PCollection<JObjectKey> refs) {
        return new ChunkData(key, refs, frozen, data);
    }

    @Override
    public ChunkData withFrozen(boolean frozen) {
        return new ChunkData(key, refsFrom, frozen, data);
    }

    @Override
    public int estimateSize() {
        return data.size();
    }
}