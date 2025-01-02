package com.usatiuk.dhfs.files.objects;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.JDataRefcounted;
import com.usatiuk.dhfs.objects.JObjectKey;

import java.util.Collection;
import java.util.LinkedHashSet;

public record ChunkData(JObjectKey key, Collection<JObjectKey> refsFrom, boolean frozen,
                        ByteString data) implements JDataRefcounted {
    public ChunkData(JObjectKey key, ByteString data) {
        this(key, new LinkedHashSet<>(), false, data);
    }

    @Override
    public ChunkData withRefsFrom(Collection<JObjectKey> refs) {
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