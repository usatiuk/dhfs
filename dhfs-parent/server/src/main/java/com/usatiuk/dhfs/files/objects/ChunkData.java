package com.usatiuk.dhfs.files.objects;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.JDataRefcounted;
import com.usatiuk.objects.common.runtime.JObjectKey;
import lombok.Builder;

import java.util.Collection;
import java.util.LinkedHashSet;

@Builder(toBuilder = true)
public record ChunkData(JObjectKey key, Collection<JObjectKey> refsFrom, boolean frozen,
                        ByteString data) implements JDataRefcounted {
    public ChunkData(JObjectKey key, ByteString data) {
        this(key, new LinkedHashSet<>(), false, data);
    }

    @Override
    public ChunkData withRefsFrom(Collection<JObjectKey> refs) {
        return this.toBuilder().refsFrom(refs).build();
    }

    @Override
    public ChunkData withFrozen(boolean frozen) {
        return this.toBuilder().frozen(frozen).build();
    }

    @Override
    public int estimateSize() {
        return data.size();
    }
}