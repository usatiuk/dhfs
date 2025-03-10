package com.usatiuk.dhfs.files.objects;

import com.google.protobuf.ByteString;
import com.usatiuk.autoprotomap.runtime.ProtoMirror;
import com.usatiuk.dhfs.objects.JDataRemote;
import com.usatiuk.dhfs.objects.JObjectKey;

public record ChunkData(JObjectKey key, ByteString data) implements JDataRemote {
    @Override
    public int estimateSize() {
        return data.size();
    }
}