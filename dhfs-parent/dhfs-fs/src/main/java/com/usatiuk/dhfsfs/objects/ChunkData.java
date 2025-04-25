package com.usatiuk.dhfsfs.objects;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.remoteobj.JDataRemote;
import com.usatiuk.dhfs.remoteobj.JDataRemoteDto;
import com.usatiuk.objects.JObjectKey;

public record ChunkData(JObjectKey key, ByteString data) implements JDataRemote, JDataRemoteDto {
    @Override
    public int estimateSize() {
        return data.size();
    }
}