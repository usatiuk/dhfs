package com.usatiuk.dhfs.files.objects;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.JDataRemote;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.dhfs.repository.JDataRemoteDto;

public record ChunkData(JObjectKey key, ByteString data) implements JDataRemote, JDataRemoteDto {
    @Override
    public int estimateSize() {
        return data.size();
    }
}