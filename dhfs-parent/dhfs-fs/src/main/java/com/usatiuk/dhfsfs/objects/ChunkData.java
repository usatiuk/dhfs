package com.usatiuk.dhfsfs.objects;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.remoteobj.JDataRemote;
import com.usatiuk.dhfs.remoteobj.JDataRemoteDto;
import com.usatiuk.objects.JObjectKey;

/**
 * ChunkData is a data structure that represents an immutable binary blob
 * @param key unique key
 * @param data binary data
 */
public record ChunkData(JObjectKey key, ByteString data) implements JDataRemote, JDataRemoteDto {
    @Override
    public int estimateSize() {
        return data.size();
    }
}