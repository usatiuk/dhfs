package com.usatiuk.dhfs.files.objects;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.persistence.ChunkDataP;
import com.usatiuk.dhfs.persistence.JObjectKeyP;
import jakarta.inject.Singleton;

@Singleton
public class ChunkDataProtoSerializer implements ProtoSerializer<ChunkDataP, ChunkData> {
    @Override
    public ChunkData deserialize(ChunkDataP message) {
        return new ChunkData(
                JObjectKey.of(message.getKey().getName()),
                message.getData()
        );
    }

    @Override
    public ChunkDataP serialize(ChunkData object) {
        return ChunkDataP.newBuilder()
                .setKey(JObjectKeyP.newBuilder().setName(object.key().name()).build())
                .setData(object.data())
                .build();
    }
}
