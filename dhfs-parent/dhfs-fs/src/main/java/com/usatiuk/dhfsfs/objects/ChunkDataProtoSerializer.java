package com.usatiuk.dhfsfs.objects;

import com.usatiuk.dhfs.ProtoSerializer;
import com.usatiuk.dhfs.persistence.ChunkDataP;
import com.usatiuk.dhfs.persistence.JObjectKeyP;
import com.usatiuk.objects.JObjectKey;
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
                .setKey(JObjectKeyP.newBuilder().setName(object.key().value()).build())
                .setData(object.data())
                .build();
    }
}
