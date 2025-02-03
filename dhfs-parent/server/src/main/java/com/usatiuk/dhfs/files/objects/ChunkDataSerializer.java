package com.usatiuk.dhfs.files.objects;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.persistence.ChunkDataP;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class ChunkDataSerializer implements ProtoSerializer<ChunkDataP, ChunkData> {
    @Override
    public ChunkData deserialize(ChunkDataP message) {
        return new ChunkData(JObjectKey.of(message.getName()), message.getData());
    }

    @Override
    public ChunkDataP serialize(ChunkData object) {
        return ChunkDataP.newBuilder()
                .setName(object.key().toString())
                .setData(object.data())
                .build();
    }
}
