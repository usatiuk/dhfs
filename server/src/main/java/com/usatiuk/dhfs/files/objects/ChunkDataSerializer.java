package com.usatiuk.dhfs.files.objects;

import com.usatiuk.dhfs.objects.persistence.ChunkDataP;
import com.usatiuk.dhfs.objects.protoserializer.ProtoDeserializer;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializer;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class ChunkDataSerializer implements ProtoSerializer<ChunkDataP, ChunkData>, ProtoDeserializer<ChunkDataP, ChunkData> {
    @Override
    public ChunkData deserialize(ChunkDataP message) {
        return new ChunkData(message.getData(), message.getName());
    }

    @Override
    public ChunkDataP serialize(ChunkData object) {
        return ChunkDataP.newBuilder()
                .setData(object.getBytes())
                .setName(object.getHash())
                .build();
    }
}
