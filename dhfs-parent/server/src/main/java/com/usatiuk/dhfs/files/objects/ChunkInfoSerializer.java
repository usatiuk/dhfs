package com.usatiuk.dhfs.files.objects;

import com.usatiuk.dhfs.objects.persistence.ChunkInfoP;
import com.usatiuk.dhfs.objects.protoserializer.ProtoDeserializer;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializer;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class ChunkInfoSerializer implements ProtoSerializer<ChunkInfoP, ChunkInfo>, ProtoDeserializer<ChunkInfoP, ChunkInfo> {
    @Override
    public ChunkInfo deserialize(ChunkInfoP message) {
        return new ChunkInfo(message.getName(), message.getSize());
    }

    @Override
    public ChunkInfoP serialize(ChunkInfo object) {
        return ChunkInfoP.newBuilder()
                .setName(object.getHash())
                .setSize(object.getSize())
                .build();
    }
}
