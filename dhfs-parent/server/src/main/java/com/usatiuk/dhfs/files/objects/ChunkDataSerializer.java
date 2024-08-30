package com.usatiuk.dhfs.files.objects;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.persistence.ChunkDataP;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Singleton;

@Singleton
public class ChunkDataSerializer implements ProtoSerializer<ChunkDataP, ChunkData> {
    @Override
    public ChunkData deserialize(ChunkDataP message) {
        return new ChunkData(message);
    }

    @Override
    public ChunkDataP serialize(ChunkData object) {
        return object.getData();
    }
}
