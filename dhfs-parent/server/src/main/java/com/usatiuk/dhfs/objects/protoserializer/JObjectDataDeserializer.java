package com.usatiuk.dhfs.objects.protoserializer;

import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.persistence.JObjectDataP;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class JObjectDataDeserializer implements ProtoDeserializer<JObjectDataP, JObjectData> {
    @Inject
    ProtoSerializerService protoSerializerService;

    @Override
    public JObjectData deserialize(JObjectDataP message) {
        return switch (message.getObjCase()) {
            case FILE -> protoSerializerService.deserialize(message.getFile());
            case DIRECTORY -> protoSerializerService.deserialize(message.getDirectory());
            case CHUNKINFO -> protoSerializerService.deserialize(message.getChunkInfo());
            case CHUNKDATA -> protoSerializerService.deserialize(message.getChunkData());
            case PEERDIRECTORY -> protoSerializerService.deserialize(message.getPeerDirectory());
            case PERSISTENTPEERINFO -> protoSerializerService.deserialize(message.getPersistentPeerInfo());
            case OBJ_NOT_SET -> throw new IllegalStateException("Type not set when deserializing");
        };
    }
}
