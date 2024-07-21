package com.usatiuk.dhfs.objects.protoserializer;

import com.usatiuk.dhfs.objects.persistence.BlobP;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class BlobDeserializer implements ProtoDeserializer<BlobP, Object> {
    @Inject
    ProtoSerializerService protoSerializerService;

    @Override
    public Object deserialize(BlobP message) {
        return switch (message.getDtypeCase()) {
            case METADATA -> protoSerializerService.deserialize(message.getMetadata());
            case DATA -> protoSerializerService.deserialize(message.getData());
            case DTYPE_NOT_SET ->
                    throw new IllegalStateException("Malformed protobuf message " + message.getDtypeCase());
        };
    }
}
