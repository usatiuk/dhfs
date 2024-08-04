package com.usatiuk.dhfs.objects.repository.invalidation;

import com.usatiuk.dhfs.objects.jkleppmanntree.helpers.JOpWrapper;
import com.usatiuk.dhfs.objects.protoserializer.ProtoDeserializer;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializerService;
import com.usatiuk.dhfs.objects.repository.OpPushPayload;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class OpPushPayloadDeserializer implements ProtoDeserializer<OpPushPayload, Op> {

    @Inject
    ProtoSerializerService protoSerializerService;

    @Override
    public Op deserialize(OpPushPayload message) {
        return switch (message.getPayloadCase()) {
            case JKLEPPMANNTREEOP -> (JOpWrapper) protoSerializerService.deserialize(message.getJKleppmannTreeOp());
            case PAYLOAD_NOT_SET -> throw new IllegalArgumentException("OpPushPayload is null");
        };
    }
}
