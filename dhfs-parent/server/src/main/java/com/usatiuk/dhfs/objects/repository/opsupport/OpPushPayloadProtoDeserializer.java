package com.usatiuk.dhfs.objects.repository.opsupport;

import com.usatiuk.dhfs.objects.jkleppmanntree.JKleppmannTreeOpWrapper;
import com.usatiuk.dhfs.objects.jkleppmanntree.JKleppmannTreePeriodicPushOp;
import com.usatiuk.dhfs.objects.protoserializer.ProtoDeserializer;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializerService;
import com.usatiuk.dhfs.objects.repository.OpPushPayload;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class OpPushPayloadProtoDeserializer implements ProtoDeserializer<OpPushPayload, Op> {

    @Inject
    ProtoSerializerService protoSerializerService;

    @Override
    public Op deserialize(OpPushPayload message) {
        return switch (message.getPayloadCase()) {
            case JKLEPPMANNTREEOP ->
                    (JKleppmannTreeOpWrapper) protoSerializerService.deserialize(message.getJKleppmannTreeOp());
            case JKLEPPMANNTREEPERIODICPUSHOP ->
                    (JKleppmannTreePeriodicPushOp) protoSerializerService.deserialize(message.getJKleppmannTreePeriodicPushOp());
            case PAYLOAD_NOT_SET -> throw new IllegalArgumentException("OpPushPayload is null");
        };
    }
}
