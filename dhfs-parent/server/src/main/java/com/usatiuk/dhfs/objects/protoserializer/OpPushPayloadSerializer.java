package com.usatiuk.dhfs.objects.protoserializer;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.jkleppmanntree.JKleppmannTreeOpWrapper;
import com.usatiuk.dhfs.objects.jkleppmanntree.JKleppmannTreePeriodicPushOp;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeOpP;
import com.usatiuk.dhfs.objects.repository.JKleppmannTreePeriodicPushOpP;
import com.usatiuk.dhfs.objects.repository.OpPushPayload;
import com.usatiuk.dhfs.objects.repository.opsupport.Op;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class OpPushPayloadSerializer implements ProtoSerializer<OpPushPayload, Op> {
    @Inject
    ProtoSerializer<JKleppmannTreePeriodicPushOpP, JKleppmannTreePeriodicPushOp> periodicPushOpSerializer;
    @Inject
    ProtoSerializer<JKleppmannTreeOpP, JKleppmannTreeOpWrapper> treeOpWrapperProtoSerializer;

    @Override
    public Op deserialize(OpPushPayload message) {
        return switch (message.getPayloadCase()) {
            case JKLEPPMANNTREEOP ->
                    (JKleppmannTreeOpWrapper) treeOpWrapperProtoSerializer.deserialize(message.getJKleppmannTreeOp());
            case JKLEPPMANNTREEPERIODICPUSHOP ->
                    (JKleppmannTreePeriodicPushOp) periodicPushOpSerializer.deserialize(message.getJKleppmannTreePeriodicPushOp());
            case PAYLOAD_NOT_SET -> throw new IllegalArgumentException("OpPushPayload is null");
        };
    }

    @Override
    public OpPushPayload serialize(Op object) {
        if (object == null) throw new IllegalArgumentException("Object to serialize shouldn't be null");
        if (object instanceof JKleppmannTreeOpWrapper op) {
            return OpPushPayload.newBuilder().setJKleppmannTreeOp(
                    treeOpWrapperProtoSerializer.serialize(op)
            ).build();
        } else if (object instanceof JKleppmannTreePeriodicPushOp op) {
            return OpPushPayload.newBuilder().setJKleppmannTreePeriodicPushOp(
                    periodicPushOpSerializer.serialize(op)
            ).build();
        } else {
            throw new IllegalArgumentException("Unexpected object type on input to serializeToTreeNodeMetaP: " + object.getClass());
        }
    }
}
