package com.usatiuk.dhfs.objects.jkleppmanntree.serializers;

import com.usatiuk.dhfs.objects.jkleppmanntree.JKleppmannTreePeriodicPushOp;
import com.usatiuk.dhfs.objects.protoserializer.ProtoDeserializer;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializer;
import com.usatiuk.dhfs.objects.repository.JKleppmannTreePeriodicPushOpP;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.UUID;

@ApplicationScoped
public class JKleppmannTreePeriodicPushOpProtoSerializer implements ProtoDeserializer<JKleppmannTreePeriodicPushOpP, JKleppmannTreePeriodicPushOp>, ProtoSerializer<JKleppmannTreePeriodicPushOpP, JKleppmannTreePeriodicPushOp> {

    @Override
    public JKleppmannTreePeriodicPushOp deserialize(JKleppmannTreePeriodicPushOpP message) {
        return new JKleppmannTreePeriodicPushOp(UUID.fromString(message.getFromUuid()), message.getTimestamp());
    }

    @Override
    public JKleppmannTreePeriodicPushOpP serialize(JKleppmannTreePeriodicPushOp object) {
        return JKleppmannTreePeriodicPushOpP.newBuilder().setTimestamp(object.getTimestamp()).setFromUuid(object.getFrom().toString()).build();
    }
}
