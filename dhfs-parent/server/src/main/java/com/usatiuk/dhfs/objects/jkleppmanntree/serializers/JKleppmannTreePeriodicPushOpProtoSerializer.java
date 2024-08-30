package com.usatiuk.dhfs.objects.jkleppmanntree.serializers;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.jkleppmanntree.JKleppmannTreePeriodicPushOp;
import com.usatiuk.dhfs.objects.repository.JKleppmannTreePeriodicPushOpP;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Singleton;

import java.util.UUID;

@Singleton
public class JKleppmannTreePeriodicPushOpProtoSerializer implements ProtoSerializer<JKleppmannTreePeriodicPushOpP, JKleppmannTreePeriodicPushOp> {

    @Override
    public JKleppmannTreePeriodicPushOp deserialize(JKleppmannTreePeriodicPushOpP message) {
        return new JKleppmannTreePeriodicPushOp(UUID.fromString(message.getFromUuid()), message.getTimestamp());
    }

    @Override
    public JKleppmannTreePeriodicPushOpP serialize(JKleppmannTreePeriodicPushOp object) {
        return JKleppmannTreePeriodicPushOpP.newBuilder().setTimestamp(object.getTimestamp()).setFromUuid(object.getFrom().toString()).build();
    }
}
