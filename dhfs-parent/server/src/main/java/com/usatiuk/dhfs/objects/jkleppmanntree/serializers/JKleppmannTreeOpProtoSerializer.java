package com.usatiuk.dhfs.objects.jkleppmanntree.serializers;

import com.usatiuk.dhfs.objects.jkleppmanntree.JKleppmannTreeOpWrapper;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeOpP;
import com.usatiuk.dhfs.objects.protoserializer.ProtoDeserializer;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializer;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializerService;
import com.usatiuk.kleppmanntree.CombinedTimestamp;
import com.usatiuk.kleppmanntree.OpMove;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.UUID;

@ApplicationScoped
public class JKleppmannTreeOpProtoSerializer implements ProtoDeserializer<JKleppmannTreeOpP, JKleppmannTreeOpWrapper>, ProtoSerializer<JKleppmannTreeOpP, JKleppmannTreeOpWrapper> {
    @Inject
    ProtoSerializerService protoSerializerService;

    @Override
    public JKleppmannTreeOpWrapper deserialize(JKleppmannTreeOpP message) {
        return new JKleppmannTreeOpWrapper(new OpMove<>(
                new CombinedTimestamp<>(message.getTimestamp(), UUID.fromString(message.getPeer())), message.getNewParentId(),
                message.hasMeta() ? protoSerializerService.deserialize(message.getMeta()) : null,
                message.getChild()
        ));
    }

    @Override
    public JKleppmannTreeOpP serialize(JKleppmannTreeOpWrapper object) {
        var builder = JKleppmannTreeOpP.newBuilder();
        builder.setTimestamp(object.getOp().timestamp().timestamp())
                .setPeer(object.getOp().timestamp().nodeId().toString())
                .setNewParentId(object.getOp().newParentId())
                .setChild(object.getOp().childId());
        if (object.getOp().newMeta() != null)
            builder.setMeta(protoSerializerService.serializeToTreeNodeMetaP(object.getOp().newMeta()));
        return builder.build();
    }
}
