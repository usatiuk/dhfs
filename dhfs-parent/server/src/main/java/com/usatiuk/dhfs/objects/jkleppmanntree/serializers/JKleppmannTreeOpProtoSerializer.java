package com.usatiuk.dhfs.objects.jkleppmanntree.serializers;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.jkleppmanntree.JKleppmannTreeOpWrapper;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeNodeMetaP;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeOpP;
import com.usatiuk.kleppmanntree.CombinedTimestamp;
import com.usatiuk.kleppmanntree.OpMove;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.UUID;

@Singleton
public class JKleppmannTreeOpProtoSerializer implements ProtoSerializer<JKleppmannTreeOpP, JKleppmannTreeOpWrapper> {
    @Inject
    ProtoSerializer<JKleppmannTreeNodeMetaP, JKleppmannTreeNodeMeta> metaProtoSerializer;

    @Override
    public JKleppmannTreeOpWrapper deserialize(JKleppmannTreeOpP message) {
        return new JKleppmannTreeOpWrapper(new OpMove<>(
                new CombinedTimestamp<>(message.getTimestamp(), UUID.fromString(message.getPeer())), message.getNewParentId(),
                message.hasMeta() ? metaProtoSerializer.deserialize(message.getMeta()) : null,
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
            builder.setMeta(metaProtoSerializer.serialize(object.getOp().newMeta()));
        return builder.build();
    }
}
