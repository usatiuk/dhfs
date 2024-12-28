package com.usatiuk.dhfs.objects.jkleppmanntree.serializers;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.jkleppmanntree.JKleppmannTreeOpWrapper;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeNodeMetaP;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeOpLogEffectP;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeOpP;
import com.usatiuk.kleppmanntree.LogEffect;
import com.usatiuk.kleppmanntree.LogEffectOld;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.UUID;

@Singleton
public class JKleppmannTreeLogEffectSerializer implements ProtoSerializer<JKleppmannTreeOpLogEffectP, LogEffect<Long, UUID, JKleppmannTreeNodeMeta, String>> {
    @Inject
    ProtoSerializer<JKleppmannTreeOpP, JKleppmannTreeOpWrapper> opProtoSerializer;
    @Inject
    ProtoSerializer<JKleppmannTreeNodeMetaP, JKleppmannTreeNodeMeta> metaProtoSerializer;

    @Override
    public LogEffect<Long, UUID, JKleppmannTreeNodeMeta, String> deserialize(JKleppmannTreeOpLogEffectP message) {
        return new LogEffect<>(
                message.hasOldParent() ? new LogEffectOld<>(
                        opProtoSerializer.deserialize(message.getOldEffectiveMove()).getOp(),
                        message.getOldParent(),
                        metaProtoSerializer.deserialize(message.getOldMeta())
                ) : null,
                opProtoSerializer.deserialize(message.getEffectiveOp()).getOp(),
                message.getNewParentId(),
                metaProtoSerializer.deserialize(message.getNewMeta()),
                message.getSelfId()
        );
    }

    @Override
    public JKleppmannTreeOpLogEffectP serialize(LogEffect<Long, UUID, JKleppmannTreeNodeMeta, String> object) {
        var builder = JKleppmannTreeOpLogEffectP.newBuilder();
        // FIXME: all these wrappers
        if (object.oldInfo() != null) {
            builder.setOldEffectiveMove(opProtoSerializer.serialize(new JKleppmannTreeOpWrapper(object.oldInfo().oldEffectiveMove())));
            builder.setOldParent(object.oldInfo().oldParent());
            builder.setOldMeta(metaProtoSerializer.serialize(object.oldInfo().oldMeta()));
        }
        builder.setEffectiveOp(opProtoSerializer.serialize(new JKleppmannTreeOpWrapper(object.effectiveOp())));
        builder.setNewParentId(object.newParentId());
        builder.setNewMeta(metaProtoSerializer.serialize(object.newMeta()));
        builder.setSelfId(object.childId());
        return builder.build();
    }
}
