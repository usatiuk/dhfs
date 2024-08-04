package com.usatiuk.dhfs.objects.jkleppmanntree.helpers;

import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JTreeNodeMeta;
import com.usatiuk.dhfs.objects.protoserializer.ProtoDeserializer;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializer;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializerService;
import com.usatiuk.dhfs.objects.repository.OpPushJKleppmannTree;
import com.usatiuk.kleppmanntree.CombinedTimestamp;
import com.usatiuk.kleppmanntree.OpMove;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.UUID;

@ApplicationScoped
public class OpProtoSerializer implements ProtoDeserializer<OpPushJKleppmannTree, JOpWrapper>, ProtoSerializer<OpPushJKleppmannTree, JOpWrapper> {
    @Inject
    ProtoSerializerService protoSerializerService;

    @Override
    public JOpWrapper deserialize(OpPushJKleppmannTree message) {
        return new JOpWrapper(new OpMove<>(
                new CombinedTimestamp<>(message.getTimestamp(), UUID.fromString(message.getPeer())), message.getNewParentId(),
                message.hasMeta() ? protoSerializerService.deserialize(message.getMeta()) : null,
                message.getChild()
        ));
    }

    @Override
    public OpPushJKleppmannTree serialize(JOpWrapper object) {
        var builder = OpPushJKleppmannTree.newBuilder();
        builder.setTimestamp(object.getOp().timestamp().timestamp())
               .setPeer(object.getOp().timestamp().nodeId().toString())
               .setNewParentId(object.getOp().newParentId())
               .setChild(object.getOp().childId());
        if (object.getOp().newMeta() != null)
            builder.setMeta(protoSerializerService.serializeToTreeNodeMetaP(object.getOp().newMeta()));
        return builder.build();
    }
}
