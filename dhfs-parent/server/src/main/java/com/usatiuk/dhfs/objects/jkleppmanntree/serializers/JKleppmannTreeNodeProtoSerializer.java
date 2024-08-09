package com.usatiuk.dhfs.objects.jkleppmanntree.serializers;

import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNode;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeNodeP;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeNodeTimestampP;
import com.usatiuk.dhfs.objects.protoserializer.ProtoDeserializer;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializer;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializerService;
import com.usatiuk.kleppmanntree.CombinedTimestamp;
import com.usatiuk.kleppmanntree.TreeNode;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.UUID;

@ApplicationScoped
public class JKleppmannTreeNodeProtoSerializer implements ProtoDeserializer<JKleppmannTreeNodeP, JKleppmannTreeNode>, ProtoSerializer<JKleppmannTreeNodeP, JKleppmannTreeNode> {
    @Inject
    ProtoSerializerService protoSerializerService;

    @Override
    public JKleppmannTreeNode deserialize(JKleppmannTreeNodeP message) {
        var node = new TreeNode<Long, UUID, JKleppmannTreeNodeMeta, String>(
                message.getId(),
                message.hasParent() ? message.getParent() : null,
                message.hasMeta() ? protoSerializerService.deserialize(message.getMeta()) : null
        );
        node.getChildren().putAll(message.getChildrenMap());
        if (message.hasLastMoveTimestamp())
            node.setLastMoveTimestamp(new CombinedTimestamp<>(message.getLastMoveTimestamp().getClock(), UUID.fromString(message.getLastMoveTimestamp().getUuid())));
        return new JKleppmannTreeNode(node);
    }

    @Override
    public JKleppmannTreeNodeP serialize(JKleppmannTreeNode object) {
        var builder = JKleppmannTreeNodeP.newBuilder().setId(object.getNode().getId()).putAllChildren(object.getNode().getChildren());
        if (object.getNode().getParent() != null)
            builder.setParent(object.getNode().getParent());
        if (object.getNode().getMeta() != null) {
            builder.setMeta(protoSerializerService.serializeToTreeNodeMetaP(object.getNode().getMeta()));
        }
        if (object.getNode().getLastMoveTimestamp() != null)
            builder.setLastMoveTimestamp(
                    JKleppmannTreeNodeTimestampP.newBuilder()
                                      .setClock(object.getNode().getLastMoveTimestamp().timestamp())
                                      .setUuid(object.getNode().getLastMoveTimestamp().nodeId().toString())
                                      .build()
                                        );
        return builder.build();
    }
}
