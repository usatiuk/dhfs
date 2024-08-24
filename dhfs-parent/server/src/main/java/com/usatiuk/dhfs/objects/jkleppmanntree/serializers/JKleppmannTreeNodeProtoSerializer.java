package com.usatiuk.dhfs.objects.jkleppmanntree.serializers;

import com.usatiuk.dhfs.objects.jkleppmanntree.JKleppmannTreeOpWrapper;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNode;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeNodeP;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeOpP;
import com.usatiuk.dhfs.objects.protoserializer.ProtoDeserializer;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializer;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializerService;
import com.usatiuk.kleppmanntree.TreeNode;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.HashMap;
import java.util.UUID;

@ApplicationScoped
public class JKleppmannTreeNodeProtoSerializer implements ProtoDeserializer<JKleppmannTreeNodeP, JKleppmannTreeNode>, ProtoSerializer<JKleppmannTreeNodeP, JKleppmannTreeNode> {
    @Inject
    ProtoSerializerService protoSerializerService;

    @Override
    public JKleppmannTreeNode deserialize(JKleppmannTreeNodeP message) {
        var children = new HashMap<String, String>();
        message.getChildrenList().forEach(child -> children.put(child.getKey(), child.getValue()));
        var node = new TreeNode<Long, UUID, JKleppmannTreeNodeMeta, String>(
                message.getId(),
                message.hasParent() ? message.getParent() : null,
                message.hasMeta() ? protoSerializerService.deserialize(message.getMeta()) : null,
                children
        );
        if (message.hasLastEffectiveOp())
            node.setLastEffectiveOp(((JKleppmannTreeOpWrapper) protoSerializerService.deserialize(message.getLastEffectiveOp())).getOp());
        return new JKleppmannTreeNode(node);
    }

    @Override
    public JKleppmannTreeNodeP serialize(JKleppmannTreeNode object) {
        var builder = JKleppmannTreeNodeP.newBuilder().setId(object.getNode().getId());
        if (object.getNode().getParent() != null)
            builder.setParent(object.getNode().getParent());
        if (object.getNode().getMeta() != null) {
            builder.setMeta(protoSerializerService.serializeToTreeNodeMetaP(object.getNode().getMeta()));
        }
        if (object.getNode().getLastEffectiveOp() != null)
            builder.setLastEffectiveOp(
                    (JKleppmannTreeOpP) protoSerializerService.serialize(new JKleppmannTreeOpWrapper(object.getNode().getLastEffectiveOp()))
            );
        object.getNode().getChildren().forEach((k, v) -> {
            builder.addChildrenBuilder().setKey(k).setValue(v);
        });
        return builder.build();
    }
}
