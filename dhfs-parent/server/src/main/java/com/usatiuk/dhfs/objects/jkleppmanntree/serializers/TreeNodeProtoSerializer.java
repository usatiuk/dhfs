package com.usatiuk.dhfs.objects.jkleppmanntree.serializers;

import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JTreeNodeMeta;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.TreeNodeJObjectData;
import com.usatiuk.dhfs.objects.persistence.TreeNodeP;
import com.usatiuk.dhfs.objects.protoserializer.ProtoDeserializer;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializer;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializerService;
import com.usatiuk.kleppmanntree.TreeNode;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class TreeNodeProtoSerializer implements ProtoDeserializer<TreeNodeP, TreeNodeJObjectData>, ProtoSerializer<TreeNodeP, TreeNodeJObjectData> {
    @Inject
    ProtoSerializerService protoSerializerService;

    @Override
    public TreeNodeJObjectData deserialize(TreeNodeP message) {
        var node = new TreeNode<>(
                message.getId(),
                message.hasParent() ? message.getParent() : null,
                message.hasMeta() ? (JTreeNodeMeta) protoSerializerService.deserialize(message.getMeta()) : null
        );
        node.getChildren().putAll(message.getChildrenMap());
        return new TreeNodeJObjectData(node);
    }

    @Override
    public TreeNodeP serialize(TreeNodeJObjectData object) {
        var builder = TreeNodeP.newBuilder().setId(object.getNode().getId()).putAllChildren(object.getNode().getChildren());
        if (object.getNode().getParent() != null)
            builder.setParent(object.getNode().getParent());
        if (object.getNode().getMeta() != null) {
            builder.setMeta(protoSerializerService.serializeToTreeNodeMetaP(object.getNode().getMeta()));
        }
        return builder.build();
    }
}
