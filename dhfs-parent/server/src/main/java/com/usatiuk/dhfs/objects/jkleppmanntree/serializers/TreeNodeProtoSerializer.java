package com.usatiuk.dhfs.objects.jkleppmanntree.serializers;

import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JTreeNodeMeta;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JTreeNodeMetaDirectory;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JTreeNodeMetaFile;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.TreeNodeJObjectData;
import com.usatiuk.dhfs.objects.persistence.TreeNodeMetaDirectoryP;
import com.usatiuk.dhfs.objects.persistence.TreeNodeMetaFileP;
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
        JTreeNodeMeta meta = switch (message.getMetaCase()) {
            case FILE -> (JTreeNodeMetaFile) protoSerializerService.deserialize(message.getFile());
            case DIR -> (JTreeNodeMetaDirectory) protoSerializerService.deserialize(message.getDir());
            case META_NOT_SET -> null;
        };
        var node = new TreeNode<>(
                message.getId(),
                message.hasParent() ? message.getParent() : null,
                meta
        );
        node.getChildren().putAll(message.getChildrenMap());
        return new TreeNodeJObjectData(node);
    }

    @Override
    public TreeNodeP serialize(TreeNodeJObjectData object) {
        var builder = TreeNodeP.newBuilder().setId(object.getNode().getId()).putAllChildren(object.getNode().getChildren());
        if (object.getNode().getParent() != null)
            builder.setParent(object.getNode().getParent());
        switch (object.getNode().getMeta()) {
            case JTreeNodeMetaFile jTreeNodeMetaFile ->
                    builder.setFile((TreeNodeMetaFileP) protoSerializerService.serialize(jTreeNodeMetaFile));
            case JTreeNodeMetaDirectory jTreeNodeMetaDirectory ->
                    builder.setDir((TreeNodeMetaDirectoryP) protoSerializerService.serialize(jTreeNodeMetaDirectory));
            case null, default -> {
            }
        }
        return builder.build();
    }
}
