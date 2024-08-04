package com.usatiuk.dhfs.objects.jkleppmanntree.serializers;

import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JTreeNodeMeta;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JTreeNodeMetaDirectory;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JTreeNodeMetaFile;
import com.usatiuk.dhfs.objects.persistence.TreeNodeMetaP;
import com.usatiuk.dhfs.objects.protoserializer.ProtoDeserializer;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializerService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class TreeNodeMetaProtoDeseraializer implements ProtoDeserializer<TreeNodeMetaP, JTreeNodeMeta> {
    @Inject
    ProtoSerializerService protoSerializerService;

    @Override
    public JTreeNodeMeta deserialize(TreeNodeMetaP message) {
        return switch (message.getMetaCase()) {
            case FILE -> (JTreeNodeMetaFile) protoSerializerService.deserialize(message.getFile());
            case DIR -> (JTreeNodeMetaDirectory) protoSerializerService.deserialize(message.getDir());
            case META_NOT_SET -> throw new IllegalArgumentException("TreeNodeMetaP is null");
        };
    }

}
