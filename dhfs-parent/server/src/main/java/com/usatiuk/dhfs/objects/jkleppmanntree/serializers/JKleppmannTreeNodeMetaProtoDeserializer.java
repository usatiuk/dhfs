package com.usatiuk.dhfs.objects.jkleppmanntree.serializers;

import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMetaDirectory;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMetaFile;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeNodeMetaP;
import com.usatiuk.dhfs.objects.protoserializer.ProtoDeserializer;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializerService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class JKleppmannTreeNodeMetaProtoDeserializer implements ProtoDeserializer<JKleppmannTreeNodeMetaP, JKleppmannTreeNodeMeta> {
    @Inject
    ProtoSerializerService protoSerializerService;

    @Override
    public JKleppmannTreeNodeMeta deserialize(JKleppmannTreeNodeMetaP message) {
        return switch (message.getMetaCase()) {
            case FILE -> (JKleppmannTreeNodeMetaFile) protoSerializerService.deserialize(message.getFile());
            case DIR -> (JKleppmannTreeNodeMetaDirectory) protoSerializerService.deserialize(message.getDir());
            case META_NOT_SET -> throw new IllegalArgumentException("TreeNodeMetaP is null");
        };
    }

}
