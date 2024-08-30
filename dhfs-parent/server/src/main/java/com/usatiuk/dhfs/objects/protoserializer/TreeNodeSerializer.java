package com.usatiuk.dhfs.objects.protoserializer;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMetaDirectory;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMetaFile;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeNodeMetaDirectoryP;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeNodeMetaFileP;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeNodeMetaP;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class TreeNodeSerializer implements ProtoSerializer<JKleppmannTreeNodeMetaP, JKleppmannTreeNodeMeta> {
    @Inject
    ProtoSerializer<JKleppmannTreeNodeMetaFileP, JKleppmannTreeNodeMetaFile> fileProtoSerializer;
    @Inject
    ProtoSerializer<JKleppmannTreeNodeMetaDirectoryP, JKleppmannTreeNodeMetaDirectory> directoryProtoSerializer;

    @Override
    public JKleppmannTreeNodeMeta deserialize(JKleppmannTreeNodeMetaP message) {
        return switch (message.getMetaCase()) {
            case FILE -> (JKleppmannTreeNodeMetaFile) fileProtoSerializer.deserialize(message.getFile());
            case DIR -> (JKleppmannTreeNodeMetaDirectory) directoryProtoSerializer.deserialize(message.getDir());
            case META_NOT_SET -> throw new IllegalArgumentException("TreeNodeMetaP is null");
        };
    }

    @Override
    public JKleppmannTreeNodeMetaP serialize(JKleppmannTreeNodeMeta object) {
        if (object == null) throw new IllegalArgumentException("Object to serialize shouldn't be null");
        if (object instanceof JKleppmannTreeNodeMetaDirectory dir) {
            return JKleppmannTreeNodeMetaP.newBuilder().setDir(
                    directoryProtoSerializer.serialize(dir)
            ).build();
        } else if (object instanceof JKleppmannTreeNodeMetaFile file) {
            return JKleppmannTreeNodeMetaP.newBuilder().setFile(
                    fileProtoSerializer.serialize(file)
            ).build();
        } else {
            throw new IllegalArgumentException("Unexpected object type on input to serializeToTreeNodeMetaP: " + object.getClass());
        }
    }
}
