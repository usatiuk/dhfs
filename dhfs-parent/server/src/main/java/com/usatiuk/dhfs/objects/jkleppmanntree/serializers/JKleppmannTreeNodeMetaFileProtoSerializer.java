package com.usatiuk.dhfs.objects.jkleppmanntree.serializers;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMetaFile;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeNodeMetaFileP;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Singleton;

@Singleton
public class JKleppmannTreeNodeMetaFileProtoSerializer implements ProtoSerializer<JKleppmannTreeNodeMetaFileP, JKleppmannTreeNodeMetaFile> {
    @Override
    public JKleppmannTreeNodeMetaFile deserialize(JKleppmannTreeNodeMetaFileP message) {
        return new JKleppmannTreeNodeMetaFile(message.getName(), message.getFileMirror());
    }

    @Override
    public JKleppmannTreeNodeMetaFileP serialize(JKleppmannTreeNodeMetaFile object) {
        return JKleppmannTreeNodeMetaFileP.newBuilder().setName(object.getName()).setFileMirror(object.getFileIno()).build();
    }
}
