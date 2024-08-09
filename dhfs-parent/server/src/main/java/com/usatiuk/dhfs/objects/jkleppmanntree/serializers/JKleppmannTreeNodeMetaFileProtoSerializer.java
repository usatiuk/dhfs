package com.usatiuk.dhfs.objects.jkleppmanntree.serializers;

import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMetaFile;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeNodeMetaFileP;
import com.usatiuk.dhfs.objects.protoserializer.ProtoDeserializer;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializer;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class JKleppmannTreeNodeMetaFileProtoSerializer implements ProtoDeserializer<JKleppmannTreeNodeMetaFileP, JKleppmannTreeNodeMetaFile>, ProtoSerializer<JKleppmannTreeNodeMetaFileP, JKleppmannTreeNodeMetaFile> {
    @Override
    public JKleppmannTreeNodeMetaFile deserialize(JKleppmannTreeNodeMetaFileP message) {
        return new JKleppmannTreeNodeMetaFile(message.getName(), message.getFileMirror());
    }

    @Override
    public JKleppmannTreeNodeMetaFileP serialize(JKleppmannTreeNodeMetaFile object) {
        return JKleppmannTreeNodeMetaFileP.newBuilder().setName(object.getName()).setFileMirror(object.getFileIno()).build();
    }
}
