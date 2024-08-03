package com.usatiuk.dhfs.objects.jklepmanntree.serializers;

import com.usatiuk.dhfs.objects.jklepmanntree.structs.JTreeNodeMetaFile;
import com.usatiuk.dhfs.objects.persistence.TreeNodeMetaFileP;
import com.usatiuk.dhfs.objects.protoserializer.ProtoDeserializer;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializer;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class JTreeNodeMetaFileSerializer implements ProtoDeserializer<TreeNodeMetaFileP, JTreeNodeMetaFile>, ProtoSerializer<TreeNodeMetaFileP, JTreeNodeMetaFile> {
    @Override
    public JTreeNodeMetaFile deserialize(TreeNodeMetaFileP message) {
        return new JTreeNodeMetaFile(message.getName(), message.getFileMirror());
    }

    @Override
    public TreeNodeMetaFileP serialize(JTreeNodeMetaFile object) {
        return TreeNodeMetaFileP.newBuilder().setName(object.getName()).setFileMirror(object.getFileIno()).build();
    }
}
