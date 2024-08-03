package com.usatiuk.dhfs.objects.jklepmanntree.serializers;

import com.usatiuk.dhfs.objects.jklepmanntree.structs.JTreeNodeMetaDirectory;
import com.usatiuk.dhfs.objects.persistence.TreeNodeMetaDirectoryP;
import com.usatiuk.dhfs.objects.protoserializer.ProtoDeserializer;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializer;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class JTreeNodeMetaDirectorySerializer implements ProtoDeserializer<TreeNodeMetaDirectoryP, JTreeNodeMetaDirectory>, ProtoSerializer<TreeNodeMetaDirectoryP, JTreeNodeMetaDirectory> {
    @Override
    public JTreeNodeMetaDirectory deserialize(TreeNodeMetaDirectoryP message) {
        return new JTreeNodeMetaDirectory(message.getName());
    }

    @Override
    public TreeNodeMetaDirectoryP serialize(JTreeNodeMetaDirectory object) {
        return TreeNodeMetaDirectoryP.newBuilder().setName(object.getName()).build();
    }
}
