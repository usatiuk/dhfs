package com.usatiuk.dhfs.objects.jkleppmanntree.serializers;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMetaDirectory;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeNodeMetaDirectoryP;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Singleton;

@Singleton
public class JKleppmannTreeNodeMetaDirectoryProtoSerializer implements ProtoSerializer<JKleppmannTreeNodeMetaDirectoryP, JKleppmannTreeNodeMetaDirectory> {
    @Override
    public JKleppmannTreeNodeMetaDirectory deserialize(JKleppmannTreeNodeMetaDirectoryP message) {
        return new JKleppmannTreeNodeMetaDirectory(message.getName());
    }

    @Override
    public JKleppmannTreeNodeMetaDirectoryP serialize(JKleppmannTreeNodeMetaDirectory object) {
        return JKleppmannTreeNodeMetaDirectoryP.newBuilder().setName(object.getName()).build();
    }
}
