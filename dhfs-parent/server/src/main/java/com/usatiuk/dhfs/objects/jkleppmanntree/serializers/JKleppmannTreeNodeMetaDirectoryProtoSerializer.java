package com.usatiuk.dhfs.objects.jkleppmanntree.serializers;

import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMetaDirectory;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeNodeMetaDirectoryP;
import com.usatiuk.dhfs.objects.protoserializer.ProtoDeserializer;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializer;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class JKleppmannTreeNodeMetaDirectoryProtoSerializer implements ProtoDeserializer<JKleppmannTreeNodeMetaDirectoryP, JKleppmannTreeNodeMetaDirectory>, ProtoSerializer<JKleppmannTreeNodeMetaDirectoryP, JKleppmannTreeNodeMetaDirectory> {
    @Override
    public JKleppmannTreeNodeMetaDirectory deserialize(JKleppmannTreeNodeMetaDirectoryP message) {
        return new JKleppmannTreeNodeMetaDirectory(message.getName());
    }

    @Override
    public JKleppmannTreeNodeMetaDirectoryP serialize(JKleppmannTreeNodeMetaDirectory object) {
        return JKleppmannTreeNodeMetaDirectoryP.newBuilder().setName(object.getName()).build();
    }
}
