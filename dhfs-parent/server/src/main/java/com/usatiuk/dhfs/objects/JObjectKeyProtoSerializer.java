package com.usatiuk.dhfs.objects;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.persistence.JObjectKeyP;
import jakarta.inject.Singleton;

@Singleton
public class JObjectKeyProtoSerializer implements ProtoSerializer<JObjectKeyP, JObjectKey> {
    @Override
    public JObjectKey deserialize(JObjectKeyP message) {
        return JObjectKey.of(message.getName());
    }

    @Override
    public JObjectKeyP serialize(JObjectKey object) {
        return JObjectKeyP.newBuilder().setName(object.name()).build();
    }
}
