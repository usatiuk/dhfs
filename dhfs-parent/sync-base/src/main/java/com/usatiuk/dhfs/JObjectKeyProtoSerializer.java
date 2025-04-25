package com.usatiuk.dhfs;

import com.usatiuk.dhfs.persistence.JObjectKeyP;
import com.usatiuk.objects.JObjectKey;
import jakarta.inject.Singleton;

@Singleton
public class JObjectKeyProtoSerializer implements ProtoSerializer<JObjectKeyP, JObjectKey> {
    @Override
    public JObjectKey deserialize(JObjectKeyP message) {
        return JObjectKey.of(message.getName());
    }

    @Override
    public JObjectKeyP serialize(JObjectKey object) {
        return JObjectKeyP.newBuilder().setName(object.value()).build();
    }
}
