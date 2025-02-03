package com.usatiuk.dhfs.objects.repository.invalidation;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.jkleppmanntree.JKleppmannTreeOpWrapper;
import com.usatiuk.dhfs.objects.repository.JKleppmannTreeOpPTemp;
import com.usatiuk.dhfs.utils.SerializationHelper;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class JKleppmannTreeOpPTempSerializer implements ProtoSerializer<JKleppmannTreeOpPTemp, JKleppmannTreeOpWrapper> {
    @Override
    public JKleppmannTreeOpWrapper deserialize(JKleppmannTreeOpPTemp message) {
        return SerializationHelper.deserialize(message.getSerialized().toByteArray());
    }

    @Override
    public JKleppmannTreeOpPTemp serialize(JKleppmannTreeOpWrapper object) {
        return JKleppmannTreeOpPTemp.newBuilder()
                .setSerialized(SerializationHelper.serialize(object))
                .build();
    }
}
