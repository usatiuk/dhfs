package com.usatiuk.dhfs.objects.jkleppmanntree.serializers;

import com.usatiuk.dhfs.SerializationHelper;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeOpLog;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeOpLogP;
import com.usatiuk.dhfs.objects.protoserializer.ProtoDeserializer;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializer;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializerService;
import com.usatiuk.kleppmanntree.CombinedTimestamp;
import com.usatiuk.kleppmanntree.LogRecord;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.TreeMap;
import java.util.UUID;

@ApplicationScoped
public class JKleppmannTreeOpLogProtoSerializer implements ProtoDeserializer<JKleppmannTreeOpLogP, JKleppmannTreeOpLog>, ProtoSerializer<JKleppmannTreeOpLogP, JKleppmannTreeOpLog> {
    @Inject
    ProtoSerializerService protoSerializerService;

    @Override
    public JKleppmannTreeOpLog deserialize(JKleppmannTreeOpLogP message) {
        var map = new TreeMap<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, String>>();
        for (var l : message.getEntriesList()) {
            map.put(new CombinedTimestamp<>(l.getClock(), UUID.fromString(l.getUuid())), SerializationHelper.deserialize(l.getSerialized().newInput()));
        }
        return new JKleppmannTreeOpLog(message.getTreeName(), map);
    }

    @Override
    public JKleppmannTreeOpLogP serialize(JKleppmannTreeOpLog object) {
        var builder = JKleppmannTreeOpLogP.newBuilder()
                .setTreeName(object.getTreeName());
        for (var e : object.getLog().entrySet()) {
            builder.addEntriesBuilder().setClock(e.getKey().timestamp()).setUuid(e.getKey().nodeId().toString())
                    .setSerialized(SerializationHelper.serialize(e.getValue()));
        }
        return builder.build();
    }
}
