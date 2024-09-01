package com.usatiuk.dhfs.objects.jkleppmanntree.serializers;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.SerializationHelper;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeOpLog;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeOpLogP;
import com.usatiuk.kleppmanntree.CombinedTimestamp;
import com.usatiuk.kleppmanntree.LogRecord;
import jakarta.inject.Singleton;

import java.util.TreeMap;
import java.util.UUID;

@Singleton
public class JKleppmannTreeOpLogProtoSerializer implements ProtoSerializer<JKleppmannTreeOpLogP, JKleppmannTreeOpLog> {

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
