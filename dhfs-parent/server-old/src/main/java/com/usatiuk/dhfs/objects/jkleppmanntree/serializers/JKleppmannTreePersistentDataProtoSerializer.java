package com.usatiuk.dhfs.objects.jkleppmanntree.serializers;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.jkleppmanntree.JKleppmannTreeOpWrapper;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreePersistentData;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeOpLogEffectP;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeOpP;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreePersistentDataP;
import com.usatiuk.kleppmanntree.*;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.HashMap;
import java.util.TreeMap;
import java.util.UUID;

@Singleton
public class JKleppmannTreePersistentDataProtoSerializer implements ProtoSerializer<JKleppmannTreePersistentDataP, JKleppmannTreePersistentData> {
    @Inject
    ProtoSerializer<JKleppmannTreeOpP, JKleppmannTreeOpWrapper> opProtoSerializer;
    @Inject
    ProtoSerializer<JKleppmannTreeOpLogEffectP, LogEffect<Long, UUID, JKleppmannTreeNodeMeta, String>> effectProtoSerializer;

    @Override
    public JKleppmannTreePersistentData deserialize(JKleppmannTreePersistentDataP message) {
        HashMap<UUID, TreeMap<CombinedTimestamp<Long, UUID>, OpMove<Long, UUID, JKleppmannTreeNodeMeta, String>>> queues = new HashMap<>();

        for (var q : message.getQueuesList()) {
            var qmap = new TreeMap<CombinedTimestamp<Long, UUID>, OpMove<Long, UUID, JKleppmannTreeNodeMeta, String>>();
            for (var o : q.getEntriesList()) {
                var op = (JKleppmannTreeOpWrapper) opProtoSerializer.deserialize(o.getOp());
                qmap.put(new CombinedTimestamp<>(o.getClock(), UUID.fromString(o.getUuid())), op.getOp());
            }
            queues.put(UUID.fromString(q.getNode()), qmap);
        }

        var log = new HashMap<UUID, Long>();

        for (var l : message.getPeerLogList()) {
            log.put(UUID.fromString(l.getHost()), l.getTimestamp());
        }

        var opLog = new TreeMap<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, String>>();
        for (var l : message.getOpLogList()) {
            opLog.put(new CombinedTimestamp<>(l.getClock(), UUID.fromString(l.getUuid())),
                    new LogRecord<>(opProtoSerializer.deserialize(l.getOp()).getOp(), l.getEffectsList().stream().map(effectProtoSerializer::deserialize).toList())
            );
        }

        return new JKleppmannTreePersistentData(
                message.getTreeName(),
                new AtomicClock(message.getClock()),
                queues,
                log,
                opLog
        );
    }

    @Override
    public JKleppmannTreePersistentDataP serialize(JKleppmannTreePersistentData object) {
        var builder = JKleppmannTreePersistentDataP.newBuilder()
                .setTreeName(object.getTreeName())
                .setClock(object.getClock().peekTimestamp());
        for (var q : object.getQueues().entrySet()) {
            if (q.getValue().isEmpty()) continue;
            var qb = builder.addQueuesBuilder();
            qb.setNode(q.getKey().toString());
            for (var e : q.getValue().entrySet()) {
                qb.addEntriesBuilder().setClock(e.getKey().timestamp()).setUuid(e.getKey().nodeId().toString())
                        .setOp((JKleppmannTreeOpP) opProtoSerializer.serialize(new JKleppmannTreeOpWrapper(e.getValue())));
            }
        }
        for (var peerLogEntry : object.getPeerTimestampLog().entrySet()) {
            builder.addPeerLogBuilder().setHost(peerLogEntry.getKey().toString()).setTimestamp(peerLogEntry.getValue());
        }
        for (var e : object.getLog().entrySet()) {
            builder.addOpLogBuilder()
                    .setClock(e.getKey().timestamp())
                    .setUuid(e.getKey().nodeId().toString())
                    .setOp(opProtoSerializer.serialize(new JKleppmannTreeOpWrapper(e.getValue().op())))
                    .addAllEffects(e.getValue().effects().stream().map(effectProtoSerializer::serialize).toList());
        }
        return builder.build();
    }
}
