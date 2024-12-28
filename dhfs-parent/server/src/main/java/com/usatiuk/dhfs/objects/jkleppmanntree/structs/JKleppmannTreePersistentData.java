package com.usatiuk.dhfs.objects.jkleppmanntree.structs;

import com.usatiuk.kleppmanntree.AtomicClock;
import com.usatiuk.kleppmanntree.CombinedTimestamp;
import com.usatiuk.kleppmanntree.LogRecord;
import com.usatiuk.kleppmanntree.OpMove;
import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;

import java.util.Collection;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.UUID;

public interface JKleppmannTreePersistentData extends JData {
    AtomicClock getClock();

    void setClock(AtomicClock clock);

    HashMap<UUID, TreeMap<CombinedTimestamp<Long, UUID>, OpMove<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey>>> getQueues();

    void setQueues(HashMap<UUID, TreeMap<CombinedTimestamp<Long, UUID>, OpMove<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey>>> queues);

    HashMap<UUID, Long> getPeerTimestampLog();

    void setPeerTimestampLog(HashMap<UUID, Long> peerTimestampLog);

    TreeMap<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey>> getLog();

    void setLog(TreeMap<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey>> log);

    default void recordOp(UUID host, OpMove<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> opMove) {
        getQueues().computeIfAbsent(host, h -> new TreeMap<>());
        getQueues().get(host).put(opMove.timestamp(), opMove);
    }

    default void removeOp(UUID host, OpMove<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> opMove) {
        getQueues().get(host).remove(opMove.timestamp(), opMove);
    }

    default void recordOp(Collection<UUID> hosts, OpMove<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> opMove) {
        for (var u : hosts) {
            recordOp(u, opMove);
        }
    }

    default void removeOp(Collection<UUID> hosts, OpMove<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> opMove) {
        for (var u : hosts) {
            removeOp(u, opMove);
        }
    }

}
