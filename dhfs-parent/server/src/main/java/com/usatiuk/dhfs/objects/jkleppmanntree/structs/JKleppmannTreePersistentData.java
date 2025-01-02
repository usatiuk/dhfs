package com.usatiuk.dhfs.objects.jkleppmanntree.structs;

import com.usatiuk.dhfs.objects.JDataRefcounted;
import com.usatiuk.kleppmanntree.CombinedTimestamp;
import com.usatiuk.kleppmanntree.LogRecord;
import com.usatiuk.kleppmanntree.OpMove;
import com.usatiuk.dhfs.objects.JObjectKey;
import lombok.Builder;

import java.util.*;

@Builder(toBuilder = true)
public record JKleppmannTreePersistentData(
        JObjectKey key, Collection<JObjectKey> refsFrom, boolean frozen,
        long clock,
        HashMap<UUID, TreeMap<CombinedTimestamp<Long, UUID>, OpMove<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey>>> queues,
        HashMap<UUID, Long> peerTimestampLog,
        TreeMap<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey>> log
) implements JDataRefcounted {
    void recordOp(UUID host, OpMove<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> opMove) {
        queues().computeIfAbsent(host, h -> new TreeMap<>());
        queues().get(host).put(opMove.timestamp(), opMove);
    }

    void removeOp(UUID host, OpMove<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> opMove) {
        queues().get(host).remove(opMove.timestamp(), opMove);
    }

    void recordOp(Collection<UUID> hosts, OpMove<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> opMove) {
        for (var u : hosts) {
            recordOp(u, opMove);
        }
    }

    void removeOp(Collection<UUID> hosts, OpMove<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> opMove) {
        for (var u : hosts) {
            removeOp(u, opMove);
        }
    }

    @Override
    public JKleppmannTreePersistentData withRefsFrom(Collection<JObjectKey> refs) {
        return this.toBuilder().refsFrom(refs).build();
    }

    @Override
    public JKleppmannTreePersistentData withFrozen(boolean frozen) {
        return this.toBuilder().frozen(frozen).build();
    }

    @Override
    public Collection<JObjectKey> collectRefsTo() {
        return List.of(new JObjectKey(key().name() + "_jt_trash"), new JObjectKey(key().name() + "_jt_root"));
    }
}
