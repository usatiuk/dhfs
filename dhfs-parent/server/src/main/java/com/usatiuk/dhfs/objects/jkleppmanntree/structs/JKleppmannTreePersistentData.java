package com.usatiuk.dhfs.objects.jkleppmanntree.structs;

import com.usatiuk.dhfs.objects.JDataRefcounted;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.PeerId;
import com.usatiuk.kleppmanntree.CombinedTimestamp;
import com.usatiuk.kleppmanntree.LogRecord;
import com.usatiuk.kleppmanntree.OpMove;
import org.pcollections.PCollection;
import org.pcollections.PMap;
import org.pcollections.PSortedMap;
import org.pcollections.TreePMap;

import java.util.*;

public record JKleppmannTreePersistentData(
        JObjectKey key, PCollection<JObjectKey> refsFrom, boolean frozen,
        long clock,
        PMap<PeerId, PSortedMap<CombinedTimestamp<Long, PeerId>, OpMove<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey>>> queues,
        HashMap<PeerId, Long> peerTimestampLog,
        TreeMap<CombinedTimestamp<Long, PeerId>, LogRecord<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey>> log
) implements JDataRefcounted {
    @Override
    public JKleppmannTreePersistentData withRefsFrom(PCollection<JObjectKey> refs) {
        return new JKleppmannTreePersistentData(key, refs, frozen, clock, queues, peerTimestampLog, log);
    }

    @Override
    public JKleppmannTreePersistentData withFrozen(boolean frozen) {
        return new JKleppmannTreePersistentData(key, refsFrom, frozen, clock, queues, peerTimestampLog, log);
    }

    public JKleppmannTreePersistentData withClock(long clock) {
        return new JKleppmannTreePersistentData(key, refsFrom, frozen, clock, queues, peerTimestampLog, log);
    }

    public JKleppmannTreePersistentData withQueues(PMap<PeerId, PSortedMap<CombinedTimestamp<Long, PeerId>, OpMove<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey>>> queues) {
        return new JKleppmannTreePersistentData(key, refsFrom, frozen, clock, queues, peerTimestampLog, log);
    }

    public JKleppmannTreePersistentData withPeerTimestampLog(HashMap<PeerId, Long> peerTimestampLog) {
        return new JKleppmannTreePersistentData(key, refsFrom, frozen, clock, queues, peerTimestampLog, log);
    }

    public JKleppmannTreePersistentData withLog(TreeMap<CombinedTimestamp<Long, PeerId>, LogRecord<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey>> log) {
        return new JKleppmannTreePersistentData(key, refsFrom, frozen, clock, queues, peerTimestampLog, log);
    }

    @Override
    public Collection<JObjectKey> collectRefsTo() {
        return List.of(new JObjectKey(key().name() + "_jt_trash"), new JObjectKey(key().name() + "_jt_root"));
    }
}
