package com.usatiuk.dhfs.jkleppmanntree.structs;

import com.usatiuk.dhfs.JDataRef;
import com.usatiuk.dhfs.JDataRefcounted;
import com.usatiuk.dhfs.jkleppmanntree.JKleppmannTreeManager;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.dhfs.PeerId;
import com.usatiuk.kleppmanntree.CombinedTimestamp;
import com.usatiuk.kleppmanntree.LogRecord;
import com.usatiuk.kleppmanntree.OpMove;
import org.pcollections.PCollection;
import org.pcollections.PMap;
import org.pcollections.PSortedMap;

import java.util.Collection;
import java.util.List;

public record JKleppmannTreePersistentData(
        JObjectKey key, PCollection<JDataRef> refsFrom, boolean frozen,
        long clock,
        PMap<PeerId, PSortedMap<CombinedTimestamp<Long, PeerId>, OpMove<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey>>> queues,
        PMap<PeerId, Long> peerTimestampLog,
        PSortedMap<CombinedTimestamp<Long, PeerId>, LogRecord<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey>> log
) implements JDataRefcounted {
    @Override
    public JKleppmannTreePersistentData withRefsFrom(PCollection<JDataRef> refs) {
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

    public JKleppmannTreePersistentData withPeerTimestampLog(PMap<PeerId, Long> peerTimestampLog) {
        return new JKleppmannTreePersistentData(key, refsFrom, frozen, clock, queues, peerTimestampLog, log);
    }

    public JKleppmannTreePersistentData withLog(PSortedMap<CombinedTimestamp<Long, PeerId>, LogRecord<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey>> log) {
        return new JKleppmannTreePersistentData(key, refsFrom, frozen, clock, queues, peerTimestampLog, log);
    }

    @Override
    public Collection<JObjectKey> collectRefsTo() {
        return List.of(JObjectKey.of(key().value().concat(JKleppmannTreeManager.ROOT_SUFFIX)), JObjectKey.of(key().value().concat(JKleppmannTreeManager.TRASH_SUFFIX)), JObjectKey.of(key().value().concat(JKleppmannTreeManager.LF_SUFFIX)));
    }
}
