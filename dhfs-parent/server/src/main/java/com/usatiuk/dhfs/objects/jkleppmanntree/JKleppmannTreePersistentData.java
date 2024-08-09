package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.kleppmanntree.AtomicClock;
import com.usatiuk.kleppmanntree.CombinedTimestamp;
import com.usatiuk.kleppmanntree.LogRecord;
import com.usatiuk.kleppmanntree.OpMove;
import lombok.Getter;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

public class JKleppmannTreePersistentData implements Serializable {
    @Getter
    private final String _name;
    @Getter
    private final AtomicClock _clock;
    @Getter
    private final HashMap<UUID, TreeMap<CombinedTimestamp<Long, UUID>, OpMove<Long, UUID, JKleppmannTreeNodeMeta, String>>> _queues = new HashMap<>();
    @Getter
    private final TreeMap<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, String>> _log = new TreeMap<>();
    @Getter
    private final HashMap<UUID, AtomicReference<Long>> _peerTimestampLog = new HashMap<>();

    JKleppmannTreePersistentData(String name, AtomicClock clock) {
        _name = name;
        _clock = clock;
    }

    public void recordOp(UUID host, OpMove<Long, UUID, JKleppmannTreeNodeMeta, String> opMove) {
        _queues.computeIfAbsent(host, h -> new TreeMap<>());
        _queues.get(host).put(opMove.timestamp(), opMove);
    }

    public void recordOp(Collection<UUID> hosts, OpMove<Long, UUID, JKleppmannTreeNodeMeta, String> opMove) {
        for (var u : hosts) {
            recordOp(u, opMove);
        }
    }
}
