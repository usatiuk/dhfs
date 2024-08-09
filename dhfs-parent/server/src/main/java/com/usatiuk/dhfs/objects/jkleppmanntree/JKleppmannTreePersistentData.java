package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.kleppmanntree.AtomicClock;
import com.usatiuk.kleppmanntree.CombinedTimestamp;
import com.usatiuk.kleppmanntree.LogRecord;
import com.usatiuk.kleppmanntree.OpMove;
import lombok.Getter;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class JKleppmannTreePersistentData implements Serializable {
    @Getter
    private final String _name;
    @Getter
    private final AtomicClock _clock;
    @Getter
    private final HashMap<UUID, Queue<OpMove<Long, UUID, ? extends JKleppmannTreeNodeMeta, String>>> _queues = new HashMap<>();
    @Getter
    private final TreeMap<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, ? extends JKleppmannTreeNodeMeta, String>> _log = new TreeMap<>();
    @Getter
    private final HashMap<UUID, AtomicReference<Long>> _peerTimestampLog = new HashMap<>();

    JKleppmannTreePersistentData(String name, AtomicClock clock) {
        _name = name;
        _clock = clock;
    }

    public void recordOp(Collection<UUID> hosts, OpMove<Long, UUID, ? extends JKleppmannTreeNodeMeta, String> opMove) {
        for (var u : hosts) {
            _queues.computeIfAbsent(u, h -> new LinkedList<>());
            _queues.get(u).add(opMove);
        }
    }
}
