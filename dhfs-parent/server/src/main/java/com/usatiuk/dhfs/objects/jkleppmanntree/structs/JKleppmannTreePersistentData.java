package com.usatiuk.dhfs.objects.jkleppmanntree.structs;

import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.jrepository.OnlyLocal;
import com.usatiuk.dhfs.objects.repository.ConflictResolver;
import com.usatiuk.kleppmanntree.AtomicClock;
import com.usatiuk.kleppmanntree.CombinedTimestamp;
import com.usatiuk.kleppmanntree.LogRecord;
import com.usatiuk.kleppmanntree.OpMove;
import lombok.Getter;

import java.util.*;

@OnlyLocal
public class JKleppmannTreePersistentData extends JObjectData {
    private final String _treeName;
    @Getter
    private final AtomicClock _clock;
    @Getter
    private final HashMap<UUID, TreeMap<CombinedTimestamp<Long, UUID>, OpMove<Long, UUID, JKleppmannTreeNodeMeta, String>>> _queues;
    @Getter
    private final HashMap<UUID, Long> _peerTimestampLog;
    @Getter
    private final TreeMap<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, String>> _log;

    public JKleppmannTreePersistentData(String treeName, AtomicClock clock,
                                        HashMap<UUID, TreeMap<CombinedTimestamp<Long, UUID>, OpMove<Long, UUID, JKleppmannTreeNodeMeta, String>>> queues,
                                        HashMap<UUID, Long> peerTimestampLog, TreeMap<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, String>> log) {
        _treeName = treeName;
        _clock = clock;
        _queues = queues;
        _peerTimestampLog = peerTimestampLog;
        _log = log;
    }

    public JKleppmannTreePersistentData(String treeName) {
        _treeName = treeName;
        _clock = new AtomicClock(1);
        _queues = new HashMap<>();
        _peerTimestampLog = new HashMap<>();
        _log = new TreeMap<>();
    }

    public static String nameFromTreeName(String treeName) {
        return treeName + "_pd";
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

    @Override
    public String getName() {
        return nameFromTreeName(_treeName);
    }

    public String getTreeName() {
        return _treeName;
    }

    @Override
    public Class<? extends ConflictResolver> getConflictResolver() {
        return null;
    }

    @Override
    public Collection<String> extractRefs() {
        return List.of();
    }
}
