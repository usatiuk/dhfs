package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.dhfs.objects.jkleppmanntree.helpers.OpQueueHelper;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JTreeNodeMeta;
import com.usatiuk.dhfs.objects.repository.invalidation.OpQueue;
import com.usatiuk.kleppmanntree.AtomicClock;
import com.usatiuk.kleppmanntree.CombinedTimestamp;
import com.usatiuk.kleppmanntree.LogOpMove;
import com.usatiuk.kleppmanntree.OpMove;
import lombok.Getter;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class JKleppmannTreePersistentData implements Serializable, OpQueue {
    @Getter
    private final String _name;
    @Getter
    private final AtomicClock _clock;
    @Getter
    private final UUID _selfUuid;
    @Getter
    private final ConcurrentHashMap<UUID, ConcurrentLinkedQueue<OpMove<Long, UUID, JTreeNodeMeta, String>>> _queues = new ConcurrentHashMap<>();
    @Getter
    private final ConcurrentSkipListMap<CombinedTimestamp<Long, UUID>, LogOpMove<Long, UUID, ? extends JTreeNodeMeta, String>> _log = new ConcurrentSkipListMap<>();
    @Getter
    private final ReentrantReadWriteLock _logLock = new ReentrantReadWriteLock();

    private transient OpQueueHelper _helper;

    JKleppmannTreePersistentData(OpQueueHelper opQueueHelper, String name, AtomicClock clock) {
        _name = name;
        _clock = clock;
        _selfUuid = opQueueHelper.getSelfUUid();
        restoreHelper(opQueueHelper);
    }

    public void restoreHelper(OpQueueHelper opQueueHelper) {
        _helper = opQueueHelper;
        _helper.registerOnConnection(this);
    }

    @Override
    public Object getForHost(UUID host) {
        if (_queues.containsKey(host)) {
            return _queues.get(host).poll();
        }
        return null;
    }

    void recordOp(OpMove<Long, UUID, JTreeNodeMeta, String> opMove) {
        for (var u : _helper.getHostList()) {
            _queues.computeIfAbsent(u, h -> new ConcurrentLinkedQueue<>());
            _queues.get(u).add(opMove);
        }
        notifyInvQueue();
    }

    protected void notifyInvQueue() {
    }
}