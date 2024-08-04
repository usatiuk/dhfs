package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.dhfs.objects.jkleppmanntree.helpers.JOpWrapper;
import com.usatiuk.dhfs.objects.jkleppmanntree.helpers.OpQueueHelper;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JTreeNodeMeta;
import com.usatiuk.dhfs.objects.repository.invalidation.Op;
import com.usatiuk.dhfs.objects.repository.invalidation.OpQueue;
import com.usatiuk.kleppmanntree.AtomicClock;
import com.usatiuk.kleppmanntree.CombinedTimestamp;
import com.usatiuk.kleppmanntree.LogRecord;
import com.usatiuk.kleppmanntree.OpMove;
import lombok.Getter;

import java.io.Serializable;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

public class JKleppmannTreePersistentData implements Serializable, OpQueue {
    private final String _name;
    @Getter
    private final AtomicClock _clock;
    @Getter
    private final UUID _selfUuid;
    @Getter
    private final ConcurrentHashMap<UUID, ConcurrentLinkedQueue<OpMove<Long, UUID, ? extends JTreeNodeMeta, String>>> _queues = new ConcurrentHashMap<>();
    @Getter
    private final TreeMap<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, ? extends JTreeNodeMeta, String>> _log = new TreeMap<>();
    @Getter
    private final ConcurrentHashMap<UUID, AtomicReference<Long>> _peerTimestampLog = new ConcurrentHashMap<>();

    private transient OpQueueHelper _helper;

    JKleppmannTreePersistentData(OpQueueHelper opQueueHelper, String name, AtomicClock clock) {
        _name = name;
        _clock = clock;
        _selfUuid = opQueueHelper.getSelfUUid();
        restoreHelper(opQueueHelper);
    }

    public void restoreHelper(OpQueueHelper opQueueHelper) {
        _helper = opQueueHelper;
        _helper.onRestore(this);
    }

    @Override
    public Op getForHost(UUID host) {
        if (_queues.containsKey(host)) {
            var peeked = _queues.get(host).peek();
            return peeked != null ? new JOpWrapper(_queues.get(host).peek()) : null;
        }
        return null;
    }

    @Override
    public String getId() {
        return _name;
    }

    @Override
    public void commitOneForHost(UUID host, Op op) {
        var got = _queues.get(host).poll();
        if (!(op instanceof JOpWrapper jw))
            throw new IllegalArgumentException("Unexpected type for commitOneForHost: " + op.getClass().getName());
        if (jw.getOp() != got) {
            throw new IllegalArgumentException("Committed op push was not the oldest");
        }
    }

    void recordOp(OpMove<Long, UUID, ? extends JTreeNodeMeta, String> opMove) {
        for (var u : _helper.getHostList()) {
            _queues.computeIfAbsent(u, h -> new ConcurrentLinkedQueue<>());
            _queues.get(u).add(opMove);
        }
        notifyInvQueue();
    }

    protected void notifyInvQueue() {
        _helper.notifyOpSender(this);
    }
}
