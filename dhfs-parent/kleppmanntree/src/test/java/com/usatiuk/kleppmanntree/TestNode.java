package com.usatiuk.kleppmanntree;

import java.util.ArrayList;
import java.util.List;

public class TestNode {
    protected final long _id;

    protected final TestClock _clock;
    protected final TestPeerInterface _peerInterface;
    protected final TestStorageInterface _storageInterface;
    protected final KleppmannTree<Long, Long, TestNodeMeta, Long, TestNodeWrapper> _tree;

    private class TestOpRecorder implements OpRecorder<Long, Long, TestNodeMeta, Long> {
        ArrayList<OpMove<Long, Long, ? extends TestNodeMeta, Long>> ops = new ArrayList<>();

        @Override
        public void recordOp(OpMove<Long, Long, ? extends TestNodeMeta, Long> op) {
            ops.add(op);
        }
    }

    private final TestOpRecorder _recorder;

    public TestNode(long id) {
        _id = id;
        _clock = new TestClock();
        _peerInterface = new TestPeerInterface(_id);
        _storageInterface = new TestStorageInterface(_id);
        _recorder = new TestOpRecorder();
        _tree = new KleppmannTree<>(_storageInterface, _peerInterface, _clock, _recorder);
    }

    List<OpMove<Long, Long, ? extends TestNodeMeta, Long>> getRecorded() {
        var ret = _recorder.ops;
        _recorder.ops = new ArrayList<>();
        return ret;
    }
}
