package com.usatiuk.kleppmanntree;

public class TestNode {
    protected final long _id;

    protected final TestClock _clock;
    protected final TestPeerInterface _peerInterface;
    protected final TestStorageInterface _storageInterface;
    protected final KleppmannTree<Long, Long, TestNodeMeta, Long, TestNodeWrapper> _tree;

    private class TestOpRecorder implements OpRecorder<Long, Long, TestNodeMeta, Long> {

        @Override
        public void recordOp(OpMove<Long, Long, ? extends TestNodeMeta, Long> op) {

        }
    }

    public TestNode(long id) {
        _id = id;
        _clock = new TestClock();
        _peerInterface = new TestPeerInterface(_id);
        _storageInterface = new TestStorageInterface(_id);
        _tree = new KleppmannTree<>(_storageInterface, _peerInterface, _clock, new TestOpRecorder());
    }
}
