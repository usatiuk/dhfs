package com.usatiuk.kleppmanntree;

public class TestNodeMetaFile extends TestNodeMeta {
    private final long _inode;

    public TestNodeMetaFile(String name, long inode) {
        super(name);
        _inode = inode;
    }

    public long getInode() {
        return _inode;
    }

    @Override
    public NodeMeta withName(String name) {
        return new TestNodeMetaFile(name, _inode);
    }
}
