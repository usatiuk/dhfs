package com.usatiuk.kleppmanntree;

import lombok.Getter;

public class TestNodeMetaFile extends TestNodeMeta {
    @Getter
    private final long _inode;

    public TestNodeMetaFile(String name, long inode) {
        super(name);
        _inode = inode;
    }

    @Override
    public NodeMeta withName(String name) {
        return new TestNodeMetaFile(name, _inode);
    }
}
