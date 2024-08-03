package com.usatiuk.kleppmanntree;

public class TestNodeMetaDir extends TestNodeMeta {
    public TestNodeMetaDir(String name) {
        super(name);
    }

    @Override
    public NodeMeta withName(String name) {
        return new TestNodeMetaDir(name);
    }
}
