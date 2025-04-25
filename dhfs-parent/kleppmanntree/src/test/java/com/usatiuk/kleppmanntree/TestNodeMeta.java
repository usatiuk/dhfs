package com.usatiuk.kleppmanntree;

public abstract class TestNodeMeta implements NodeMeta {
    private final String _name;

    public TestNodeMeta(String name) {
        _name = name;
    }

    @Override
    public String name() {
        return _name;
    }

    abstract public NodeMeta withName(String name);
}
