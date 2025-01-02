package com.usatiuk.kleppmanntree;

public abstract class TestNodeMeta implements NodeMeta {
    private final String _name;

    @Override
    public String getName() {
        return _name;
    }

    public TestNodeMeta(String name) {
        _name = name;
    }

    abstract public NodeMeta withName(String name);
}
