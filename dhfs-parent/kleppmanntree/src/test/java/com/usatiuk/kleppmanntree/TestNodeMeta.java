package com.usatiuk.kleppmanntree;

import lombok.Getter;

public abstract class TestNodeMeta implements NodeMeta {
    @Getter
    private final String _name;

    public TestNodeMeta(String name) {_name = name;}

    abstract public NodeMeta withName(String name);
}
