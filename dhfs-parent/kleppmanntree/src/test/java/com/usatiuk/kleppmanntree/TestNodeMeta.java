package com.usatiuk.kleppmanntree;

import lombok.Getter;

public class TestNodeMeta implements NodeMeta<String> {
    @Getter
    private final String _name;

    public TestNodeMeta(String name) {_name = name;}
}
