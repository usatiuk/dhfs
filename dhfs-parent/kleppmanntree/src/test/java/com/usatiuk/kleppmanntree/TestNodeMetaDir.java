package com.usatiuk.kleppmanntree;

public class TestNodeMetaDir implements NodeMeta<String> {
    private final String name;

    public TestNodeMetaDir(String name) {this.name = name;}

    @Override
    public String getName() {
        return name;
    }
}
