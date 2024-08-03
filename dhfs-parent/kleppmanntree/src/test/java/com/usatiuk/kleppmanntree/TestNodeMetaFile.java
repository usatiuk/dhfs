package com.usatiuk.kleppmanntree;

import lombok.Getter;

public class TestNodeMetaFile implements NodeMeta<String> {
    private final String name;
    @Getter
    private final long inode;

    public TestNodeMetaFile(String name, long inode) {
        this.name = name;
        this.inode = inode;
    }

    @Override
    public String getName() {
        return name;
    }
}
