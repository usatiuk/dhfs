package com.usatiuk.dhfs.objects.jkleppmanntree.structs;

public class JTreeNodeMetaDirectory extends JTreeNodeMeta {
    public JTreeNodeMetaDirectory(String name) {
        super(name);
    }

    @Override
    public JTreeNodeMeta withName(String name) {
        return new JTreeNodeMetaDirectory(name);
    }
}
