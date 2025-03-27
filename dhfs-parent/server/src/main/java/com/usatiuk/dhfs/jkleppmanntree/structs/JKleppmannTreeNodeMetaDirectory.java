package com.usatiuk.dhfs.jkleppmanntree.structs;

public class JKleppmannTreeNodeMetaDirectory extends JKleppmannTreeNodeMeta {
    public JKleppmannTreeNodeMetaDirectory(String name) {
        super(name);
    }

    @Override
    public JKleppmannTreeNodeMeta withName(String name) {
        return new JKleppmannTreeNodeMetaDirectory(name);
    }
}
