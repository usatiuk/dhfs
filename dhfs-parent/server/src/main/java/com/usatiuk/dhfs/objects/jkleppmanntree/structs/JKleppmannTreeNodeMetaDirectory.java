package com.usatiuk.dhfs.objects.jkleppmanntree.structs;

public class JKleppmannTreeNodeMetaDirectory extends JKleppmannTreeNodeMeta {
    public JKleppmannTreeNodeMetaDirectory(String name) {
        super(name);
    }

    @Override
    public JKleppmannTreeNodeMeta withName(String name) {
        return new JKleppmannTreeNodeMetaDirectory(name);
    }
}
