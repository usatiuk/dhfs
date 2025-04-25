package com.usatiuk.dhfsfs.objects;

import com.usatiuk.dhfs.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.objects.JObjectKey;

import java.util.Collection;
import java.util.List;

public class JKleppmannTreeNodeMetaDirectory extends JKleppmannTreeNodeMeta {
    public JKleppmannTreeNodeMetaDirectory(String name) {
        super(name);
    }

    @Override
    public JKleppmannTreeNodeMeta withName(String name) {
        return new JKleppmannTreeNodeMetaDirectory(name);
    }

    @Override
    public Collection<JObjectKey> collectRefsTo() {
        return List.of();
    }
}
