package com.usatiuk.dhfs.objects.jkleppmanntree.structs;

import com.usatiuk.autoprotomap.runtime.ProtoMirror;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeNodeMetaDirectoryP;

//@ProtoMirror(JKleppmannTreeNodeMetaDirectoryP.class)
public class JKleppmannTreeNodeMetaDirectory extends JKleppmannTreeNodeMeta {
    public JKleppmannTreeNodeMetaDirectory(String name) {
        super(name);
    }

    @Override
    public JKleppmannTreeNodeMeta withName(String name) {
        return new JKleppmannTreeNodeMetaDirectory(name);
    }
}
