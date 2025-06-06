package com.usatiuk.dhfs.jkleppmanntree.structs;

import com.usatiuk.kleppmanntree.NodeMeta;
import com.usatiuk.objects.JObjectKey;

import java.util.Collection;

public interface JKleppmannTreeNodeMeta extends NodeMeta {
    JKleppmannTreeNodeMeta withName(String name);

    Collection<JObjectKey> collectRefsTo();
}
