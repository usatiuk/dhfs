package com.usatiuk.dhfs.objects.jkleppmanntree.structs;

import com.usatiuk.kleppmanntree.NodeMeta;
import lombok.Getter;

public abstract class JKleppmannTreeNodeMeta implements NodeMeta {
    @Getter
    private final String _name;

    public JKleppmannTreeNodeMeta(String name) {_name = name;}

    public abstract JKleppmannTreeNodeMeta withName(String name);
}
