package com.usatiuk.dhfs.objects.jklepmanntree.structs;

import com.usatiuk.kleppmanntree.NodeMeta;
import lombok.Getter;

public abstract class JTreeNodeMeta implements NodeMeta {
    @Getter
    private final String _name;

    public JTreeNodeMeta(String name) {_name = name;}

    public abstract JTreeNodeMeta withName(String name);
}
