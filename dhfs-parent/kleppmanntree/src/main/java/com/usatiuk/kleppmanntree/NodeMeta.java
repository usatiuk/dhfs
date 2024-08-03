package com.usatiuk.kleppmanntree;

import java.io.Serializable;

public interface NodeMeta extends Serializable {
    String getName();

    NodeMeta withName(String name);
}
