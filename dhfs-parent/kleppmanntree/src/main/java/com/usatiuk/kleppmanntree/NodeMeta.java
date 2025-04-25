package com.usatiuk.kleppmanntree;

import java.io.Serializable;

public interface NodeMeta extends Serializable {
    String name();

    NodeMeta withName(String name);
}
