package com.usatiuk.kleppmanntree;

import java.io.Serializable;

public interface NodeMeta<NameT> extends Serializable {
    public NameT getName();
}
