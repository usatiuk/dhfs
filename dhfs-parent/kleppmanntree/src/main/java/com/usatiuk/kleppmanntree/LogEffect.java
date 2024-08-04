package com.usatiuk.kleppmanntree;

import java.io.Serializable;

public record LogEffect<MetaT extends NodeMeta, NodeIdT>(
        LogEffectOld<MetaT, NodeIdT> oldInfo,
        NodeIdT newParentId,
        MetaT newMeta,
        NodeIdT childId) implements Serializable {
}
