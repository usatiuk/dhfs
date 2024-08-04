package com.usatiuk.kleppmanntree;

import java.io.Serializable;

public record LogEffectOld<MetaT extends NodeMeta, NodeIdT>
        (NodeIdT oldParent, MetaT oldMeta) implements Serializable {}
