package com.usatiuk.kleppmanntree;

import java.io.Serializable;

public record LogOpMoveOld<NameT, MetaT extends NodeMeta<NameT>, NodeIdT>
        (NodeIdT oldParent, MetaT oldMeta) implements Serializable {}
