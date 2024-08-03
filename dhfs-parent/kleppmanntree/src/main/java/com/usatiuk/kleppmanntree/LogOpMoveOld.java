package com.usatiuk.kleppmanntree;

public record LogOpMoveOld<NameT, MetaT extends NodeMeta<NameT>, NodeIdT>
        (NodeIdT oldParent, MetaT oldMeta) {}
