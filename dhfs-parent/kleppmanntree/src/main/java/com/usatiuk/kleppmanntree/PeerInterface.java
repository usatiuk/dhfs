package com.usatiuk.kleppmanntree;

import java.util.Collection;

public interface PeerInterface<PeerIdT extends Comparable<PeerIdT>> {
    public PeerIdT getSelfId();
    public Collection<PeerIdT> getAllPeers();
}
