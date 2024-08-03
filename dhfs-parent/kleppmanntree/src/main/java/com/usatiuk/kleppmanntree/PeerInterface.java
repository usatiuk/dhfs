package com.usatiuk.kleppmanntree;

import java.util.Collection;

public interface PeerInterface<PeerIdT extends Comparable<PeerIdT>> {
    PeerIdT getSelfId();
    Collection<PeerIdT> getAllPeers();
}
