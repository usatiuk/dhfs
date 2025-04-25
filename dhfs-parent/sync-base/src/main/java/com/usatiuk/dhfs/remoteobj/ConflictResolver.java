package com.usatiuk.dhfs.remoteobj;

import com.usatiuk.dhfs.peersync.PeerId;

public interface ConflictResolver {
    void resolve(PeerId fromPeer, RemoteObjectMeta ours, RemoteObjectMeta theirs);
}
