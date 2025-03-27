package com.usatiuk.dhfs;

public interface ConflictResolver {
    void resolve(PeerId fromPeer, RemoteObjectMeta ours, RemoteObjectMeta theirs);
}
