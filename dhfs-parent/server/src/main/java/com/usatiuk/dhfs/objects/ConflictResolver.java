package com.usatiuk.dhfs.objects;

public interface ConflictResolver {
    void resolve(PeerId fromPeer, RemoteObjectMeta ours, RemoteObjectMeta theirs);
}
