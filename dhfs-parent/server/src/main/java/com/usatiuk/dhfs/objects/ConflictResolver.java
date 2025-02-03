package com.usatiuk.dhfs.objects;

public interface ConflictResolver {
    void resolve(PeerId fromPeer, RemoteObject<?> ours, RemoteObject<?> theirs);
}
