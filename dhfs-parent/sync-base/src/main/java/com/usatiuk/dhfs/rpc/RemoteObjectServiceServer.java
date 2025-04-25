package com.usatiuk.dhfs.rpc;

import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.dhfs.repository.*;
import io.quarkus.grpc.GrpcService;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;

// Note: RunOnVirtualThread hangs somehow
@GrpcService
@RolesAllowed("cluster-member")
public class RemoteObjectServiceServer implements DhfsObjectSyncGrpc {
    @Inject
    SecurityIdentity identity;
    @Inject
    RemoteObjectServiceServerImpl remoteObjectServiceServerImpl;

    private PeerId getIdentity() {
        return PeerId.of(identity.getPrincipal().getName().substring(3));
    }

    @Override
    @Blocking
    public Uni<GetObjectReply> getObject(GetObjectRequest request) {
        return remoteObjectServiceServerImpl.getObject(getIdentity(), request);
    }

    @Override
    @Blocking
    public Uni<CanDeleteReply> canDelete(CanDeleteRequest request) {
        return remoteObjectServiceServerImpl.canDelete(getIdentity(), request);
    }

    @Override
    @Blocking
    public Uni<OpPushReply> opPush(OpPushRequest request) {
        return remoteObjectServiceServerImpl.opPush(getIdentity(), request);
    }

    @Override
    @Blocking
    public Uni<PingReply> ping(PingRequest request) {
        return remoteObjectServiceServerImpl.ping(getIdentity(), request);
    }
}
