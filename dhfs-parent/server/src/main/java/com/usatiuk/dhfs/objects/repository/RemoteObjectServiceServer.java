package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.*;
import com.usatiuk.dhfs.objects.persistence.JObjectKeyP;
import com.usatiuk.dhfs.objects.repository.invalidation.InvalidationQueueService;
import com.usatiuk.dhfs.objects.repository.invalidation.Op;
import com.usatiuk.dhfs.objects.repository.invalidation.OpHandler;
import com.usatiuk.dhfs.objects.repository.syncmap.DtoMapperService;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import com.usatiuk.dhfs.objects.transaction.TransactionManager;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcService;
import io.quarkus.logging.Log;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

// Note: RunOnVirtualThread hangs somehow
@GrpcService
@RolesAllowed("cluster-member")
public class RemoteObjectServiceServer implements DhfsObjectSyncGrpc {
    @Inject
    SecurityIdentity identity;
    @Inject
    RemoteObjectServiceServerImpl remoteObjectServiceServerImpl;

    PeerId getIdentity() {
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
