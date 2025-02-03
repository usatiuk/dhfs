package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.*;
import com.usatiuk.dhfs.objects.repository.invalidation.InvalidationQueueService;
import com.usatiuk.dhfs.objects.repository.invalidation.Op;
import com.usatiuk.dhfs.objects.repository.invalidation.OpHandler;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcService;
import io.quarkus.logging.Log;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;

// Note: RunOnVirtualThread hangs somehow
@GrpcService
@RolesAllowed("cluster-member")
public class RemoteObjectServiceServer implements DhfsObjectSyncGrpc {
//    @Inject
//    SyncHandler syncHandler;

    @Inject
    TransactionManager txm;
    @Inject
    PeerManager peerManager;
    @Inject
    Transaction curTx;
    @Inject
    PersistentPeerDataService persistentPeerDataService;

    @Inject
    InvalidationQueueService invalidationQueueService;
    @Inject
    SecurityIdentity identity;
    //    @Inject
//    ProtoSerializer<JObjectDataP, JObjectData> dataProtoSerializer;
    @Inject
    ProtoSerializer<OpPushPayload, Op> opProtoSerializer;
    @Inject
    ProtoSerializer<GetObjectReply, ReceivedObject> receivedObjectProtoSerializer;
    @Inject
    RemoteTransaction remoteTx;
    @Inject
    OpHandler opHandler;

    @Override
    @Blocking
    public Uni<GetObjectReply> getObject(GetObjectRequest request) {
        Log.info("<-- getObject: " + request.getName() + " from " + identity.getPrincipal().getName().substring(3));

        var obj = txm.run(() -> {
            var got = remoteTx.get(JDataRemote.class, JObjectKey.of(request.getName())).orElse(null);
            if (got == null) {
                Log.info("<-- getObject NOT FOUND: " + request.getName() + " from " + identity.getPrincipal().getName().substring(3));
                throw new StatusRuntimeException(Status.NOT_FOUND);
            }
            return got;
        });

        var serialized = receivedObjectProtoSerializer.serialize(obj.toReceivedObject());
        return Uni.createFrom().item(serialized);
//        // Does @Blocking break this?
//        return Uni.createFrom().emitter(emitter -> {
//            try {
//            } catch (Exception e) {
//                emitter.fail(e);
//            }
//            var replyObj = txm.run(() -> {
//                var cur = curTx.get(JDataRemote.class, JObjectKey.of(request.getName())).orElseThrow(() -> new StatusRuntimeException(Status.NOT_FOUND));
//                // Obj.markSeen before markSeen of its children
//                obj.markSeen();
//                return obj.runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (meta, data) -> {
//                    if (meta.isOnlyLocal())
//                        throw new StatusRuntimeExceptionNoStacktrace(Status.INVALID_ARGUMENT.withDescription("Trying to get local-only object"));
//                    if (data == null) {
//                        Log.info("<-- getObject FAIL: " + request.getName() + " from " + request.getSelfUuid());
//                        throw new StatusRuntimeException(Status.ABORTED.withDescription("Not available locally"));
//                    }
//                    data.extractRefs().forEach(ref ->
//                            jObjectManager.get(ref)
//                                    .orElseThrow(() -> new IllegalStateException("Non-hydrated refs for local object?"))
//                                    .markSeen());
//
//                    return ApiObject.newBuilder()
//                            .setHeader(obj.getMeta().toRpcHeader())
//                            .setContent(dataProtoSerializer.serialize(obj.getData())).build();
//                });
//            });
//            var ret = GetObjectReply.newBuilder()
//                    .setSelfUuid(persistentPeerDataService.getSelfUuid().toString())
//                    .setObject(replyObj).build();
//            emitter.complete(ret);
//            // TODO: Could this cause problems if we wait for too long?
////            obj.commitFenceAsync(() -> emitter.complete(ret));
//        });
    }

    @Override
    public Uni<CanDeleteReply> canDelete(CanDeleteRequest request) {
        throw new NotImplementedException();
    }

//    @Override
//    @Blocking
//    public Uni<CanDeleteReply> canDelete(CanDeleteRequest request) {
//        if (request.getSelfUuid().isBlank()) throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
//        if (!persistentPeerDataService.existsHost(UUID.fromString(request.getSelfUuid())))
//            throw new StatusRuntimeException(Status.UNAUTHENTICATED);
//
//        Log.info("<-- canDelete: " + request.getName() + " from " + request.getSelfUuid());
//
//        var builder = CanDeleteReply.newBuilder();
//
//        var obj = jObjectManager.get(request.getName());
//
//        builder.setSelfUuid(persistentPeerDataService.getSelfUuid().toString());
//        builder.setObjName(request.getName());
//
//        if (obj.isPresent()) try {
//            boolean tryUpdate = obj.get().runReadLocked(JObjectManager.ResolutionStrategy.NO_RESOLUTION, (m, d) -> {
//                if (m.isDeleted() && !m.isDeletionCandidate())
//                    throw new IllegalStateException("Object " + m.getName() + " is deleted but not a deletion candidate");
//                builder.setDeletionCandidate(m.isDeletionCandidate());
//                builder.addAllReferrers(m.getReferrers());
//                return m.isDeletionCandidate() && !m.isDeleted();
//            });
//            // FIXME

    /// /            if (tryUpdate) {
    /// /                obj.get().runWriteLocked(JObjectManager.ResolutionStrategy.NO_RESOLUTION, (m, d, b, v) -> {
    /// /                    return null;
    /// /                });
    /// /            }
//        } catch (DeletedObjectAccessException dox) {
//            builder.setDeletionCandidate(true);
//        }
//        else {
//            builder.setDeletionCandidate(true);
//        }
//
//        var ret = builder.build();
//
//        if (!ret.getDeletionCandidate())
//            for (var rr : request.getOurReferrersList())
//                autoSyncProcessor.add(rr);
//
//        return Uni.createFrom().item(ret);
//    }

//    @Override
//    @Blocking
//    public Uni<IndexUpdateReply> indexUpdate(IndexUpdatePush request) {
//        if (request.getSelfUuid().isBlank()) throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
//        if (!persistentPeerDataService.existsHost(UUID.fromString(request.getSelfUuid())))
//            throw new StatusRuntimeException(Status.UNAUTHENTICATED);
//
//        Log.info("<-- indexUpdate: " + request.getHeader().getName());
//        return jObjectTxManager.executeTxAndFlush(() -> {
//            return Uni.createFrom().item(syncHandler.handleRemoteUpdate(request));
//        });
//    }
    @Override
    @Blocking
    public Uni<OpPushReply> opPush(OpPushRequest request) {
        try {
            var ops = request.getMsgList().stream().map(opProtoSerializer::deserialize).toList();
            for (var op : ops) {
                Log.info("<-- op: " + op + " from " + identity.getPrincipal().getName().substring(3));
                txm.run(() -> {
                    opHandler.handleOp(PeerId.of(identity.getPrincipal().getName().substring(3)), op);
                });
            }
        } catch (Exception e) {
            Log.error(e, e);
            throw e;
        }
        return Uni.createFrom().item(OpPushReply.getDefaultInstance());
    }


    @Override
    @Blocking
    public Uni<PingReply> ping(PingRequest request) {
        if (request.getSelfUuid().isBlank()) throw new StatusRuntimeException(Status.INVALID_ARGUMENT);

        return Uni.createFrom().item(PingReply.newBuilder().setSelfUuid(persistentPeerDataService.getSelfUuid().toString()).build());
    }
}
