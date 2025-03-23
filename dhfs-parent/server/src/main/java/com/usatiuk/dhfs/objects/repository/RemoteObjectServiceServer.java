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
    @Inject
    ProtoSerializer<OpP, Op> opProtoSerializer;
    @Inject
    ProtoSerializer<GetObjectReply, ReceivedObject> receivedObjectProtoSerializer;
    @Inject
    RemoteTransaction remoteTx;
    @Inject
    OpHandler opHandler;
    @Inject
    DtoMapperService dtoMapperService;
    @Inject
    AutosyncProcessor autosyncProcessor;

    @Override
    @Blocking
    public Uni<GetObjectReply> getObject(GetObjectRequest request) {
        Log.info("<-- getObject: " + request.getName() + " from " + identity.getPrincipal().getName().substring(3));

        Pair<RemoteObjectMeta, JDataRemoteDto> got = txm.run(() -> {
            var meta = remoteTx.getMeta(JObjectKey.of(request.getName().getName())).orElse(null);
            var obj = remoteTx.getDataLocal(JDataRemote.class, JObjectKey.of(request.getName().getName())).orElse(null);
            if (meta != null && !meta.seen())
                curTx.put(meta.withSeen(true));
            if (obj != null)
                for (var ref : obj.collectRefsTo()) {
                    var refMeta = remoteTx.getMeta(ref).orElse(null);
                    if (refMeta != null && !refMeta.seen())
                        curTx.put(refMeta.withSeen(true));
                }
            return Pair.of(meta, obj == null ? null : dtoMapperService.toDto(obj, obj.dtoClass()));
        });

        if ((got.getValue() != null) && (got.getKey() == null)) {
            Log.error("Inconsistent state for object meta: " + request.getName());
            throw new StatusRuntimeException(Status.INTERNAL);
        }

        if (got.getValue() == null) {
            Log.info("<-- getObject NOT FOUND: " + request.getName() + " from " + identity.getPrincipal().getName().substring(3));
            throw new StatusRuntimeException(Status.NOT_FOUND);
        }

        var serialized = receivedObjectProtoSerializer.serialize(new ReceivedObject(got.getKey().changelog(), got.getRight()));
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
    @Blocking
    public Uni<CanDeleteReply> canDelete(CanDeleteRequest request) {
        var peerId = identity.getPrincipal().getName().substring(3);

        Log.info("<-- canDelete: " + request.getName() + " from " + peerId);

        var builder = CanDeleteReply.newBuilder();

        txm.run(() -> {
            var obj = curTx.get(RemoteObjectMeta.class, JObjectKey.of(request.getName().getName())).orElse(null);

            if (obj == null) {
                builder.setDeletionCandidate(true);
                return;
            }

            builder.setDeletionCandidate(!obj.frozen() && obj.refsFrom().isEmpty());

            if (!builder.getDeletionCandidate()) {
                for (var r : obj.refsFrom()) {
                    builder.addReferrers(JObjectKeyP.newBuilder().setName(r.toString()).build());
                    curTx.onCommit(() -> autosyncProcessor.add(r));
                }
            }
        });
        return Uni.createFrom().item(builder.build());
    }

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
        return Uni.createFrom().item(PingReply.getDefaultInstance());
    }
}
