package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.dhfs.objects.jrepository.DeletedObjectAccessException;
import com.usatiuk.dhfs.objects.jrepository.JObject;
import com.usatiuk.dhfs.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.objects.jrepository.PushResolution;
import com.usatiuk.dhfs.objects.persistence.JObjectDataP;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializerService;
import com.usatiuk.dhfs.objects.repository.autosync.AutoSyncProcessor;
import com.usatiuk.dhfs.objects.repository.movedummies.MoveDummyEntry;
import com.usatiuk.dhfs.objects.repository.movedummies.MoveDummyRegistry;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcService;
import io.quarkus.logging.Log;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;

import java.util.UUID;

// Note: RunOnVirtualThread hangs somehow
@GrpcService
@RolesAllowed("cluster-member")
public class RemoteObjectServiceServer implements DhfsObjectSyncGrpc {
    @Inject
    SyncHandler syncHandler;

    @Inject
    JObjectManager jObjectManager;

    @Inject
    RemoteHostManager remoteHostManager;

    @Inject
    AutoSyncProcessor autoSyncProcessor;

    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    @Inject
    InvalidationQueueService invalidationQueueService;

    @Inject
    ProtoSerializerService protoSerializerService;

    @Inject
    MoveDummyRegistry moveDummyRegistry;

    @Override
    @Blocking
    public Uni<GetObjectReply> getObject(GetObjectRequest request) {
        if (request.getSelfUuid().isBlank()) throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
        if (!persistentRemoteHostsService.existsHost(UUID.fromString(request.getSelfUuid())))
            throw new StatusRuntimeException(Status.UNAUTHENTICATED);

        Log.info("<-- getObject: " + request.getName() + " from " + request.getSelfUuid());

        var pushedMoves = request.getForConflict() ? null : moveDummyRegistry.withPendingForHost(UUID.fromString(request.getSelfUuid()), pm -> {
            if (pm.isEmpty()) return null;

            var builder = PushedMoves.newBuilder();

            // FIXME:
            int count = 0;
            var it = pm.iterator();
            while (it.hasNext()) {
                count++;
                if (count > 100) break;

                var next = it.next();

                var obj = jObjectManager.get(next.parent());

                if (obj.isEmpty()) {
                    it.remove();
                    continue;
                }

                ObjectHeader header;

                try {
                    header = obj.get().runReadLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d) -> {
                        if (m.getKnownClass().isAnnotationPresent(PushResolution.class))
                            throw new NotImplementedException();

                        return m.toRpcHeader();
                    });
                } catch (DeletedObjectAccessException e) {
                    it.remove();
                    continue;
                }

                builder.addPushedMoves(PushedMove.newBuilder()
                        .setParent(header)
                        .setKid(next.child())
                        .build());
            }

            return builder.build();
        });

        var replyBuilder = GetObjectReply.newBuilder()
                .setSelfUuid(persistentRemoteHostsService.getSelfUuid().toString());

        if (pushedMoves != null) {
            replyBuilder.setPushedMoves(pushedMoves);

            if (pushedMoves.getPushedMovesCount() >= 100)
                return Uni.createFrom().item(replyBuilder.build());
        }

        var obj = jObjectManager.get(request.getName()).orElseThrow(() -> new StatusRuntimeException(Status.NOT_FOUND));

        Pair<ObjectHeader, JObjectDataP> read = obj.runReadLocked(JObject.ResolutionStrategy.LOCAL_ONLY, (meta, data) -> {
            if (data == null) {
                Log.info("<-- getObject FAIL: " + request.getName() + " from " + request.getSelfUuid());
                throw new StatusRuntimeException(Status.ABORTED.withDescription("Not available locally"));
            }
            data.extractRefs().forEach(ref -> jObjectManager.get(ref).ifPresent(JObject::markSeen));
            return Pair.of(meta.toRpcHeader(), protoSerializerService.serializeToJObjectDataP(data));
        });
        obj.markSeen();
        var replyObj = ApiObject.newBuilder().setHeader(read.getLeft()).setContent(read.getRight()).build();
        return Uni.createFrom().item(replyBuilder.setObject(replyObj).build());
    }

    @Override
    @Blocking
    public Uni<CanDeleteReply> canDelete(CanDeleteRequest request) {
        if (request.getSelfUuid().isBlank()) throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
        if (!persistentRemoteHostsService.existsHost(UUID.fromString(request.getSelfUuid())))
            throw new StatusRuntimeException(Status.UNAUTHENTICATED);

        Log.info("<-- canDelete: " + request.getName() + " from " + request.getSelfUuid());

        var builder = CanDeleteReply.newBuilder();

        var obj = jObjectManager.get(request.getName());

        builder.setSelfUuid(persistentRemoteHostsService.getSelfUuid().toString());
        builder.setObjName(request.getName());

        if (obj.isPresent()) try {
            obj.get().runReadLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d) -> {
                if (m.isDeleted() && !m.isDeletionCandidate())
                    throw new IllegalStateException("Object " + m.getName() + " is deleted but not a deletion candidate");
                builder.setDeletionCandidate(m.isDeletionCandidate());
                builder.addAllReferrers(m.getReferrers());
                return null;
            });
        } catch (DeletedObjectAccessException dox) {
            builder.setDeletionCandidate(true);
        }
        else {
            builder.setDeletionCandidate(true);
        }

        var ret = builder.build();

        if (!ret.getDeletionCandidate())
            for (var rr : request.getOurReferrersList())
                autoSyncProcessor.add(rr);

        return Uni.createFrom().item(ret);
    }

    @Override
    public Uni<ConfirmPushedMoveReply> confirmPushedMove(ConfirmPushedMoveRequest request) {
        Log.info("<-- confirmPushedMove: from " + request.getSelfUuid());
        for (var m : request.getConfirmedMovesList())
            moveDummyRegistry.commitForHost(UUID.fromString(request.getSelfUuid()), new MoveDummyEntry(m.getParent(), m.getKid()));

        return Uni.createFrom().item(ConfirmPushedMoveReply.getDefaultInstance());
    }

    @Override
    @Blocking
    public Uni<GetIndexReply> getIndex(GetIndexRequest request) {
        if (request.getSelfUuid().isBlank()) throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
        if (!persistentRemoteHostsService.existsHost(UUID.fromString(request.getSelfUuid())))
            throw new StatusRuntimeException(Status.UNAUTHENTICATED);

        Log.info("<-- getIndex: from " + request.getSelfUuid());

        var objs = jObjectManager.findAll();

        var reqUuid = UUID.fromString(request.getSelfUuid());

        for (var obj : objs) {
            Log.trace("GI: " + obj + " to " + reqUuid);
            invalidationQueueService.pushInvalidationToOne(reqUuid, obj);
        }

        return Uni.createFrom().item(GetIndexReply.getDefaultInstance());
    }

    @Override
    @Blocking
    public Uni<IndexUpdateReply> indexUpdate(IndexUpdatePush request) {
        if (request.getSelfUuid().isBlank()) throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
        if (!persistentRemoteHostsService.existsHost(UUID.fromString(request.getSelfUuid())))
            throw new StatusRuntimeException(Status.UNAUTHENTICATED);

//        Log.info("<-- indexUpdate: " + request.getHeader().getName());
        return Uni.createFrom().item(syncHandler.handleRemoteUpdate(request));
    }

    @Override
    @Blocking
    public Uni<PingReply> ping(PingRequest request) {
        if (request.getSelfUuid().isBlank()) throw new StatusRuntimeException(Status.INVALID_ARGUMENT);

        return Uni.createFrom().item(PingReply.newBuilder().setSelfUuid(persistentRemoteHostsService.getSelfUuid().toString()).build());
    }
}
