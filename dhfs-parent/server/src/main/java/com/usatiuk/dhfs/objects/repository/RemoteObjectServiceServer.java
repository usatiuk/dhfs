package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.dhfs.objects.jrepository.DeletedObjectAccessException;
import com.usatiuk.dhfs.objects.jrepository.JObject;
import com.usatiuk.dhfs.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.objects.persistence.JObjectDataP;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializerService;
import com.usatiuk.dhfs.objects.repository.autosync.AutoSyncProcessor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcService;
import io.quarkus.logging.Log;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
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

    @Override
    @Blocking
    public Uni<GetObjectReply> getObject(GetObjectRequest request) {
        if (request.getSelfUuid().isBlank()) throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
        if (!persistentRemoteHostsService.existsHost(UUID.fromString(request.getSelfUuid())))
            throw new StatusRuntimeException(Status.UNAUTHENTICATED);

        Log.info("<-- getObject: " + request.getName() + " from " + request.getSelfUuid());

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
        return Uni.createFrom().item(GetObjectReply.newBuilder()
                .setSelfUuid(persistentRemoteHostsService.getSelfUuid().toString())
                .setObject(replyObj).build());
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
            for (var rr : ret.getReferrersList())
                autoSyncProcessor.add(rr);

        return Uni.createFrom().item(ret);
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
            Log.trace("GI: " + obj.getName() + " to " + reqUuid);
            invalidationQueueService.pushInvalidationToOne(reqUuid, obj.getName());
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
