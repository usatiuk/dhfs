package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.repository.distributed.*;
import com.usatiuk.dhfs.storage.SerializationHelper;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectManager;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcService;
import io.quarkus.logging.Log;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import java.util.UUID;

// Note: RunOnVirtualThread hangs somehow
@GrpcService
public class RemoteObjectServiceServer implements DhfsObjectSyncGrpc {
    @Inject
    SyncHandler syncHandler;

    @Inject
    JObjectManager jObjectManager;

    @Inject
    RemoteHostManager remoteHostManager;

    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    @Override
    @Blocking
    public Uni<GetObjectReply> getObject(GetObjectRequest request) {
        if (request.getSelfUuid().isBlank()) throw new StatusRuntimeException(Status.INVALID_ARGUMENT);

        remoteHostManager.handleConnectionSuccess(UUID.fromString(request.getSelfUuid()));

        Log.info("<-- getObject: " + request.getName());

        var obj = jObjectManager.get(request.getName()).orElseThrow(() -> new StatusRuntimeException(Status.NOT_FOUND));

        if (!obj.tryLocalResolve())
            throw new StatusRuntimeException(Status.UNAVAILABLE.withDescription("Not available locally"));
        //FIXME:
        Pair<ObjectHeader, ByteString> read = obj.runReadLocked((meta, data) -> Pair.of(meta.toRpcHeader(), SerializationHelper.serialize(data)));
        var replyObj = ApiObject.newBuilder().setHeader(read.getLeft()).setContent(read.getRight()).build();
        return Uni.createFrom().item(GetObjectReply.newBuilder().setObject(replyObj).build());
    }

    @Override
    @Blocking
    public Uni<IndexUpdatePush> getIndex(GetIndexRequest request) {
        if (request.getSelfUuid().isBlank()) throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
        remoteHostManager.handleConnectionSuccess(UUID.fromString(request.getSelfUuid()));

        Log.info("<-- getIndex: ");
        var builder = IndexUpdatePush.newBuilder().setSelfUuid(persistentRemoteHostsService.getSelfUuid().toString());

        var objs = jObjectManager.find("");

        for (var obj : objs) {
            obj.runReadLocked((meta) -> {
                builder.addHeader(meta.toRpcHeader());
                return null;
            });
        }
        return Uni.createFrom().item(builder.build());
    }

    @Override
    @Blocking
    public Uni<IndexUpdateReply> indexUpdate(IndexUpdatePush request) {
        if (request.getSelfUuid().isBlank()) throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
        remoteHostManager.handleConnectionSuccess(UUID.fromString(request.getSelfUuid()));

//        Log.info("<-- indexUpdate: " + request.getHeader().getName());
        return Uni.createFrom().item(syncHandler.handleRemoteUpdate(request));
    }

    @Override
    @Blocking
    public Uni<PingReply> ping(PingRequest request) {
        if (request.getSelfUuid().isBlank()) throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
        remoteHostManager.handleConnectionSuccess(UUID.fromString(request.getSelfUuid()));

        return Uni.createFrom().item(PingReply.newBuilder().setSelfUuid(persistentRemoteHostsService.getSelfUuid().toString()).build());
    }
}
