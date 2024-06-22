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
import org.eclipse.microprofile.config.inject.ConfigProperty;

// Note: RunOnVirtualThread hangs somehow
@GrpcService
public class RemoteObjectServiceServer implements DhfsObjectSyncGrpc {
    @ConfigProperty(name = "dhfs.objects.distributed.selfname")
    String selfname;

    @Inject
    SyncHandler syncHandler;

    @Inject
    JObjectManager jObjectManager;

    @Inject
    RemoteHostManager remoteHostManager;

    @Override
    @Blocking
    public Uni<GetObjectReply> getObject(GetObjectRequest request) {
        if (request.getSelfname().isBlank()) throw new StatusRuntimeException(Status.INVALID_ARGUMENT);

        remoteHostManager.handleConnectionSuccess(request.getSelfname());

        Log.info("<-- getObject: " + request.getName());

        var obj = jObjectManager.get(request.getName()).orElseThrow(() -> new StatusRuntimeException(Status.NOT_FOUND));

        Pair<ObjectHeader, ByteString> read = obj.runReadLocked((meta, data) -> Pair.of(meta.toRpcHeader(), SerializationHelper.serialize(data)));
        var replyObj = ApiObject.newBuilder().setHeader(read.getLeft()).setContent(read.getRight()).build();
        return Uni.createFrom().item(GetObjectReply.newBuilder().setObject(replyObj).build());
    }

    @Override
    @Blocking
    public Uni<IndexUpdatePush> getIndex(GetIndexRequest request) {
        if (request.getSelfname().isBlank()) throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
        remoteHostManager.handleConnectionSuccess(request.getSelfname());

        Log.info("<-- getIndex: ");
        var builder = IndexUpdatePush.newBuilder().setSelfname(selfname);

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
        if (request.getSelfname().isBlank()) throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
        remoteHostManager.handleConnectionSuccess(request.getSelfname());

//        Log.info("<-- indexUpdate: " + request.getHeader().getName());
        return Uni.createFrom().item(syncHandler.handleRemoteUpdate(request));
    }

    @Override
    @Blocking
    public Uni<PingReply> ping(PingRequest request) {
        if (request.getSelfname().isBlank()) throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
        remoteHostManager.handleConnectionSuccess(request.getSelfname());

        return Uni.createFrom().item(PingReply.newBuilder().setSelfname(selfname).build());
    }
}
