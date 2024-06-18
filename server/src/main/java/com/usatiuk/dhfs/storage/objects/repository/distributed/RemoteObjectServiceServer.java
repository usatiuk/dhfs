package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.repository.distributed.*;
import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcService;
import io.quarkus.logging.Log;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Optional;

// Note: RunOnVirtualThread hangs somehow
@GrpcService
public class RemoteObjectServiceServer implements DhfsObjectSyncGrpc {
    @ConfigProperty(name = "dhfs.objects.distributed.selfname")
    String selfname;

    @Inject
    ObjectPersistentStore objectPersistentStore;

    @Inject
    ObjectIndexService objectIndexService;

    @Inject
    SyncHandler syncHandler;

    @Override
    @Blocking
    public Uni<GetObjectReply> getObject(GetObjectRequest request) {
        Log.info("<-- getObject: " + request.getName());

        var meta = objectIndexService.getMeta(request.getName()).orElseThrow(() -> new StatusRuntimeException(Status.NOT_FOUND));

        Optional<Pair<ObjectHeader, byte[]>> readOpt = meta.runReadLocked((data) -> {
            if (objectPersistentStore.existsObject(request.getName())) {
                ObjectHeader header = data.toRpcHeader();
                byte[] bytes = objectPersistentStore.readObject(request.getName());
                return Optional.of(Pair.of(header, bytes));
            } else {
                return Optional.empty();
            }
        });
        var read = readOpt.orElseThrow(() -> new StatusRuntimeException(Status.NOT_FOUND));
        var replyObj = ApiObject.newBuilder().setHeader(read.getLeft()).setContent(ByteString.copyFrom(read.getRight())).build();
        return Uni.createFrom().item(GetObjectReply.newBuilder().setObject(replyObj).build());
    }

    @Override
    @Blocking
    public Uni<GetIndexReply> getIndex(GetIndexRequest request) {
        Log.info("<-- getIndex: ");
        var builder = GetIndexReply.newBuilder().setSelfname(selfname);
        objectIndexService.forAllRead((name, meta) -> {
            builder.addObjects(meta.runReadLocked(ObjectMetaData::toRpcHeader));
        });
        return Uni.createFrom().item(builder.build());
    }

    @Override
    @Blocking
    public Uni<IndexUpdateReply> indexUpdate(IndexUpdatePush request) {
        Log.info("<-- indexUpdate: " + request.getHeader().getName());
        return Uni.createFrom().item(syncHandler.handleRemoteUpdate(request));
    }

    @Override
    public Uni<PingReply> ping(PingRequest request) {
        return Uni.createFrom().item(PingReply.newBuilder().setSelfname(selfname).build());
    }
}
