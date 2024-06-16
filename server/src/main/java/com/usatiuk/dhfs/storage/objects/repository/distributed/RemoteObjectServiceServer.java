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

import java.util.Optional;

// Note: RunOnVirtualThread hangs somehow
@GrpcService
public class RemoteObjectServiceServer implements DhfsObjectSyncGrpc {
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
        var metaOpt = objectIndexService.getMeta(request.getName());
        if (metaOpt.isEmpty()) throw new StatusRuntimeException(Status.NOT_FOUND);
        var meta = metaOpt.get();
        Optional<Pair<Long, byte[]>> read = meta.runReadLocked((data) -> {
            if (objectPersistentStore.existsObject(request.getName()))
                return Optional.of(Pair.of(meta.getMtime(), objectPersistentStore.readObject(request.getName())));
            return Optional.empty();
        });
        if (read.isEmpty()) throw new StatusRuntimeException(Status.NOT_FOUND);
        var obj = read.get().getRight();
        var header = ObjectHeader.newBuilder().setName(request.getName()).setMtime(read.get().getLeft()).setAssumeUnique(meta.getAssumeUnique()).build();
        var replyObj = ApiObject.newBuilder().setHeader(header).setContent(ByteString.copyFrom(obj)).build();
        return Uni.createFrom().item(GetObjectReply.newBuilder().setObject(replyObj).build());
    }

    @Override
    @Blocking
    public Uni<GetIndexReply> getIndex(GetIndexRequest request) {
        Log.info("<-- getIndex: ");
        var builder = GetIndexReply.newBuilder();
        objectIndexService.forAllRead((name, meta) -> {
            var entry = ObjectHeader.newBuilder().setName(name).setMtime(meta.getMtime()).setAssumeUnique(meta.getAssumeUnique()).build();
            builder.addObjects(entry);
        });
        return Uni.createFrom().item(builder.build());
    }

    @Override
    @Blocking
    public Uni<IndexUpdateReply> indexUpdate(IndexUpdatePush request) {
        Log.info("<-- indexUpdate: " + request.getName() + " from: " + request.getPrevMtime() + " to: " + request.getMtime());
        return Uni.createFrom().item(syncHandler.handleRemoteUpdate(request));
    }
}
