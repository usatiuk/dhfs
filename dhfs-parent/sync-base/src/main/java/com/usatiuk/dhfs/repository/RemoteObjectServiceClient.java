package com.usatiuk.dhfs.repository;

import com.usatiuk.dhfs.ProtoSerializer;
import com.usatiuk.dhfs.*;
import com.usatiuk.dhfs.persistence.JObjectKeyP;
import com.usatiuk.dhfs.repository.invalidation.InvalidationQueueService;
import com.usatiuk.dhfs.repository.invalidation.Op;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.transaction.Transaction;
import com.usatiuk.objects.transaction.TransactionManager;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@ApplicationScoped
public class RemoteObjectServiceClient {
    private final ExecutorService _batchExecutor = Executors.newVirtualThreadPerTaskExecutor();
    @Inject
    PersistentPeerDataService persistentPeerDataService;
    @Inject
    RpcClientFactory rpcClientFactory;
    @Inject
    TransactionManager txm;
    @Inject
    Transaction curTx;
    @Inject
    RemoteTransaction remoteTx;
    @Inject
    SyncHandler syncHandler;
    @Inject
    InvalidationQueueService invalidationQueueService;
    @Inject
    ProtoSerializer<OpP, Op> opProtoSerializer;
    @Inject
    ProtoSerializer<GetObjectReply, ReceivedObject> receivedObjectProtoSerializer;
    @Inject
    PeerManager peerManager;

    public Pair<PeerId, ReceivedObject> getSpecificObject(JObjectKey key, PeerId peerId) {
        return rpcClientFactory.withObjSyncClient(peerId, (peer, client) -> {
            var reply = client.getObject(GetObjectRequest.newBuilder().setName(JObjectKeyP.newBuilder().setName(key.toString()).build()).build());
            var deserialized = receivedObjectProtoSerializer.deserialize(reply);
            return Pair.of(peer, deserialized);
        });
    }

    public void getObject(JObjectKey key, Function<Pair<PeerId, ReceivedObject>, Boolean> onReceive) {
        var objMeta = remoteTx.getMeta(key).orElse(null);

        if (objMeta == null) {
            throw new IllegalArgumentException("Object " + key + " not found");
        }

        var targetVersion = objMeta.versionSum();
        var targets = objMeta.knownRemoteVersions().isEmpty()
                ? peerManager.getAvailableHosts()
                : objMeta.knownRemoteVersions().entrySet().stream()
                .filter(entry -> entry.getValue().equals(targetVersion))
                .map(Map.Entry::getKey).toList();

        if (targets.isEmpty())
            throw new IllegalStateException("No targets for object " + key);

        Log.info("Downloading object " + key + " from " + targets);

        rpcClientFactory.withObjSyncClient(targets, (peer, client) -> {
            var reply = client.getObject(GetObjectRequest.newBuilder().setName(JObjectKeyP.newBuilder().setName(key.toString()).build()).build());

            var deserialized = receivedObjectProtoSerializer.deserialize(reply);

            if (!onReceive.apply(Pair.of(peer, deserialized))) {
                throw new StatusRuntimeException(Status.ABORTED.withDescription("Failed to process object " + key + " from " + peer));
            }

            return null;
        });
    }

    public OpPushReply pushOps(PeerId target, List<Op> ops) {
        var barrier = new CountDownLatch(ops.size());
        for (Op op : ops) {
            txm.run(() -> {
                for (var ref : op.getEscapedRefs()) {
                    curTx.get(RemoteObjectMeta.class, ref).map(m -> m.withSeen(true)).ifPresent(curTx::put);
                }
            }).onFlush(barrier::countDown);
        }
        var builder = OpPushRequest.newBuilder();
        for (Op op : ops) {
            builder.addMsg(opProtoSerializer.serialize(op));
        }
        var built = builder.build();
        try {
            barrier.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        rpcClientFactory.withObjSyncClient(target, (tgt, client) -> client.opPush(built));
        return OpPushReply.getDefaultInstance();
    }

    public Collection<Pair<PeerId, CanDeleteReply>> canDelete(Collection<PeerId> targets, JObjectKey objKey, Collection<JDataRef> ourReferrers) {
        Log.trace("Asking canDelete for " + objKey + " from " + targets.stream().map(PeerId::toString).collect(Collectors.joining(", ")));
        try {
            return _batchExecutor.invokeAll(targets.stream().<Callable<Pair<PeerId, CanDeleteReply>>>map(h -> () -> {
                var req = CanDeleteRequest.newBuilder().setName(JObjectKeyP.newBuilder().setName(objKey.toString()).build());
                for (var ref : ourReferrers) {
                    req.addOurReferrers(JObjectKeyP.newBuilder().setName(ref.obj().toString()).build());
                }
                return Pair.of(h, rpcClientFactory.withObjSyncClient(h, (p, client) -> client.canDelete(req.build())));
            }).toList()).stream().map(f -> {
                try {
                    return f.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }).toList();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
