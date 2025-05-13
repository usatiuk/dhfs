package com.usatiuk.dhfs.rpc;

import com.usatiuk.dhfs.ProtoSerializer;
import com.usatiuk.dhfs.invalidation.InvalidationQueueService;
import com.usatiuk.dhfs.invalidation.Op;
import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.dhfs.peersync.PersistentPeerDataService;
import com.usatiuk.dhfs.peersync.ReachablePeerManager;
import com.usatiuk.dhfs.persistence.JObjectKeyP;
import com.usatiuk.dhfs.remoteobj.ReceivedObject;
import com.usatiuk.dhfs.remoteobj.RemoteObjectMeta;
import com.usatiuk.dhfs.remoteobj.RemoteTransaction;
import com.usatiuk.dhfs.remoteobj.SyncHandler;
import com.usatiuk.dhfs.repository.*;
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

/**
 * Helper class for calling remote peers RPCs.
 */
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
    ReachablePeerManager reachablePeerManager;

    /**
     * Download a specific object from a specific peer.
     *
     * @param key    the key of the object to download
     * @param peerId the ID of the peer to download from
     * @return a pair of the peer ID from which the object was downloaded and the downloaded object
     */
    public Pair<PeerId, ReceivedObject> getSpecificObject(JObjectKey key, PeerId peerId) {
        return rpcClientFactory.withObjSyncClient(peerId, (peer, client) -> {
            var reply = client.getObject(GetObjectRequest.newBuilder().setName(JObjectKeyP.newBuilder().setName(key.toString()).build()).build());
            var deserialized = receivedObjectProtoSerializer.deserialize(reply);
            return Pair.of(peer, deserialized);
        });
    }

    /**
     * Download a specific object from some reachable peer.
     *
     * @param key       the key of the object to download
     * @param onReceive a callback function to process the received object
     */
    public void getObject(JObjectKey key, Function<Pair<PeerId, ReceivedObject>, Boolean> onReceive) {
        var objMeta = remoteTx.getMeta(key).orElse(null);

        if (objMeta == null) {
            throw new IllegalArgumentException("Object " + key + " not found");
        }

        var targetVersion = objMeta.versionSum();
        var targets = objMeta.knownRemoteVersions().isEmpty()
                ? reachablePeerManager.getAvailableHosts()
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

    /**
     * Push a list of operations to a specific peer.
     *
     * @param target the ID of the peer to push to
     * @param ops    the list of operations to push
     * @return the reply from the peer
     */
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

    /**
     * Ask given peers if they can delete the object with the given key.
     *
     * @param targets the list of peers to ask
     * @param objKey  the key of the object to delete
     * @return a collection of pairs of peer IDs and their replies
     */
    public Collection<Pair<PeerId, CanDeleteReply>> canDelete(Collection<PeerId> targets, JObjectKey objKey) {
        Log.trace("Asking canDelete for " + objKey + " from " + targets.stream().map(PeerId::toString).collect(Collectors.joining(", ")));
        try {
            return _batchExecutor.invokeAll(targets.stream().<Callable<Pair<PeerId, CanDeleteReply>>>map(h -> () -> {
                var req = CanDeleteRequest.newBuilder().setName(JObjectKeyP.newBuilder().setName(objKey.toString()).build());
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
