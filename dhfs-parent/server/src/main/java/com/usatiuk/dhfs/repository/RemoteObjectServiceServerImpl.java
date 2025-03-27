package com.usatiuk.dhfs.repository;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.*;
import com.usatiuk.dhfs.persistence.JObjectKeyP;
import com.usatiuk.dhfs.repository.invalidation.Op;
import com.usatiuk.dhfs.repository.invalidation.OpHandler;
import com.usatiuk.dhfs.repository.syncmap.DtoMapperService;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.transaction.Transaction;
import com.usatiuk.objects.transaction.TransactionManager;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

// Note: RunOnVirtualThread hangs somehow
@ApplicationScoped
public class RemoteObjectServiceServerImpl {
    @Inject
    TransactionManager txm;
    @Inject
    PeerManager peerManager;
    @Inject
    Transaction curTx;

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

    public Uni<GetObjectReply> getObject(PeerId from, GetObjectRequest request) {
        Log.info("<-- getObject: " + request.getName() + " from " + from);

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
            Log.info("<-- getObject NOT FOUND: " + request.getName() + " from " + from);
            throw new StatusRuntimeException(Status.NOT_FOUND);
        }

        var serialized = receivedObjectProtoSerializer.serialize(new ReceivedObject(got.getKey().changelog(), got.getRight()));
        return Uni.createFrom().item(serialized);
    }

    public Uni<CanDeleteReply> canDelete(PeerId from, CanDeleteRequest request) {
        var peerId = from;

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
                    builder.addReferrers(JObjectKeyP.newBuilder().setName(r.obj().toString()).build());
                    curTx.onCommit(() -> autosyncProcessor.add(r.obj()));
                }
            }
        });
        return Uni.createFrom().item(builder.build());
    }

    public Uni<OpPushReply> opPush(PeerId from, OpPushRequest request) {
        try {
            var ops = request.getMsgList().stream().map(opProtoSerializer::deserialize).toList();
            for (var op : ops) {
                Log.info("<-- op: " + op + " from " + from);
                txm.run(() -> {
                    opHandler.handleOp(from, op);
                });
            }
        } catch (Exception e) {
            Log.error(e, e);
            throw e;
        }
        return Uni.createFrom().item(OpPushReply.getDefaultInstance());
    }

    public Uni<PingReply> ping(PeerId from, PingRequest request) {
        return Uni.createFrom().item(PingReply.getDefaultInstance());
    }
}
