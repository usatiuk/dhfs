package com.usatiuk.dhfs.rpc;

import com.usatiuk.dhfs.ProtoSerializer;
import com.usatiuk.dhfs.autosync.AutosyncProcessor;
import com.usatiuk.dhfs.invalidation.Op;
import com.usatiuk.dhfs.invalidation.OpHandlerService;
import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.dhfs.peersync.ReachablePeerManager;
import com.usatiuk.dhfs.persistence.JObjectKeyP;
import com.usatiuk.dhfs.remoteobj.*;
import com.usatiuk.dhfs.repository.*;
import com.usatiuk.dhfs.syncmap.DtoMapperService;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.transaction.Transaction;
import com.usatiuk.objects.transaction.TransactionHandle;
import com.usatiuk.objects.transaction.TransactionManager;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

// Note: RunOnVirtualThread hangs somehow
@ApplicationScoped
public class RemoteObjectServiceServerImpl {
    @Inject
    TransactionManager txm;
    @Inject
    ReachablePeerManager reachablePeerManager;
    @Inject
    Transaction curTx;

    @Inject
    ProtoSerializer<OpP, Op> opProtoSerializer;
    @Inject
    ProtoSerializer<GetObjectReply, ReceivedObject> receivedObjectProtoSerializer;
    @Inject
    RemoteTransaction remoteTx;
    @Inject
    OpHandlerService opHandlerService;
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
        Log.infov("<-- canDelete: {0} from {1}", request, from);

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

            if (!builder.getDeletionCandidate()) {
                Log.infov("Not deletion candidate: {0}, {1} (asked from {2})", obj, builder, from);
            }
        });
        return Uni.createFrom().item(builder.build());
    }

    public Uni<OpPushReply> opPush(PeerId from, OpPushRequest request) {
        if (request.getMsgCount() == 0) {
            Log.infov("<-- opPush: empty from {0}", from);
            return Uni.createFrom().item(OpPushReply.getDefaultInstance());
        }

        var handles = new ArrayList<TransactionHandle>();
        try {
            var ops = request.getMsgList().stream().map(opProtoSerializer::deserialize).toList();
            for (var op : ops) {
                Log.infov("<-- opPush: {0} from {1}", op, from);
                var handle = txm.run(() -> {
                    opHandlerService.handleOp(from, op);
                });
                handles.add(handle);
            }
        } catch (Exception e) {
            Log.error("Error handling ops", e);
            throw e;
        }
        return Uni.createFrom().emitter(e -> {
            var counter = new AtomicLong(handles.size());
            for (var handle : handles) {
                handle.onFlush(() -> {
                    if (counter.decrementAndGet() == 0) {
                        e.complete(OpPushReply.getDefaultInstance());
                    }
                });
            }
        });
    }

    public Uni<PingReply> ping(PeerId from, PingRequest request) {
        return Uni.createFrom().item(PingReply.getDefaultInstance());
    }
}
