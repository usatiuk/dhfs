package com.usatiuk.dhfs.objects.repository.opsupport;

import com.usatiuk.dhfs.objects.repository.PeerManager;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class OpObjectRegistry {
    private final ConcurrentHashMap<String, OpObject> _objects = new ConcurrentHashMap<>();
    @Inject
    OpSender opSender;
    @Inject
    PeerManager remoteHostManager;

    public void registerObject(OpObject obj) {
        _objects.put(obj.getId(), obj);
        remoteHostManager.registerConnectEventListener(host -> {
            opSender.push(obj);
        });
    }

    public void acceptExternalOps(String objId, UUID from, List<Op> ops) {
        var got = _objects.get(objId);
        if (got == null)
            throw new StatusRuntimeException(Status.NOT_FOUND.withDescription("Queue with id " + objId + " not registered"));
        got.addToTx();
        for (Op op : ops)
            got.acceptExternalOp(from, op);
    }

    public void pushBootstrapData(UUID host) {
        for (var o : _objects.values()) {
            // FIXME: Split transactions for objects?
            o.addToTx();
            o.pushBootstrap(host);
        }
    }


    @Scheduled(every = "${dhfs.objects.periodic-push-op-interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    @Blocking
    void periodicPush() {
        for (var obj : _objects.values()) {
            opSender.push(obj);
        }
    }
}
