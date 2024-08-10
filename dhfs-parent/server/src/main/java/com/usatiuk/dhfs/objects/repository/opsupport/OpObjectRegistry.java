package com.usatiuk.dhfs.objects.repository.opsupport;

import com.usatiuk.dhfs.objects.repository.PeerManager;
import com.usatiuk.dhfs.objects.repository.RemoteObjectServiceClient;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class OpObjectRegistry {
    @Inject
    OpSender opSender;
    @Inject
    PeerManager remoteHostManager;
    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;

    private final ConcurrentHashMap<String, OpObject> _objects = new ConcurrentHashMap<>();

    public void registerObject(OpObject obj) {
        _objects.put(obj.getId(), obj);
        remoteHostManager.registerConnectEventListener(host -> {
            opSender.push(obj);
        });
    }

    public void acceptExternalOp(String objId, UUID from, Op op) {
        var got = _objects.get(objId);
        if (got == null)
            throw new StatusRuntimeException(Status.NOT_FOUND.withDescription("Queue with id " + objId + " not registered"));
        got.acceptExternalOp(from, op);
    }

    public void pushBootstrapData(UUID host) {
        for (var o : _objects.values()) {
            o.pushBootstrap(host);
        }
    }


    @Scheduled(every = "10s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    @Blocking
    void periodicPush() {
        for (var obj : _objects.values()) {
            var periodicPushOp = obj.getPeriodicPushOp();
            if (periodicPushOp == null) continue;
            for (var h : remoteHostManager.getAvailableHosts()) {
                try {
                    remoteObjectServiceClient.pushOp(periodicPushOp, obj.getId(), h);
                } catch (Exception e) {
                    Log.warn("Error pushing periodic op for " + h + " of " + obj.getId(), e);
                }
            }
        }
    }
}
