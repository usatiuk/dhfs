package com.usatiuk.dhfs.objects.repository.opsupport;

import com.usatiuk.dhfs.objects.repository.PeerManager;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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
        boolean push = false;
        for (Op op : ops)
            push |= got.acceptExternalOp(from, op);
        if (push)
            opSender.push(got);
    }

    public void pushBootstrapData(UUID host) {
        for (var o : _objects.values()) {
            // FIXME: Split transactions for objects?
            o.addToTx();
            o.pushBootstrap(host);
        }
    }
}
