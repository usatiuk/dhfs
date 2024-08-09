package com.usatiuk.dhfs.objects.repository.opsupport;

import com.usatiuk.dhfs.objects.repository.PeerManager;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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

    private final ConcurrentHashMap<String, OpObject<?>> _objects = new ConcurrentHashMap<>();

    public void registerObject(OpObject<?> obj) {
        _objects.put(obj.getId(), obj);
        remoteHostManager.registerConnectEventListener(host -> {
            opSender.push(obj);
        });
    }

    public void acceptExternalOp(String objId, UUID from, Op op) {
        var got = _objects.get(objId);
        if (got == null)
            throw new StatusRuntimeException(Status.NOT_FOUND.withDescription("Queue with id " + objId + " not registered"));
        dispatchOp(got, from, op);
    }

    private <OpLocalT extends Op> void dispatchOp(OpObject<OpLocalT> obj, UUID from, Op op) {
        obj.acceptExternalOp(from, (OpLocalT) op);
    }
}
