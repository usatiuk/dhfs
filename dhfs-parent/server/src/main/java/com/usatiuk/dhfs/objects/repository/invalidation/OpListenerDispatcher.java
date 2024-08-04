package com.usatiuk.dhfs.objects.repository.invalidation;

import jakarta.enterprise.context.ApplicationScoped;

import java.util.HashMap;
import java.util.UUID;

@ApplicationScoped
public class OpListenerDispatcher {
    private final HashMap<String, IncomingOpListener> _listeners = new HashMap<>();

    public void registerListener(String queueId, IncomingOpListener listener) {
        _listeners.put(queueId, listener);
    }

    public void accept(String queueId, UUID hostFrom, Op op) {
        var got = _listeners.get(queueId);
        if (got == null) throw new IllegalArgumentException("Queue with id " + queueId + " not registered");
        got.accept(hostFrom, op);
    }
}
