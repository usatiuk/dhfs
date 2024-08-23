package com.usatiuk.dhfs.objects.jrepository;

import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializerService;
import com.usatiuk.dhfs.objects.repository.invalidation.InvalidationQueueService;
import com.usatiuk.utils.VoidFn;
import io.quarkus.logging.Log;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Collection;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

@ApplicationScoped
public class JObjectTxManager {
    private final ThreadLocal<TxState> _state = new ThreadLocal<>();
    @Inject
    ProtoSerializerService protoSerializerService;
    @Inject
    JObjectLRU jObjectLRU;
    @Inject
    JObjectManager jObjectManager;
    @Inject
    JObjectWriteback jObjectWriteback;
    @Inject
    InvalidationQueueService invalidationQueueService;
    @Inject
    TxWriteback txWriteback;
    @ConfigProperty(name = "dhfs.objects.ref_verification")
    boolean refVerification;
    private ExecutorService _serializerThreads;
    private AtomicLong _transientTxId = new AtomicLong();

    public JObjectTxManager() {
        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("tx-serializer-%d")
                .build();

        // FIXME:
        _serializerThreads = Executors.newFixedThreadPool(8, factory);
    }

    public void begin() {
        if (_state.get() != null)
            throw new IllegalStateException("Transaction already running");

        _state.set(new TxState());
    }

    public void drop(JObject<?> obj) {
        var state = _state.get();
        if (state == null)
            throw new IllegalStateException("Transaction not running");
        Log.debug("Dropping " + obj.getMeta().getName() + " from " + state._id);
        obj.assertRwLock();
        state._writeObjects.remove(obj);
        obj.rwUnlock();
    }

    public void commit() {
        var state = _state.get();
        if (state == null)
            throw new IllegalStateException("Transaction not running");

        if (state._writeObjects.isEmpty()) {
//            Log.trace("Empty transaction " + state._id);
            _state.remove();
            return;
        }

        Log.debug("Committing transaction " + state._id);

        for (var obj : state._writeObjects.entrySet()) {
            Log.debug("Committing " + obj.getKey().getMeta().getName() + " deleted=" + obj.getKey().getMeta().isDeleted() + " deletion-candidate=" + obj.getKey().getMeta().isDeletionCandidate());

            var newExternalHash = obj.getKey().getMeta().externalHash();

            // FIXME
            notifyWrite(obj.getKey(),
                    obj.getValue() == null || !Objects.equals(obj.getValue().meta(), protoSerializerService.serialize(obj.getKey().getMeta())),
                    obj.getValue() == null || newExternalHash != obj.getValue().externalHash(),
                    obj.getValue() == null || (obj.getValue().data() == null) != (obj.getKey().getData() == null) ||
                            (obj.getValue().data() == null) ||
                            !Objects.equals(obj.getValue().data(), protoSerializerService.serialize(obj.getKey().getData()))
            );

            if (refVerification) {
                var oldRefs = (obj.getValue() == null || obj.getValue().data() == null)
                        ? null
                        : ((JObjectData) protoSerializerService.deserialize(obj.getValue().data())).extractRefs();
                verifyRefs(obj.getKey(), oldRefs);
            }
        }

        var bundle = txWriteback.createBundle();
        var latch = new CountDownLatch(state._writeObjects.size());

        state._writeObjects.forEach((key, value) -> {
            try {
                if (key.getMeta().isDeleted()) {
                    bundle.delete(key);
                    latch.countDown();
                } else if (key.getMeta().isHaveLocalCopy() && key.getData() != null) {
                    _serializerThreads.execute(() -> {
                        bundle.commit(key,
                                protoSerializerService.serialize(key.getMeta()),
                                protoSerializerService.serializeToJObjectDataP(key.getData())
                        );
                        latch.countDown();
                    });
                } else if (key.getMeta().isHaveLocalCopy() && key.getData() == null) {
                    bundle.commitMetaChange(key,
                            protoSerializerService.serialize(key.getMeta())
                    );
                    latch.countDown();
                } else if (!key.getMeta().isHaveLocalCopy()) {
                    bundle.commit(key,
                            protoSerializerService.serialize(key.getMeta()),
                            null
                    );
                    latch.countDown();
                }
            } catch (Exception e) {
                Log.error("Error committing object " + key.getMeta().getName(), e);
            }
        });

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        state._writeObjects.forEach((key, value) -> key.rwUnlock());

        txWriteback.commitBundle(bundle);

        _state.remove();
    }

    private <T extends JObjectData> void notifyWrite(JObject<?> obj, boolean metaChanged,
                                                     boolean externalChanged, boolean hasDataChanged) {
        jObjectLRU.updateSize(obj);
        jObjectManager.runWriteListeners(obj, metaChanged, externalChanged);
        if (externalChanged && obj.getMeta().isHaveLocalCopy()) {
            invalidationQueueService.pushInvalidationToAll(obj);
        }
    }

    private void verifyRefs(JObject<?> obj, @Nullable Collection<String> oldRefs) {
        if (!refVerification) return;

        if (obj.getData() == null) return;
        if (obj.getMeta().isDeleted()) return;
        var newRefs = obj.getData().extractRefs();
        if (oldRefs != null)
            for (var o : oldRefs)
                if (!newRefs.contains(o)) {
                    jObjectManager.get(o).ifPresent(refObj -> {
                        if (refObj.getMeta().checkRef(obj.getMeta().getName()))
                            throw new IllegalStateException("Object " + o + " is referenced from " + obj.getMeta().getName() + " but shouldn't be");
                    });
                }
        for (var r : newRefs) {
            var refObj = jObjectManager.get(r).orElseThrow(() -> new IllegalStateException("Object " + r + " not found but should be referenced from " + obj.getMeta().getName()));
            if (refObj.getMeta().isDeleted())
                throw new IllegalStateException("Object " + r + " deleted but referenced from " + obj.getMeta().getName());
            if (!refObj.getMeta().checkRef(obj.getMeta().getName()))
                throw new IllegalStateException("Object " + r + " is not referenced by " + obj.getMeta().getName() + " but should be");
        }
    }

    public void rollback() {
        var state = _state.get();
        if (state == null)
            throw new IllegalStateException("Transaction not running");
        Log.debug("Rollback of " + state._id);

        for (var obj : state._writeObjects.entrySet()) {
            Log.debug("Rollback of " + obj.getKey().getMeta().getName());
            try {
                if (obj.getValue() == null) {
                    Log.warn("Skipped rollback of " + obj.getKey().getMeta().getName());
                    continue;
                }
                obj.getKey().rollback(
                        protoSerializerService.deserialize(obj.getValue().meta()),
                        obj.getValue().data() != null ? protoSerializerService.deserialize(obj.getValue().data()) : null);
                obj.getKey().updateDeletionState();
            } finally {
                obj.getKey().rwUnlock();
            }
        }

        _state.remove();
    }

    public void executeTx(VoidFn fn) {
        // FIXME: Exception handling/nested tx stuff?
        if (_state.get() != null) {
            fn.apply();
            return;
        }

        begin();
        try {
            fn.apply();
            commit();
        } catch (Exception e) {
            Log.debug("Error in transaction " + _state.get()._id, e);
            rollback();
            throw e;
        }
    }

    public <T> T executeTx(Supplier<T> fn) {
        if (_state.get() != null) {
            return fn.get();
        }

        begin();
        try {
            var ret = fn.get();
            commit();
            return ret;
        } catch (Exception e) {
            Log.debug("Error in transaction " + _state.get()._id, e);
            rollback();
            throw e;
        }
    }

    void addToTx(JObject<?> obj, boolean copy) {
        if (obj.getMeta().getKnownClass().isAnnotationPresent(NoTransaction.class))
            throw new IllegalArgumentException("NoTransaction objects shouldn't be in transaction");

        var state = _state.get();

        if (state == null)
            throw new IllegalStateException("Transaction not running");

        Log.debug("Adding " + obj.getMeta().getName() + " to transaction " + state._id);

        obj.assertRwLock();
        obj.rwLock();

        state._writeObjects.put(obj,
                copy ? new JObjectSnapshot(
                        protoSerializerService.serialize(obj.getMeta()),
                        obj.getData() != null ? protoSerializerService.serializeToJObjectDataP(obj.getData()) : null,
                        obj.getMeta().externalHash())
                        : null);
    }

    private class TxState {
        private final long _id = _transientTxId.incrementAndGet();
        private final HashMap<JObject<?>, JObjectSnapshot> _writeObjects = new HashMap<>();
    }
}
