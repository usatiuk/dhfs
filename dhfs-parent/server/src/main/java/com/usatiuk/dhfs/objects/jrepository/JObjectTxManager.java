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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
        Log.debug("Starting transaction");

        _state.set(new TxState());
    }

    public void commit() {
        var state = _state.get();
        if (state == null)
            throw new IllegalStateException("Transaction not running");
        Log.debug("Committing transaction");


        for (var obj : state._writeObjects.values()) {
            Log.debug("Committing " + obj.obj().getMeta().getName() + " deleted=" + obj.obj().getMeta().isDeleted() + " deletion-candidate=" + obj.obj().getMeta().isDeletionCandidate());

            var newExternalHash = obj.obj().getMeta().externalHash();

            // FIXME
            notifyWrite(obj.obj(),
                    !Objects.equals(obj.meta(), protoSerializerService.serialize(obj.obj().getMeta())),
                    newExternalHash != obj.externalHash(),
                    (obj.data() == null) != (obj.obj().getData() == null) ||
                            (obj.data() == null) ||
                            !Objects.equals(obj.data(), protoSerializerService.serialize(obj.obj().getData()))
            );

            if (refVerification) {
                var oldRefs = obj.data() == null
                        ? null
                        : ((JObjectData) protoSerializerService.deserialize(obj.data())).extractRefs();
                verifyRefs(obj.obj(), oldRefs);
            }
        }

        var bundle = txWriteback.createBundle();

        try {
            _serializerThreads.invokeAll(state._writeObjects.values().stream().<Callable<Void>>map(
                    obj -> () -> {
                        try {
                            if (obj.obj().getMeta().isDeleted())
                                bundle.delete(obj.obj());
                            else if (obj.obj().getMeta().isHaveLocalCopy() && obj.obj().getData() != null)
                                bundle.commit(obj.obj(),
                                        protoSerializerService.serialize(obj.obj().getMeta()),
                                        protoSerializerService.serializeToJObjectDataP(obj.obj().getData())
                                );
                            else if (obj.obj().getMeta().isHaveLocalCopy() && obj.obj().getData() == null)
                                bundle.commitMetaChange(obj.obj(),
                                        protoSerializerService.serialize(obj.obj().getMeta())
                                );
                            else if (!obj.obj().getMeta().isHaveLocalCopy())
                                bundle.commit(obj.obj(),
                                        protoSerializerService.serialize(obj.obj().getMeta()),
                                        null
                                );
                        } catch (Exception e) {
                            Log.error("Error committing object " + obj.obj().getMeta().getName(), e);
                        }
                        return null;
                    }
            ).toList());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        state._writeObjects.values().forEach(o -> o.obj().rwUnlock());

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
        Log.debug("Rollback");

        for (var obj : state._writeObjects.values()) {
            Log.debug("Rollback of " + obj.obj().getMeta().getName());
            try {
                obj.obj().rollback(
                        protoSerializerService.deserialize(obj.meta()),
                        obj.data() != null ? protoSerializerService.deserialize(obj.data()) : null);
                obj.obj().updateDeletionState();
            } finally {
                obj.obj().rwUnlock();
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
            Log.debug("Error in transaction", e);
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
            Log.debug("Error in transaction", e);
            rollback();
            throw e;
        }
    }

    void addToTx(JObject<?> obj) {
        var state = _state.get();

        if (state == null)
            throw new IllegalStateException("Transaction not running");

        Log.debug("Adding " + obj.getMeta().getName() + " to transaction");

        obj.assertRwLock();
        obj.rwLock();

        state._writeObjects.put(obj.getMeta().getName(),
                new JObjectSnapshot(
                        obj,
                        protoSerializerService.serialize(obj.getMeta()),
                        obj.getData() != null ? protoSerializerService.serializeToJObjectDataP(obj.getData()) : null,
                        obj.getMeta().externalHash()
                ));
    }

    private class TxState {
        private final HashMap<String, JObjectSnapshot> _writeObjects = new HashMap<>();
    }
}