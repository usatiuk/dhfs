package com.usatiuk.dhfs.objects.jrepository;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.persistence.JObjectDataP;
import com.usatiuk.dhfs.objects.persistence.ObjectMetadataP;
import com.usatiuk.dhfs.objects.repository.invalidation.InvalidationQueueService;
import com.usatiuk.utils.VoidFn;
import io.quarkus.logging.Log;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

@ApplicationScoped
public class JObjectTxManager {
    private final ThreadLocal<TxState> _state = new ThreadLocal<>();

    @Inject
    ProtoSerializer<JObjectDataP, JObjectData> dataProtoSerializer;
    @Inject
    ProtoSerializer<ObjectMetadataP, ObjectMetadata> metaProtoSerializer;

    @Inject
    JObjectLRU jObjectLRU;
    @Inject
    JObjectManager jObjectManager;
    @Inject
    InvalidationQueueService invalidationQueueService;
    @Inject
    TxWriteback txWriteback;
    @ConfigProperty(name = "dhfs.objects.ref_verification")
    boolean refVerification;
    private final ExecutorService _serializerThreads;
    private final AtomicLong _transientTxId = new AtomicLong();

    public JObjectTxManager() {
        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("tx-serializer-%d")
                .build();

        // FIXME:
        _serializerThreads = Executors.newFixedThreadPool(16, factory);
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

    // Returns Id of bundle to wait for, or -1 if there was nothing written
    public long commit() {
        var state = _state.get();
        if (state == null)
            throw new IllegalStateException("Transaction not running");

        if (state._writeObjects.isEmpty()) {
//            Log.trace("Empty transaction " + state._id);
            state._callbacks.forEach(c -> c.accept(null));
            _state.remove();
            return -1;
        }

        Log.debug("Committing transaction " + state._id);

        for (var obj : state._writeObjects.entrySet()) {
            Log.debug("Committing " + obj.getKey().getMeta().getName() + " deleted=" + obj.getKey().getMeta().isDeleted() + " deletion-candidate=" + obj.getKey().getMeta().isDeletionCandidate());

            var dataDiff = obj.getValue().snapshot == null
                    || obj.getKey().getMeta().changelogHash() != obj.getValue().snapshot.changelogHash()
                    || obj.getValue()._forceInvalidated;

            if (refVerification) {
                boolean dataDiffReal = obj.getValue().snapshot == null
                        || (obj.getValue().snapshot.data() == null) != (obj.getKey().getData() == null)
                        || (obj.getKey().getData() != null &&
                        !Objects.equals(obj.getValue().snapshot.data(), dataProtoSerializer.serialize(obj.getKey().getData())));

                if (dataDiffReal && !dataDiff && obj.getValue().snapshot.data() != null) {
                    var msg = "Data diff not equal for " + obj.getKey().getMeta().getName() + " " + obj.getKey().getData() + " before = " + ((obj.getValue().snapshot != null) ? obj.getValue().snapshot.data() : null) + " after = " + ((obj.getKey().getData() != null) ? dataProtoSerializer.serialize(obj.getKey().getData()) : null);
                    throw new IllegalStateException(msg);
                }
                if (dataDiff && !dataDiffReal)
                    Log.warn("Useless update for " + obj.getKey().getMeta().getName());
            }

//            if (obj.getValue()._copy && !obj.getValue()._mutators.isEmpty())
//                throw new IllegalStateException("Object copied but had mutators!");

            if (refVerification && !obj.getValue()._copy) {
                var cur = dataProtoSerializer.serialize(obj.getKey().getData());
                for (var mut : obj.getValue()._mutators.reversed())
                    revertMutator(obj.getKey(), mut);
                var rev = dataProtoSerializer.serialize(obj.getKey().getData());

                if (obj.getValue().snapshot.data() != null && !Objects.equals(rev, obj.getValue().snapshot.data()))
                    throw new IllegalStateException("Mutator could not be reverted for object " + obj.getKey().getMeta().getName() + "\n old = " + obj.getValue().snapshot.data() + "\n reverted = " + rev + "\n");

                for (var mut : obj.getValue()._mutators)
                    applyMutator(obj.getKey(), mut);

                var cur2 = dataProtoSerializer.serialize(obj.getKey().getData());
                if (!Objects.equals(cur, cur2))
                    throw new IllegalStateException("Mutator could not be reapplied for object " + obj.getKey().getMeta().getName() + "\n old = " + cur + "\n reapplied = " + cur2 + "\n");
            }

            notifyWrite(obj.getKey(),
                    obj.getValue().snapshot == null || !Objects.equals(obj.getValue().snapshot.meta(), metaProtoSerializer.serialize(obj.getKey().getMeta())),
                    obj.getValue().snapshot == null || dataDiff);

            if (refVerification) {
                var oldRefs = (obj.getValue().snapshot == null || obj.getValue().snapshot.data() == null)
                        ? null
                        : ((JObjectData) dataProtoSerializer.deserialize(obj.getValue().snapshot.data())).extractRefs();
                verifyRefs(obj.getKey(), oldRefs);
            }
        }

        var bundle = txWriteback.createBundle();
        var latch = new CountDownLatch(state._writeObjects.size());
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

        state._writeObjects.forEach((key, value) -> {
            try {
                var dataDiff = value.snapshot == null
                        || key.getMeta().changelogHash() != value.snapshot.changelogHash()
                        || value._forceInvalidated;

                key.getMeta().setLastModifiedTx(bundle.getId());
                if (key.getMeta().isDeleted()) {
                    bundle.delete(key);
                    latch.countDown();
                } else if (key.getMeta().isHaveLocalCopy() && (key.getData() != null && dataDiff)) {
                    _serializerThreads.execute(() -> {
                        try {
                            bundle.commit(key,
                                    metaProtoSerializer.serialize(key.getMeta()),
                                    dataProtoSerializer.serialize(key.getData())
                            );
                        } catch (Throwable t) {
                            Log.error("Error serializing " + key.getMeta().getName(), t);
                            errors.add(t);
                        } finally {
                            latch.countDown();
                        }
                    });
                } else if (key.getMeta().isHaveLocalCopy() && !dataDiff) {
                    bundle.commitMetaChange(key,
                            metaProtoSerializer.serialize(key.getMeta())
                    );
                    latch.countDown();
                } else if (!key.getMeta().isHaveLocalCopy()) {
                    bundle.commit(key,
                            metaProtoSerializer.serialize(key.getMeta()),
                            null
                    );
                    latch.countDown();
                } else {
                    throw new IllegalStateException("Unexpected object flush combination");
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

        if (!errors.isEmpty()) {
            throw new RuntimeException("Errors when committing!");
        }

        state._writeObjects.forEach((key, value) -> key.rwUnlock());

        state._callbacks.forEach(s -> txWriteback.asyncFence(bundle.getId(), () -> s.accept(null)));

        txWriteback.commitBundle(bundle);

        _state.remove();

        return bundle.getId();
    }

    private <T extends JObjectData> void notifyWrite(JObject<?> obj, boolean metaChanged, boolean hasDataChanged) {
        jObjectLRU.updateSize(obj);
        jObjectManager.runWriteListeners(obj, metaChanged, hasDataChanged);
        if (hasDataChanged && obj.getMeta().isHaveLocalCopy()) {
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

    private <T extends JObjectData> void applyMutator(JObject<?> obj, JMutator<T> mutator) {
        mutator.mutate((T) obj.getData());
    }

    private <T extends JObjectData> void revertMutator(JObject<?> obj, JMutator<T> mutator) {
        mutator.revert((T) obj.getData());
    }

    public void rollback(String message) {
        var state = _state.get();
        if (state == null)
            throw new IllegalStateException("Transaction not running");
        Log.debug("Rollback of " + state._id);

        for (var obj : state._writeObjects.entrySet()) {
            Log.debug("Rollback of " + obj.getKey().getMeta().getName());
            try {
                if (obj.getValue()._copy) {
                    obj.getKey().rollback(
                            metaProtoSerializer.deserialize(obj.getValue().snapshot.meta()),
                            obj.getValue().snapshot.data() != null ? dataProtoSerializer.deserialize(obj.getValue().snapshot.data()) : null);
                } else {
                    for (var mut : obj.getValue()._mutators.reversed())
                        revertMutator(obj.getKey(), mut);
                    obj.getKey().rollback(metaProtoSerializer.deserialize(obj.getValue().snapshot.meta()), obj.getKey().getData());
                }
                obj.getKey().updateDeletionState();
            } finally {
                obj.getKey().rwUnlock();
            }
        }

        state._callbacks.forEach(c -> c.accept(message != null ? message : "Unknown error"));
        Log.debug("Rollback of " + state._id + " done");
        _state.remove();
    }

    public void executeTxAndFlushAsync(VoidFn fn, Consumer<String> callback) {
        var state = _state.get();
        if (state != null) {
            _state.get()._callbacks.add(callback);
            fn.apply();
            return;
        }

        begin();
        try {
            _state.get()._callbacks.add(callback);
            fn.apply();
            commit();
        } catch (Exception e) {
            Log.debug("Error in transaction " + _state.get()._id, e);
            rollback(e.getMessage());
            throw e;
        }
    }

    public void executeTxAndFlush(VoidFn fn) {
        executeTxAndFlush(() -> {
            fn.apply();
            return null;
        });
    }

    public <T> T executeTxAndFlush(Supplier<T> fn) {
        if (_state.get() != null) {
            throw new IllegalStateException("Can't wait for transaction to flush from non-top-level tx");
        }

        begin();
        try {
            var ret = fn.get();
            var bundleId = commit();
            if (bundleId != -1)
                txWriteback.fence(bundleId);
            return ret;
        } catch (Exception e) {
            Log.debug("Error in transaction " + _state.get()._id, e);
            rollback(e.getMessage());
            throw e;
        }
    }

    public void executeTx(VoidFn fn) {
        executeTx(() -> {
            fn.apply();
            return null;
        });
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
            rollback(e.getMessage());
            throw e;
        }
    }

    public void forceInvalidate(JObject<?> obj) {
        var state = _state.get();

        if (state == null)
            throw new IllegalStateException("Transaction not running");

        obj.assertRwLock();

        var got = state._writeObjects.get(obj);
        if (got != null)
            got._forceInvalidated = true;
    }

    void addToTx(JObject<?> obj, boolean copy) {
        var state = _state.get();

        if (state == null)
            throw new IllegalStateException("Transaction not running");

        Log.debug("Adding " + obj.getMeta().getName() + " to transaction " + state._id);

        obj.assertRwLock();
        obj.rwLock();

        var snapshot = copy
                ? new JObjectSnapshot(
                metaProtoSerializer.serialize(obj.getMeta()),
                (obj.getData() == null) ? null : dataProtoSerializer.serialize(obj.getData()),
                obj.getMeta().changelogHash())
                : new JObjectSnapshot(
                metaProtoSerializer.serialize(obj.getMeta()), (!refVerification || (obj.getData() == null)) ? null : dataProtoSerializer.serialize(obj.getData()),
                obj.getMeta().changelogHash());

        state._writeObjects.put(obj, new TxState.TxObjectState(snapshot, copy));
    }

    <T extends JObjectData> void addMutator(JObject<T> obj, JMutator<? super T> mut) {
        var state = _state.get();

        if (state == null)
            throw new IllegalStateException("Transaction not running");

        obj.assertRwLock();

        //TODO: Asserts for rwLock/rwLockNoCopy?

        var got = state._writeObjects.get(obj);
        if (got == null) throw new IllegalStateException("Object not in transaction");
        if (got._copy)
            throw new IllegalStateException("Mutating object locked with copy?");
        got._mutators.addLast(mut);
    }

    private class TxState {
        private final long _id = _transientTxId.incrementAndGet();
        private final HashMap<JObject<?>, TxObjectState> _writeObjects = new HashMap<>();
        private final ArrayList<Consumer<String>> _callbacks = new ArrayList<>();

        private static class TxObjectState {
            final JObjectSnapshot snapshot;
            final List<JMutator<?>> _mutators = new LinkedList<>();
            boolean _forceInvalidated = false;
            final boolean _copy;

            private TxObjectState(JObjectSnapshot snapshot, boolean copy) {
                this.snapshot = snapshot;
                _copy = copy;
            }
        }
    }
}
