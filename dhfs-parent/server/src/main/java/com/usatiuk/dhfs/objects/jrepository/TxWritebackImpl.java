package com.usatiuk.dhfs.objects.jrepository;

import com.usatiuk.dhfs.objects.persistence.JObjectDataP;
import com.usatiuk.dhfs.objects.persistence.ObjectMetadataP;
import com.usatiuk.dhfs.objects.repository.persistence.ObjectPersistentStore;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import lombok.Getter;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ApplicationScoped
public class TxWritebackImpl implements TxWriteback {
    private final TreeMap<Long, MutablePair<TxBundle, Boolean>> _pendingBundles = new TreeMap<>();
    @Inject
    ObjectPersistentStore objectPersistentStore;
    private AtomicLong _counter = new AtomicLong();
    private ExecutorService _writebackExecutor;

    @Startup
    void init() {
        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("com-writeback-%d")
                .build();

        _writebackExecutor = Executors.newFixedThreadPool(1, factory);
        for (int i = 0; i < 1; i++) {
            _writebackExecutor.submit(this::writeback);
        }
    }

    void shutdown(@Observes @Priority(10) ShutdownEvent event) {
        _writebackExecutor.shutdownNow();

    }

    private void writeback() {
        while (!Thread.interrupted()) {
            try {
                TxBundle bundle;
                synchronized (_pendingBundles) {
                    while (_pendingBundles.isEmpty()
                            || !_pendingBundles.firstEntry().getValue().getRight())
                        _pendingBundles.wait();
                    bundle = _pendingBundles.pollFirstEntry().getValue().getLeft();
                }

                for (var c : bundle._committed) {
                    Log.trace("Writing new " + c.getLeft());
                    objectPersistentStore.writeNewObject(c.getLeft(), c.getMiddle(), c.getRight());
                }
                for (var c : bundle._meta) {
                    Log.trace("Writing new (meta) " + c.getLeft());
                    objectPersistentStore.writeNewObjectMeta(c.getLeft(), c.getRight());
                }
                if (Log.isDebugEnabled())
                    for (var d : bundle._deleted)
                        Log.debug("Deleting from persistent storage " + d); // FIXME: For tests

                objectPersistentStore.commitTx(
                        new TxManifest(
                                Stream.concat(bundle._committed.stream().map(t -> t.getLeft()),
                                        bundle._meta.stream().map(p -> p.getLeft())).collect(Collectors.toCollection(ArrayList::new)),
                                new ArrayList<>(bundle._deleted)
                        )
                );
                Log.trace("Transaction " + bundle.getId() + " committed");
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                Log.error("Uncaught exception in writeback", e);
            } catch (Throwable o) {
                Log.error("Uncaught THROWABLE in writeback", o);
            }
        }
        Log.info("Writeback thread exiting");
    }

    @Override
    public com.usatiuk.dhfs.objects.jrepository.TxBundle createBundle() {
        synchronized (_pendingBundles) {
            var bundle = new TxBundle(_counter.incrementAndGet());
            _pendingBundles.put(bundle.getId(), MutablePair.of(bundle, false));
            return bundle;
        }
    }

    @Override
    public void commitBundle(com.usatiuk.dhfs.objects.jrepository.TxBundle bundle) {
        synchronized (_pendingBundles) {
            _pendingBundles.get(bundle.getId()).setRight(Boolean.TRUE);
            if (_pendingBundles.firstKey().equals(bundle.getId()))
                _pendingBundles.notify();
        }
    }

    @Override
    public void fence(long txId) {
    }

    @Getter
    private static class TxManifest implements com.usatiuk.dhfs.objects.repository.persistence.TxManifest {
        private final ArrayList<String> _written;
        private final ArrayList<String> _deleted;

        private TxManifest(ArrayList<String> written, ArrayList<String> deleted) {
            _written = written;
            _deleted = deleted;
        }
    }

    private class TxBundle implements com.usatiuk.dhfs.objects.jrepository.TxBundle {
        private final long _txId;

        private final ArrayList<Triple<String, ObjectMetadataP, JObjectDataP>> _committed = new ArrayList<>();
        private final ArrayList<Pair<String, ObjectMetadataP>> _meta = new ArrayList<>();
        private final ArrayList<String> _deleted = new ArrayList<>();

        private TxBundle(long txId) {_txId = txId;}

        @Override
        public long getId() {
            return _txId;
        }

        @Override
        public void commit(String objName, ObjectMetadataP meta, JObjectDataP data) {
            _committed.add(Triple.of(objName, meta, data));
        }

        @Override
        public void commitMetaChange(String objName, ObjectMetadataP meta) {
            _meta.add(Pair.of(objName, meta));
        }

        @Override
        public void delete(String objName) {
            _deleted.add(objName);
        }
    }
}
