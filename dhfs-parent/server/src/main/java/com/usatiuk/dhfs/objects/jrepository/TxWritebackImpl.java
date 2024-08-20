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
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ApplicationScoped
public class TxWritebackImpl implements TxWriteback {
    @Inject
    ObjectPersistentStore objectPersistentStore;
    @Inject
    JObjectSizeEstimator jObjectSizeEstimator;

    @ConfigProperty(name = "dhfs.objects.writeback.limit")
    long sizeLimit;

    private final TreeMap<Long, MutablePair<TxBundle, Boolean>> _pendingBundles = new TreeMap<>();
    private final Object _flushWaitSynchronizer = new Object();
    private long currentSize = 0;
    private AtomicLong _counter = new AtomicLong();
    private ExecutorService _writebackExecutor;
    private ExecutorService _commitExecutor;

    @Startup
    void init() {
        {
            BasicThreadFactory factory = new BasicThreadFactory.Builder()
                    .namingPattern("tx-writeback-%d")
                    .build();

            _writebackExecutor = Executors.newSingleThreadExecutor(factory);
            _writebackExecutor.submit(this::writeback);
        }

        {
            BasicThreadFactory factory = new BasicThreadFactory.Builder()
                    .namingPattern("writeback-commit-%d")
                    .build();

            _commitExecutor = Executors.newFixedThreadPool(8, factory);
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


                ArrayList<Callable<Void>> tasks = new ArrayList<>();

                for (var c : bundle._committed) {
                    tasks.add(() -> {
                        Log.trace("Writing new " + c.getLeft());
                        objectPersistentStore.writeNewObject(c.getLeft().getMeta().getName(), c.getMiddle(), c.getRight());
                        return null;
                    });
                }
                for (var c : bundle._meta) {
                    tasks.add(() -> {
                        Log.trace("Writing new (meta) " + c.getLeft());
                        objectPersistentStore.writeNewObjectMeta(c.getLeft().getMeta().getName(), c.getRight());
                        return null;
                    });
                }
                if (Log.isDebugEnabled())
                    for (var d : bundle._deleted)
                        Log.debug("Deleting from persistent storage " + d.getMeta().getName()); // FIXME: For tests

                _commitExecutor.invokeAll(tasks);

                objectPersistentStore.commitTx(
                        new TxManifest(
                                Stream.concat(bundle._committed.stream().map(t -> t.getLeft().getMeta().getName()),
                                        bundle._meta.stream().map(p -> p.getLeft().getMeta().getName())).collect(Collectors.toCollection(ArrayList::new)),
                                bundle._deleted.stream().map(o -> o.getMeta().getName()).collect(Collectors.toCollection(ArrayList::new))
                        )
                );
                Log.trace("Transaction " + bundle.getId() + " committed");
                synchronized (_flushWaitSynchronizer) {
                    currentSize -= ((TxBundle) bundle).calculateTotalSize();
                    if (currentSize <= sizeLimit)
                        _flushWaitSynchronizer.notifyAll();
                }
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
        boolean wait = false;
        while (true) {
            if (wait) {
                synchronized (_flushWaitSynchronizer) {
                    while (currentSize > sizeLimit) {
                        try {
                            _flushWaitSynchronizer.wait();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    wait = false;
                }
            }
            synchronized (_pendingBundles) {
                synchronized (_flushWaitSynchronizer) {
                    if (currentSize > sizeLimit) {
                        wait = true;
                        continue;
                    }
                }
                var bundle = new TxBundle(_counter.incrementAndGet());
                _pendingBundles.put(bundle.getId(), MutablePair.of(bundle, false));
                return bundle;
            }
        }
    }

    @Override
    public void commitBundle(com.usatiuk.dhfs.objects.jrepository.TxBundle bundle) {
        synchronized (_pendingBundles) {
            _pendingBundles.get(bundle.getId()).setRight(Boolean.TRUE);
            if (_pendingBundles.firstKey().equals(bundle.getId()))
                _pendingBundles.notify();
            synchronized (_flushWaitSynchronizer) {
                currentSize += ((TxBundle) bundle).calculateTotalSize();
            }
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

        private long _size = -1;

        private final ArrayList<Triple<JObject<?>, ObjectMetadataP, JObjectDataP>> _committed = new ArrayList<>();
        private final ArrayList<Pair<JObject<?>, ObjectMetadataP>> _meta = new ArrayList<>();
        private final ArrayList<JObject<?>> _deleted = new ArrayList<>();

        private TxBundle(long txId) {_txId = txId;}

        @Override
        public long getId() {
            return _txId;
        }

        @Override
        public void commit(JObject<?> obj, ObjectMetadataP meta, JObjectDataP data) {
            synchronized (_committed) {
                _committed.add(Triple.of(obj, meta, data));
            }
        }

        @Override
        public void commitMetaChange(JObject<?> obj, ObjectMetadataP meta) {
            synchronized (_meta) {
                _meta.add(Pair.of(obj, meta));
            }
        }

        @Override
        public void delete(JObject<?> obj) {
            synchronized (_deleted) {
                _deleted.add(obj);
            }
        }

        public long calculateTotalSize() {
            if (_size >= 0) return _size;
            long out = 0;
            for (var c : _committed)
                out = out + jObjectSizeEstimator.estimateObjectSize(c.getLeft().getData()) + c.getMiddle().getSerializedSize()
                        + (c.getRight() != null ? c.getRight().getSerializedSize() : 0);
            for (var c : _meta)
                out = out + jObjectSizeEstimator.estimateObjectSize(c.getLeft().getData()) + c.getRight().getSerializedSize();
            for (var c : _deleted)
                out = out + jObjectSizeEstimator.estimateObjectSize(c.getData());
            _size = out;
            return _size;
        }
    }
}
