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
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ApplicationScoped
public class TxWritebackImpl implements TxWriteback {
    private final TreeMap<Long, MutablePair<TxBundle, Boolean>> _pendingBundles = new TreeMap<>();
    private final Object _flushWaitSynchronizer = new Object();
    @Inject
    ObjectPersistentStore objectPersistentStore;
    @ConfigProperty(name = "dhfs.objects.writeback.limit")
    long sizeLimit;
    private long currentSize = 0;
    private AtomicLong _counter = new AtomicLong();
    private ExecutorService _writebackExecutor;
    private ExecutorService _commitExecutor;
    private ExecutorService _statusExecutor;
    private AtomicLong _waitedTotal = new AtomicLong(0);

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
        _statusExecutor = Executors.newSingleThreadExecutor();
        _statusExecutor.submit(() -> {
            try {
                while (true) {
                    Thread.sleep(1000);
                    if (currentSize > 0)
                        Log.info("Tx commit status: size="
                                + currentSize / 1024 / 1024 + "MB");
                }
            } catch (InterruptedException ignored) {
            }
        });
    }

    void shutdown(@Observes @Priority(10) ShutdownEvent event) {
        _writebackExecutor.shutdownNow();
        Log.info("Total tx bundle wait time: " + _waitedTotal.get() + "ms");
    }

    private void writeback() {
        while (!Thread.interrupted()) {
            try {
                TxBundle bundle = new TxBundle(0);
                synchronized (_pendingBundles) {
                    while (_pendingBundles.isEmpty()
                            || !_pendingBundles.firstEntry().getValue().getRight())
                        _pendingBundles.wait();
                    while (!_pendingBundles.isEmpty() && _pendingBundles.firstEntry().getValue().getRight()) {
                        var toCompress = _pendingBundles.pollFirstEntry().getValue().getLeft();
                        currentSize -= toCompress._size;
                        bundle.compress(toCompress);
                    }
                    currentSize += bundle.calculateTotalSize();
                }

                var latch = new CountDownLatch(bundle._committed.size() + bundle._meta.size());
                ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

                for (var c : bundle._committed.values()) {
                    _commitExecutor.execute(() -> {
                        try {
                            Log.trace("Writing new " + c.newMeta.getName());
                            objectPersistentStore.writeNewObject(c.newMeta.getName(), c.newMeta, c.newData);
                        } catch (Throwable t) {
                            Log.error("Error writing " + c.newMeta.getName(), t);
                            errors.add(t);
                        } finally {
                            latch.countDown();
                        }
                    });
                }
                for (var c : bundle._meta.values()) {
                    _commitExecutor.execute(() -> {
                        try {
                            Log.trace("Writing (meta) " + c.newMeta.getName());
                            objectPersistentStore.writeNewObjectMeta(c.newMeta.getName(), c.newMeta);
                        } catch (Throwable t) {
                            Log.error("Error writing " + c.newMeta.getName(), t);
                            errors.add(t);
                        } finally {
                            latch.countDown();
                        }
                    });
                }
                if (Log.isDebugEnabled())
                    for (var d : bundle._deleted.keySet())
                        Log.debug("Deleting from persistent storage " + d.getMeta().getName()); // FIXME: For tests

                latch.await();
                if (!errors.isEmpty()) {
                    throw new RuntimeException("Errors in writeback!");
                }
                objectPersistentStore.commitTx(
                        new TxManifest(
                                Stream.concat(bundle._committed.keySet().stream().map(t -> t.getMeta().getName()),
                                        bundle._meta.keySet().stream().map(t -> t.getMeta().getName())).collect(Collectors.toCollection(ArrayList::new)),
                                bundle._deleted.keySet().stream().map(t -> t.getMeta().getName()).collect(Collectors.toCollection(ArrayList::new))
                        ));
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
                    long started = System.currentTimeMillis();
                    while (currentSize > sizeLimit) {
                        try {
                            _flushWaitSynchronizer.wait();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    long waited = System.currentTimeMillis() - started;
                    _waitedTotal.addAndGet(waited);
                    if (Log.isTraceEnabled())
                        Log.trace("Thread " + Thread.currentThread().getName() + " waited for tx bundle for " + waited + " ms");
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
        private final HashMap<JObject<?>, CommittedEntry> _committed = new HashMap<>();
        private final HashMap<JObject<?>, CommittedMeta> _meta = new HashMap<>();
        private final HashMap<JObject<?>, Integer> _deleted = new HashMap<>();
        private long _txId;
        private long _size = -1;

        private TxBundle(long txId) {_txId = txId;}

        @Override
        public long getId() {
            return _txId;
        }

        @Override
        public void commit(JObject<?> obj, ObjectMetadataP meta, JObjectDataP data) {
            synchronized (_committed) {
                _committed.put(obj, new CommittedEntry(meta, data, obj.estimateSize()));
            }
        }

        @Override
        public void commitMetaChange(JObject<?> obj, ObjectMetadataP meta) {
            synchronized (_meta) {
                _meta.put(obj, new CommittedMeta(meta, obj.estimateSize()));
            }
        }

        @Override
        public void delete(JObject<?> obj) {
            synchronized (_deleted) {
                _deleted.put(obj, obj.estimateSize());
            }
        }

        public long calculateTotalSize() {
            if (_size >= 0) return _size;
            long out = 0;
            for (var c : _committed.values())
                out += c.size;
            for (var c : _meta.values())
                out += c.size;
            for (var c : _deleted.entrySet())
                out += c.getValue();
            _size = out;
            return _size;
        }

        public void compress(TxBundle other) {
            if (_txId >= other._txId)
                throw new IllegalArgumentException("Compressing an older bundle into newer");

            _txId = other._txId;
            _size = -1;

            for (var d : other._deleted.entrySet()) {
                _committed.remove(d.getKey());
                _meta.remove(d.getKey());
                _deleted.put(d.getKey(), d.getValue());
            }

            for (var c : other._committed.entrySet()) {
                _committed.put(c.getKey(), c.getValue());
                _meta.remove(c.getKey());
                _deleted.remove(c.getKey());
            }

            for (var m : other._meta.entrySet()) {
                var deleted = _deleted.remove(m.getKey());
                if (deleted != null) {
                    _committed.put(m.getKey(), new CommittedEntry(m.getValue().newMeta, null, m.getKey().estimateSize()));
                    continue;
                }
                var committed = _committed.remove(m.getKey());
                if (committed != null) {
                    _committed.put(m.getKey(), new CommittedEntry(m.getValue().newMeta, committed.newData, m.getKey().estimateSize()));
                    continue;
                }
                _meta.put(m.getKey(), m.getValue());
            }
        }

        private record CommittedEntry(ObjectMetadataP newMeta, JObjectDataP newData, int size) {}

        private record CommittedMeta(ObjectMetadataP newMeta, int size) {}

        private record Deleted(JObject<?> handle) {}
    }
}
