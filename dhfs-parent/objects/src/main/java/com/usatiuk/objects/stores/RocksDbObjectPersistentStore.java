package com.usatiuk.objects.stores;

import com.google.protobuf.ByteString;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.JObjectKeyMax;
import com.usatiuk.objects.JObjectKeyMin;
import com.usatiuk.objects.iterators.CloseableKvIterator;
import com.usatiuk.objects.iterators.IteratorStart;
import com.usatiuk.objects.iterators.ReversibleKvIterator;
import com.usatiuk.objects.snapshot.Snapshot;
import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.rocksdb.*;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.Optional;

@ApplicationScoped
@IfBuildProperty(name = "dhfs.objects.persistence", stringValue = "rocks")
public class RocksDbObjectPersistentStore implements ObjectPersistentStore {
    private static final String DB_NAME = "objects";
    private static final byte[] DB_VER_OBJ_NAME = "__DB_VER_OBJ".getBytes(StandardCharsets.UTF_8);
    private final Path _root;
    private Options _options;
    private TransactionDBOptions _transactionDBOptions;
    private TransactionDB _db;
    private boolean _ready = false;

    public RocksDbObjectPersistentStore(@ConfigProperty(name = "dhfs.objects.persistence.files.root") String root) {
        _root = Path.of(root).resolve("objects");
    }

    void init(@Observes @Priority(100) StartupEvent event) throws RocksDBException {
        if (!_root.toFile().exists()) {
            Log.info("Initializing with root " + _root);
            _root.toFile().mkdirs();
        }

        RocksDB.loadLibrary();

        _options = new Options().setCreateIfMissing(true);
        _transactionDBOptions = new TransactionDBOptions();
        _db = TransactionDB.open(_options, _transactionDBOptions, _root.toString());

        try (var txn = _db.beginTransaction(new WriteOptions())) {
            var read = readTxId(txn);
            if (read.isPresent()) {
                Log.infov("Read tx id {0}", read.get());
            } else {
                txn.put(DB_VER_OBJ_NAME, ByteBuffer.allocate(8).putLong(0).array());
                txn.commit();
            }
        }

        _ready = true;
    }

    private Optional<Long> readTxId(Transaction txn) throws RocksDBException {
        var value = txn.get(new ReadOptions(), DB_VER_OBJ_NAME);
        return Optional.ofNullable(value).map(ByteBuffer::wrap).map(ByteBuffer::getLong);
    }

    void shutdown(@Observes @Priority(900) ShutdownEvent event) {
        _ready = false;
        _db.close();
    }

    private void verifyReady() {
        if (!_ready) throw new IllegalStateException("Wrong service order!");
    }

    @Nonnull
    @Override
    public Optional<ByteString> readObject(JObjectKey name) {
        verifyReady();
        byte[] got = null;
        try {
            got = _db.get(new ReadOptions(), name.bytes());
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        return Optional.ofNullable(got).map(ByteString::copyFrom);
    }

    @Override
    public Snapshot<JObjectKey, ByteString> getSnapshot() {
        var txn = _db.beginTransaction(new WriteOptions());
        txn.setSnapshot();
        var rocksDbSnapshot = txn.getSnapshot();
        long commitId = 0;
        try {
            commitId = readTxId(txn).orElseThrow();
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        long finalCommitId = commitId;
        return new Snapshot<JObjectKey, ByteString>() {
            private final Transaction _txn = txn;
            private final long _id = finalCommitId;
            private final org.rocksdb.Snapshot _rocksDbSnapshot = rocksDbSnapshot;
            private boolean _closed = false;

            @Override
            public CloseableKvIterator<JObjectKey, ByteString> getIterator(IteratorStart start, JObjectKey key) {
                assert !_closed;
                return new RocksDbKvIterator(_txn, start, key, _rocksDbSnapshot);
            }

            @Nonnull
            @Override
            public Optional<ByteString> readObject(JObjectKey name) {
                assert !_closed;
                try (var readOptions = new ReadOptions().setSnapshot(_rocksDbSnapshot)) {
                    var got = _txn.get(readOptions, name.bytes());
                    return Optional.ofNullable(got).map(ByteString::copyFrom);
                } catch (RocksDBException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public long id() {
                assert !_closed;
                return _id;
            }

            @Override
            public void close() {
                assert !_closed;
                _closed = true;
                _txn.close();
            }
        };
    }

    @Override
    public Runnable prepareTx(TxManifestRaw names, long txId) {
        verifyReady();
        var txn = _db.beginTransaction(new WriteOptions());
        try {
            for (var written : names.written()) {
                txn.put(written.getKey().bytes(), written.getValue().toByteArray());
            }
            for (JObjectKey key : names.deleted()) {
                txn.delete(key.bytes());
            }

            assert txId > readTxId(txn).orElseThrow();

            txn.put(DB_VER_OBJ_NAME, ByteBuffer.allocate(8).putLong(txId).array());
        } catch (Throwable t) {
            txn.close();
            throw new RuntimeException(t);
        }
        return () -> {
            try {
                txn.commit();
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            } finally {
                txn.close();
            }
        };
    }

    @Override
    public long getTotalSpace() {
        verifyReady();
        return _root.toFile().getTotalSpace();
    }

    @Override
    public long getFreeSpace() {
        verifyReady();
        return _root.toFile().getFreeSpace();
    }

    @Override
    public long getUsableSpace() {
        verifyReady();
        return _root.toFile().getUsableSpace();
    }

    private class RocksDbKvIterator extends ReversibleKvIterator<JObjectKey, ByteString> {
        private final RocksIterator _iterator;
        private final org.rocksdb.Snapshot _rocksDbSnapshot;
        private final ReadOptions _readOptions;
        private boolean _hasNext;

        RocksDbKvIterator(Transaction txn, IteratorStart start, JObjectKey key, org.rocksdb.Snapshot rocksDbSnapshot) {
            _rocksDbSnapshot = rocksDbSnapshot;
            _readOptions = new ReadOptions().setSnapshot(_rocksDbSnapshot);
            _iterator = txn.getIterator(_readOptions);
            verifyReady();

            if (key instanceof JObjectKeyMin) {
                _iterator.seekToFirst();
            } else if (key instanceof JObjectKeyMax) {
                _iterator.seekToLast();
            } else {
                _iterator.seek(key.bytes());
            }
            _hasNext = _iterator.isValid();
        }

        @Override
        public void close() {
            _iterator.close();
        }

        @Override
        protected void reverse() {
            if (_hasNext) {
                if (_goingForward) {
                    _iterator.prev();
                } else {
                    _iterator.next();
                }
            } else {
                if (_goingForward) {
                    _iterator.seekToLast();
                } else {
                    _iterator.seekToFirst();
                }
            }
            _goingForward = !_goingForward;
            _hasNext = _iterator.isValid();
        }

        @Override
        protected JObjectKey peekImpl() {
            if (!_hasNext) {
                throw new NoSuchElementException("No more elements");
            }
            return JObjectKey.fromByteBuffer(ByteBuffer.wrap(_iterator.key()));
        }

        @Override
        protected void skipImpl() {
            if (_goingForward) {
                _iterator.next();
            } else {
                _iterator.prev();
            }
            _hasNext = _iterator.isValid();
        }

        @Override
        protected boolean hasImpl() {
            return _hasNext;
        }

        @Override
        protected Pair<JObjectKey, ByteString> nextImpl() {
            if (!_hasNext) {
                throw new NoSuchElementException("No more elements");
            }
            var key = JObjectKey.fromByteBuffer(ByteBuffer.wrap(_iterator.key()));
            var value = ByteString.copyFrom(_iterator.value());
            if (_goingForward) {
                _iterator.next();
            } else {
                _iterator.prev();
            }
            _hasNext = _iterator.isValid();
            return Pair.of(key, value);
        }
    }
}
