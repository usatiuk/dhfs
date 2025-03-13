package com.usatiuk.dhfs.objects.persistence;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.*;
import com.usatiuk.dhfs.objects.snapshot.Snapshot;
import com.usatiuk.dhfs.supportlib.UninitializedByteBuffer;
import com.usatiuk.dhfs.utils.RefcountedCloseable;
import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.lmdbjava.*;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;

@ApplicationScoped
@IfBuildProperty(name = "dhfs.objects.persistence", stringValue = "lmdb")
public class LmdbObjectPersistentStore implements ObjectPersistentStore {
    private final Path _root;
    private Env<ByteBuffer> _env;
    private Dbi<ByteBuffer> _db;
    private boolean _ready = false;
    private final AtomicReference<RefcountedCloseable<Txn<ByteBuffer>>> _curReadTxn = new AtomicReference<>();

    private long _lastTxId = 0;

    private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();

    private static final String DB_NAME = "objects";
    private static final byte[] DB_VER_OBJ_NAME = "__DB_VER_OBJ".getBytes(StandardCharsets.UTF_8);

    public LmdbObjectPersistentStore(@ConfigProperty(name = "dhfs.objects.persistence.files.root") String root) {
        _root = Path.of(root).resolve("objects");
    }

    void init(@Observes @Priority(100) StartupEvent event) throws IOException {
        if (!_root.toFile().exists()) {
            Log.info("Initializing with root " + _root);
            _root.toFile().mkdirs();
        }
        _env = create()
                .setMapSize(1_000_000_000_000L)
                .setMaxDbs(1)
                .open(_root.toFile(), EnvFlags.MDB_NOTLS);
        _db = _env.openDbi(DB_NAME, MDB_CREATE);

        var bb = ByteBuffer.allocateDirect(DB_VER_OBJ_NAME.length);
        bb.put(DB_VER_OBJ_NAME);
        bb.flip();

        try (Txn<ByteBuffer> txn = _env.txnRead()) {
            var value = _db.get(txn, bb);
            if (value != null) {
                var ver = value.getLong();
                Log.infov("Read version: {0}", ver);
                _lastTxId = ver;
            }
        }

        _ready = true;
    }

    void shutdown(@Observes @Priority(900) ShutdownEvent event) throws IOException {
        _ready = false;
        _db.close();
        _env.close();
    }

    private void verifyReady() {
        if (!_ready) throw new IllegalStateException("Wrong service order!");
    }

    @Nonnull
    @Override
    public Collection<JObjectKey> findAllObjects() {
//        try (Txn<ByteBuffer> txn = env.txnRead()) {
//            try (var cursor = db.openCursor(txn)) {
//                var keys = List.of();
//                while (cursor.next()) {
//                    keys.add(JObjectKey.fromBytes(cursor.key()));
//                }
//                return keys;
//            }
//        }
        return List.of();
    }


    @Nonnull
    @Override
    public Optional<ByteString> readObject(JObjectKey name) {
        verifyReady();
        try (Txn<ByteBuffer> txn = _env.txnRead()) {
            var value = _db.get(txn, name.toByteBuffer());
            return Optional.ofNullable(value).map(ByteString::copyFrom);
        }
    }

    private RefcountedCloseable<Txn<ByteBuffer>> getCurTxn() {
        _lock.readLock().lock();
        try {
            var got = _curReadTxn.get();
            var refInc = Optional.ofNullable(got).map(RefcountedCloseable::ref).orElse(null);
            if (refInc != null) {
                return got;
            } else {
                var newTxn = new RefcountedCloseable<>(_env.txnRead());
                _curReadTxn.compareAndSet(got, newTxn);
                return newTxn;
            }
        } finally {
            _lock.readLock().unlock();
        }
    }

    private class LmdbKvIterator extends ReversibleKvIterator<JObjectKey, ByteString> {
        private final RefcountedCloseable<Txn<ByteBuffer>> _txn;
        private final Cursor<ByteBuffer> _cursor;
        private boolean _hasNext = false;

        private static final Cleaner CLEANER = Cleaner.create();
        private final MutableObject<Boolean> _closed = new MutableObject<>(false);
        //        private final Exception _allocationStacktrace = new Exception();
        private final Exception _allocationStacktrace = null;

        LmdbKvIterator(RefcountedCloseable<Txn<ByteBuffer>> txn, IteratorStart start, JObjectKey key) {
            _txn = txn;
            _goingForward = true;

            _cursor = _db.openCursor(_txn.get());

            var closedRef = _closed;
            var bt = _allocationStacktrace;
            CLEANER.register(this, () -> {
                if (!closedRef.getValue()) {
                    Log.error("Iterator was not closed before GC, allocated at: {0}", bt);
                    System.exit(-1);
                }
            });

            verifyReady();
            if (!_cursor.get(key.toByteBuffer(), GetOp.MDB_SET_RANGE)) {
                return;
            }

            var got = JObjectKey.fromByteBuffer(_cursor.key());
            _cursor.key().flip();
            var cmp = got.compareTo(key);

            assert cmp >= 0;

            _hasNext = true;

            if (cmp == 0) {
                switch (start) {
                    case LT -> {
                        _hasNext = _cursor.prev();
                        if (!_hasNext) {
                            _hasNext = _cursor.first();
                        }
                    }
                    case GT -> {
                        _hasNext = _cursor.next();
                    }
                    case LE, GE -> {
                    }
                }
            } else {
                switch (start) {
                    case LT, LE -> {
                        _hasNext = _cursor.prev();
                        if (!_hasNext) {
                            _hasNext = _cursor.first();
                        }
                    }
                    case GT, GE -> {
                    }
                }
            }

            var realGot = JObjectKey.fromByteBuffer(_cursor.key());
            _cursor.key().flip();

            switch (start) {
                case LT -> {
//                    assert !_hasNext || realGot.compareTo(key) < 0;
                }
                case LE -> {
//                    assert !_hasNext || realGot.compareTo(key) <= 0;
                }
                case GT -> {
                    assert !_hasNext || realGot.compareTo(key) > 0;
                }
                case GE -> {
                    assert !_hasNext || realGot.compareTo(key) >= 0;
                }
            }
            Log.tracev("got: {0}, hasNext: {1}", realGot, _hasNext);
        }

        LmdbKvIterator(IteratorStart start, JObjectKey key) {
            this(getCurTxn(), start, key);
        }

        @Override
        public void close() {
            if (_closed.getValue()) {
                return;
            }
            _closed.setValue(true);
            _cursor.close();
            _txn.unref();
        }

        @Override
        protected void reverse() {
            if (_hasNext) {
                if (_goingForward) {
                    _hasNext = _cursor.prev();
                } else {
                    _hasNext = _cursor.next();
                }
            } else {
                if (_goingForward) {
                    _hasNext = _cursor.last();
                } else {
                    _hasNext = _cursor.first();
                }
            }
            _goingForward = !_goingForward;
        }

        @Override
        protected JObjectKey peekImpl() {
            if (!_hasNext) {
                throw new NoSuchElementException("No more elements");
            }
            var ret = JObjectKey.fromByteBuffer(_cursor.key());
            _cursor.key().flip();
            return ret;
        }

        @Override
        protected void skipImpl() {
            if (_goingForward)
                _hasNext = _cursor.next();
            else
                _hasNext = _cursor.prev();
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
            // TODO: Right now with java serialization it doesn't matter, it's all copied to arrays anyway
//            var val = _cursor.val();
//            var bbDirect = UninitializedByteBuffer.allocateUninitialized(val.remaining());
//            bbDirect.put(val);
//            bbDirect.flip();
//            var bs = UnsafeByteOperations.unsafeWrap(bbDirect);
//            var ret = Pair.of(JObjectKey.fromByteBuffer(_cursor.key()), bs);
            var ret = Pair.of(JObjectKey.fromByteBuffer(_cursor.key()), ByteString.copyFrom(_cursor.val()));
            if (_goingForward)
                _hasNext = _cursor.next();
            else
                _hasNext = _cursor.prev();
            Log.tracev("Read: {0}, hasNext: {1}", ret, _hasNext);
            return ret;
        }
    }

    @Override
    public CloseableKvIterator<JObjectKey, ByteString> getIterator(IteratorStart start, JObjectKey key) {
        return new KeyPredicateKvIterator<>(new LmdbKvIterator(start, key), start, key, (k) -> !Arrays.equals(k.name().getBytes(StandardCharsets.UTF_8), DB_VER_OBJ_NAME));
    }

    @Override
    public Snapshot<JObjectKey, ByteString> getSnapshot() {
        _lock.readLock().lock();
        try {
            var txn = new RefcountedCloseable<>(_env.txnRead());
            var commitId = getLastCommitId();
            return new Snapshot<JObjectKey, ByteString>() {
                private boolean _closed = false;
                private final RefcountedCloseable<Txn<ByteBuffer>> _txn = txn;
                private final long _id = commitId;

                @Override
                public CloseableKvIterator<JObjectKey, ByteString> getIterator(IteratorStart start, JObjectKey key) {
                    assert !_closed;
                    return new KeyPredicateKvIterator<>(new LmdbKvIterator(_txn.ref(), start, key), start, key, (k) -> !Arrays.equals(k.name().getBytes(StandardCharsets.UTF_8), DB_VER_OBJ_NAME));
                }

                @Nonnull
                @Override
                public Optional<ByteString> readObject(JObjectKey name) {
                    assert !_closed;
                    var got = _db.get(_txn.get(), name.toByteBuffer());
                    var ret = Optional.ofNullable(got).map(ByteString::copyFrom);
                    return ret;
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
                    _txn.unref();
                }
            };
        } finally {
            _lock.readLock().unlock();
        }
    }

    @Override
    public void commitTx(TxManifestRaw names, long txId, Consumer<Runnable> commitLocked) {
        verifyReady();
        try (Txn<ByteBuffer> txn = _env.txnWrite()) {
            for (var written : names.written()) {
                // TODO:
                var bb = UninitializedByteBuffer.allocateUninitialized(written.getValue().size());
                bb.put(written.getValue().asReadOnlyByteBuffer());
                bb.flip();
                _db.put(txn, written.getKey().toByteBuffer(), bb);
            }
            for (JObjectKey key : names.deleted()) {
                _db.delete(txn, key.toByteBuffer());
            }

            var bb = ByteBuffer.allocateDirect(DB_VER_OBJ_NAME.length);
            bb.put(DB_VER_OBJ_NAME);
            bb.flip();
            var bbData = ByteBuffer.allocateDirect(8);

            commitLocked.accept(() -> {
                _lock.writeLock().lock();
                try {
                    var realTxId = txId;
                    if (realTxId == -1)
                        realTxId = _lastTxId + 1;

                    assert realTxId > _lastTxId;
                    _lastTxId = realTxId;

                    bbData.putLong(realTxId);
                    bbData.flip();
                    _db.put(txn, bb, bbData);

                    _curReadTxn.set(null);

                    txn.commit();
                } finally {
                    _lock.writeLock().unlock();
                }
            });
        }
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

    @Override
    public long getLastCommitId() {
        _lock.readLock().lock();
        try {
            return _lastTxId;
        } finally {
            _lock.readLock().unlock();
        }
    }

}
