package com.usatiuk.objects.stores;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.JObjectKeyMax;
import com.usatiuk.objects.JObjectKeyMin;
import com.usatiuk.objects.iterators.CloseableKvIterator;
import com.usatiuk.objects.iterators.IteratorStart;
import com.usatiuk.objects.iterators.KeyPredicateKvIterator;
import com.usatiuk.objects.iterators.ReversibleKvIterator;
import com.usatiuk.objects.snapshot.Snapshot;
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
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Optional;

import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;

@ApplicationScoped
@IfBuildProperty(name = "dhfs.objects.persistence", stringValue = "lmdb")
public class LmdbObjectPersistentStore implements ObjectPersistentStore {
    private static final String DB_NAME = "objects";
    private static final ByteBuffer DB_VER_OBJ_NAME;

    static {
        byte[] tmp = "__DB_VER_OBJ".getBytes(StandardCharsets.UTF_8);
        var bb = ByteBuffer.allocateDirect(tmp.length);
        bb.put(tmp);
        bb.flip();
        DB_VER_OBJ_NAME = bb.asReadOnlyBuffer();
    }

    private final Path _root;
    private Env<ByteBuffer> _env;
    private Dbi<ByteBuffer> _db;
    private boolean _ready = false;

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

        try (Txn<ByteBuffer> txn = _env.txnWrite()) {
            var read = readTxId(txn);
            if (read.isPresent()) {
                Log.infov("Read tx id {0}", read.get());
            } else {
                var bbData = ByteBuffer.allocateDirect(8);
                bbData.putLong(0);
                bbData.flip();
                _db.put(txn, DB_VER_OBJ_NAME.asReadOnlyBuffer(), bbData);
                txn.commit();
            }
        }

        _ready = true;
    }

    private Optional<Long> readTxId(Txn<ByteBuffer> txn) {
        var value = _db.get(txn, DB_VER_OBJ_NAME.asReadOnlyBuffer());
        return Optional.ofNullable(value).map(ByteBuffer::getLong);
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
    public Optional<ByteString> readObject(JObjectKey name) {
        verifyReady();
        try (Txn<ByteBuffer> txn = _env.txnRead()) {
            var value = _db.get(txn, name.toByteBuffer());
            return Optional.ofNullable(value).map(ByteString::copyFrom);
        }
    }

    @Override
    public Snapshot<JObjectKey, ByteString> getSnapshot() {
        var txn = new RefcountedCloseable<>(_env.txnRead());
        long commitId = readTxId(txn.get()).orElseThrow();
        return new Snapshot<JObjectKey, ByteString>() {
            private final RefcountedCloseable<Txn<ByteBuffer>> _txn = txn;
            private final long _id = commitId;
            private boolean _closed = false;

            @Override
            public CloseableKvIterator<JObjectKey, ByteString> getIterator(IteratorStart start, JObjectKey key) {
                assert !_closed;
                return new KeyPredicateKvIterator<>(new LmdbKvIterator(_txn.ref(), start, key), start, key, (k) -> !StandardCharsets.UTF_8.encode(k.value()).equals(DB_VER_OBJ_NAME.asReadOnlyBuffer()));
            }

            @Nonnull
            @Override
            public Optional<ByteString> readObject(JObjectKey name) {
                assert !_closed;
                var got = _db.get(_txn.get(), name.toByteBuffer());
                var ret = Optional.ofNullable(got).map(read -> {
                    var uninitBb = UninitializedByteBuffer.allocateUninitialized(got.remaining());
                    uninitBb.put(got);
                    uninitBb.flip();
                    return UnsafeByteOperations.unsafeWrap(uninitBb);
                });
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
    }

    @Override
    public Runnable prepareTx(TxManifestRaw names, long txId) {
        verifyReady();
        var txn = _env.txnWrite();
        try {
            for (var written : names.written()) {
                var putBb = _db.reserve(txn, written.getKey().toByteBuffer(), written.getValue().size());
                written.getValue().copyTo(putBb);
            }
            for (JObjectKey key : names.deleted()) {
                _db.delete(txn, key.toByteBuffer());
            }

            assert txId > readTxId(txn).orElseThrow();

            var bbData = ByteBuffer.allocateDirect(8);
            bbData.putLong(txId);
            bbData.flip();
            _db.put(txn, DB_VER_OBJ_NAME.asReadOnlyBuffer(), bbData);
        } catch (Throwable t) {
            txn.close();
            throw t;
        }
        return () -> {
            try {
                txn.commit();
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

    private class LmdbKvIterator extends ReversibleKvIterator<JObjectKey, ByteString> {
        private static final Cleaner CLEANER = Cleaner.create();
        private final RefcountedCloseable<Txn<ByteBuffer>> _txn;
        private final Cursor<ByteBuffer> _cursor;
        private final MutableObject<Boolean> _closed = new MutableObject<>(false);
        //        private final Exception _allocationStacktrace = new Exception();
        private final Exception _allocationStacktrace = null;
        private boolean _hasNext = false;

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

            if (key instanceof JObjectKeyMin) {
                _hasNext = _cursor.first();
                return;
            } else if (key instanceof JObjectKeyMax) {
                _hasNext = _cursor.last();
                return;
            }


            if (key.toByteBuffer().remaining() == 0) {
                if (!_cursor.first())
                    return;
            } else if (!_cursor.get(key.toByteBuffer(), GetOp.MDB_SET_RANGE)) {
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

}
