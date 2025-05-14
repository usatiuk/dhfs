package com.usatiuk.objects.stores;

import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.JObjectKeyMax;
import com.usatiuk.objects.JObjectKeyMin;
import com.usatiuk.objects.iterators.*;
import com.usatiuk.objects.snapshot.Snapshot;
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
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;

/**
 * Persistent object storage using LMDB
 */
@ApplicationScoped
@IfBuildProperty(name = "dhfs.objects.persistence", stringValue = "lmdb")
public class LmdbObjectPersistentStore implements ObjectPersistentStore {
    private static final String DB_NAME = "objects";

    // LMDB object name for the transaction id
    private static final String DB_VER_OBJ_NAME_STR = "__DB_VER_OBJ";
    private static final ByteBuffer DB_VER_OBJ_NAME;

    @ConfigProperty(name = "dhfs.objects.persistence.lmdb.size", defaultValue = "1000000000000")
    long lmdbSize;

    static {
        byte[] tmp = DB_VER_OBJ_NAME_STR.getBytes(StandardCharsets.ISO_8859_1);
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
        Log.info("Opening LMDB with root " + _root);
        _env = create()
                .setMapSize(lmdbSize)
                .setMaxDbs(1)
                .open(_root.toFile(), EnvFlags.MDB_NOTLS);
        _db = _env.openDbi(DB_NAME, MDB_CREATE);

        Log.info("Opened LMDB with root " + _root);

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
        Log.info("LMDB storage ready");
    }

    private Optional<Long> readTxId(Txn<ByteBuffer> txn) {
        var value = _db.get(txn, DB_VER_OBJ_NAME.asReadOnlyBuffer());
        return Optional.ofNullable(value).map(ByteBuffer::getLong);
    }

    void shutdown(@Observes @Priority(900) ShutdownEvent event) throws IOException {
        if (!_ready) {
            return;
        }
        _ready = false;
        _db.close();
        _env.close();
    }

    private void verifyReady() {
        if (!_ready) throw new IllegalStateException("Wrong service order!");
    }

    /**
     * Get a snapshot of the database.
     * Note that the ByteBuffers are invalid after the snapshot is closed.
     *
     * @return a snapshot of the database
     */
    @Override
    public Snapshot<JObjectKey, ByteBuffer> getSnapshot() {
        verifyReady();
        var txn = _env.txnRead();
        try {
            long commitId = readTxId(txn).orElseThrow();
            return new Snapshot<JObjectKey, ByteBuffer>() {
                private final Txn<ByteBuffer> _txn = txn;
                private final long _id = commitId;
                private boolean _closed = false;

                @Override
                public List<CloseableKvIterator<JObjectKey, MaybeTombstone<ByteBuffer>>> getIterator(IteratorStart start, JObjectKey key) {
                    assert !_closed;
                    return List.of(new KeyPredicateKvIterator<>(new LmdbKvIterator(_txn, start, key), start, key, (k) -> !k.value().equals(DB_VER_OBJ_NAME_STR)));
                }

                @Nonnull
                @Override
                public Optional<ByteBuffer> readObject(JObjectKey name) {
                    assert !_closed;
                    var got = _db.get(_txn, name.toByteBuffer());
                    var ret = Optional.ofNullable(got).map(ByteBuffer::asReadOnlyBuffer);
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
                    _txn.close();
                }
            };
        } catch (Exception e) {
            txn.close();
            throw e;
        }
    }

    @Override
    public void commitTx(TxManifestRaw names, long txId) {
        verifyReady();
        try (var txn = _env.txnWrite()) {
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
            txn.commit();
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

    private class LmdbKvIterator extends ReversibleKvIterator<JObjectKey, MaybeTombstone<ByteBuffer>> {
        private static final Cleaner CLEANER = Cleaner.create();
        private final Txn<ByteBuffer> _txn; // Managed by the snapshot
        private final Cursor<ByteBuffer> _cursor;
        private final MutableObject<Boolean> _closed = new MutableObject<>(false);
        //        private final Exception _allocationStacktrace = new Exception();
//        private final Exception _allocationStacktrace = null;
        private boolean _hasNext = false;
        private JObjectKey _peekedNextKey = null;

        LmdbKvIterator(Txn<ByteBuffer> txn, IteratorStart start, JObjectKey key) {
            _txn = txn;
            _goingForward = true;

            _cursor = _db.openCursor(_txn);

            var closedRef = _closed;
//            var bt = _allocationStacktrace;
//            CLEANER.register(this, () -> {
//                if (!closedRef.getValue()) {
//                    Log.error("Iterator was not closed before GC, allocated at: {0}", bt);
//                    System.exit(-1);
//                }
//            });

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

//            var realGot = JObjectKey.fromByteBuffer(_cursor.key());
//            _cursor.key().flip();
//
//            switch (start) {
//                case LT -> {
////                    assert !_hasNext || realGot.compareTo(key) < 0;
//                }
//                case LE -> {
////                    assert !_hasNext || realGot.compareTo(key) <= 0;
//                }
//                case GT -> {
//                    assert !_hasNext || realGot.compareTo(key) > 0;
//                }
//                case GE -> {
//                    assert !_hasNext || realGot.compareTo(key) >= 0;
//                }
//            }
//            Log.tracev("got: {0}, hasNext: {1}", realGot, _hasNext);
        }

        @Override
        public void close() {
            if (_closed.getValue()) {
                return;
            }
            _closed.setValue(true);
            _cursor.close();
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
            _peekedNextKey = null;
        }

        @Override
        protected JObjectKey peekImpl() {
            if (!_hasNext) {
                throw new NoSuchElementException("No more elements");
            }
            if (_peekedNextKey != null) {
                return _peekedNextKey;
            }
            var ret = JObjectKey.fromByteBuffer(_cursor.key());
            _cursor.key().flip();
            _peekedNextKey = ret;
            return ret;
        }

        @Override
        protected void skipImpl() {
            if (_goingForward)
                _hasNext = _cursor.next();
            else
                _hasNext = _cursor.prev();
            _peekedNextKey = null;
        }

        @Override
        protected boolean hasImpl() {
            return _hasNext;
        }

        @Override
        protected Pair<JObjectKey, MaybeTombstone<ByteBuffer>> nextImpl() {
            if (!_hasNext) {
                throw new NoSuchElementException("No more elements");
            }
            // TODO: Right now with java serialization it doesn't matter, it's all copied to arrays anyway
            var val = _cursor.val();
            Pair<JObjectKey, MaybeTombstone<ByteBuffer>> ret = Pair.of(JObjectKey.fromByteBuffer(_cursor.key()), new DataWrapper<>(val.asReadOnlyBuffer()));
            if (_goingForward)
                _hasNext = _cursor.next();
            else
                _hasNext = _cursor.prev();
//            Log.tracev("Read: {0}, hasNext: {1}", ret, _hasNext);
            _peekedNextKey = null;
            return ret;
        }
    }

}
