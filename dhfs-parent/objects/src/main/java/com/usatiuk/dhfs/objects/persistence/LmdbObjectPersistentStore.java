package com.usatiuk.dhfs.objects.persistence;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.CloseableKvIterator;
import com.usatiuk.dhfs.objects.JObjectKey;
import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.lmdbjava.*;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;

@ApplicationScoped
@IfBuildProperty(name = "dhfs.objects.persistence", stringValue = "lmdb")
public class LmdbObjectPersistentStore implements ObjectPersistentStore {
    private final Path _root;
    private Env<byte[]> _env;
    private Dbi<byte[]> _db;
    private boolean _ready = false;

    private static final String DB_NAME = "objects";

    public LmdbObjectPersistentStore(@ConfigProperty(name = "dhfs.objects.persistence.files.root") String root) {
        _root = Path.of(root).resolve("objects");
    }

    void init(@Observes @Priority(100) StartupEvent event) throws IOException {
        if (!_root.toFile().exists()) {
            Log.info("Initializing with root " + _root);
            _root.toFile().mkdirs();
        }
        _env = create(ByteArrayProxy.PROXY_BA)
                .setMapSize(1_000_000_000_000L)
                .setMaxDbs(1)
                .open(_root.toFile(), EnvFlags.MDB_NOTLS);
        _db = _env.openDbi(DB_NAME, MDB_CREATE);
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
        try (Txn<byte[]> txn = _env.txnRead()) {
            var key = name.toString().getBytes(StandardCharsets.UTF_8);
            var value = _db.get(txn, key);
            return Optional.ofNullable(value).map(ByteString::copyFrom);
        }
    }

    private class LmdbKvIterator implements CloseableKvIterator<JObjectKey, ByteString> {
        private final Txn<byte[]> _txn = _env.txnRead();
        private final Cursor<byte[]> _cursor = _db.openCursor(_txn);
        private boolean _hasNext = false;

        LmdbKvIterator(IteratorStart start, JObjectKey key) {
            verifyReady();
            if (!_cursor.get(key.toString().getBytes(StandardCharsets.UTF_8), GetOp.MDB_SET_RANGE)) {
                return;
            }

            var got = JObjectKey.fromBytes(_cursor.key());
            var cmp = got.compareTo(key);

            assert cmp >= 0;

            _hasNext = true;

            if (cmp == 0) {
                switch (start) {
                    case LT -> {
                        _hasNext = _cursor.prev();
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
                    }
                    case GT, GE -> {
                    }
                }
            }
        }

        @Override
        public void close() {
            _cursor.close();
            _txn.close();
        }

        @Override
        public boolean hasNext() {
            return _hasNext;
        }

        @Override
        public Pair<JObjectKey, ByteString> next() {
            if (!_hasNext) {
                throw new NoSuchElementException("No more elements");
            }
            var ret = Pair.of(JObjectKey.fromBytes(_cursor.key()), ByteString.copyFrom(_cursor.val()));
            _hasNext = _cursor.next();
            return ret;
        }

        @Override
        public JObjectKey peekNextKey() {
            if (!_hasNext) {
                throw new NoSuchElementException("No more elements");
            }
            return JObjectKey.fromBytes(_cursor.key());
        }
    }

    @Override
    public CloseableKvIterator<JObjectKey, ByteString> getIterator(IteratorStart start, JObjectKey key) {
        return new LmdbKvIterator(start, key);
    }

    @Override
    public void commitTx(TxManifestRaw names) {
        verifyReady();
        try (Txn<byte[]> txn = _env.txnWrite()) {
            for (var written : names.written()) {
                var key = written.getKey().toString().getBytes(StandardCharsets.UTF_8);
                _db.put(txn, key, written.getValue().toByteArray());
            }
            for (JObjectKey key : names.deleted()) {
                var keyBytes = key.toString().getBytes(StandardCharsets.UTF_8);
                _db.delete(txn, keyBytes);
            }
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

    @Override
    public long getUsableSpace() {
        verifyReady();
        return _root.toFile().getUsableSpace();
    }

}
