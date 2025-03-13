package com.usatiuk.dhfs.objects.persistence;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.*;
import com.usatiuk.dhfs.objects.snapshot.Snapshot;
import com.usatiuk.dhfs.utils.RefcountedCloseable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
import org.lmdbjava.Txn;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Consumer;

@ApplicationScoped
public class SerializingObjectPersistentStore {
    @Inject
    ObjectSerializer<JDataVersionedWrapper> serializer;

    @Inject
    ObjectPersistentStore delegateStore;

    @Nonnull
    Collection<JObjectKey> findAllObjects() {
        return delegateStore.findAllObjects();
    }

    @Nonnull
    Optional<JDataVersionedWrapper> readObject(JObjectKey name) {
        return delegateStore.readObject(name).map(serializer::deserialize);
    }

    public TxManifestRaw prepareManifest(TxManifestObj<? extends JDataVersionedWrapper> names) {
        return new TxManifestRaw(
                names.written().stream()
                        .map(e -> Pair.of(e.getKey(), serializer.serialize(e.getValue())))
                        .toList()
                , names.deleted());
    }

    public Snapshot<JObjectKey, JDataVersionedWrapper> getSnapshot() {
        return new Snapshot<JObjectKey, JDataVersionedWrapper>() {
            private final Snapshot<JObjectKey, ByteString> _backing = delegateStore.getSnapshot();

            @Override
            public CloseableKvIterator<JObjectKey, JDataVersionedWrapper> getIterator(IteratorStart start, JObjectKey key) {
                return new MappingKvIterator<>(_backing.getIterator(start, key), d -> serializer.deserialize(d));
            }

            @Nonnull
            @Override
            public Optional<JDataVersionedWrapper> readObject(JObjectKey name) {
                return _backing.readObject(name).map(serializer::deserialize);
            }

            @Override
            public long id() {
                return _backing.id();
            }

            @Override
            public void close() {
                _backing.close();
            }
        };

    }


//    void commitTx(TxManifestObj<? extends JDataVersionedWrapper> names, Consumer<Runnable> commitLocked) {
//        delegateStore.commitTx(prepareManifest(names), commitLocked);
//    }

    void commitTx(TxManifestRaw names, long txId, Consumer<Runnable> commitLocked) {
        delegateStore.commitTx(names, txId, commitLocked);
    }

    long getLastCommitId() {
        return delegateStore.getLastCommitId();
    }
}
