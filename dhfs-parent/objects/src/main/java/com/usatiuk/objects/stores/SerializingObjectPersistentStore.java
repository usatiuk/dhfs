package com.usatiuk.objects.stores;

import com.usatiuk.objects.JDataVersionedWrapper;
import com.usatiuk.objects.JDataVersionedWrapperSerializer;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.iterators.*;
import com.usatiuk.objects.snapshot.Snapshot;
import com.usatiuk.utils.ListUtils;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Serializing wrapper for the ObjectPersistentStore.
 * It serializes the objects before storing them in the persistent store.
 * It deserializes the objects after reading them from the persistent store.
 */
@ApplicationScoped
public class SerializingObjectPersistentStore {
    @Inject
    JDataVersionedWrapperSerializer serializer;

    @Inject
    ObjectPersistentStore delegateStore;

    /**
     * Get a snapshot of the persistent store, with deserialized objects.
     *
     * The objects are deserialized lazily, only when their data is accessed.
     *
     * @return a snapshot of the persistent store
     */
    public Snapshot<JObjectKey, JDataVersionedWrapper> getSnapshot() {
        return new Snapshot<JObjectKey, JDataVersionedWrapper>() {
            private final Snapshot<JObjectKey, ByteBuffer> _backing = delegateStore.getSnapshot();

            @Override
            public List<CloseableKvIterator<JObjectKey, MaybeTombstone<JDataVersionedWrapper>>> getIterator(IteratorStart start, JObjectKey key) {
                return ListUtils.map(_backing.getIterator(start, key),
                        i -> new MappingKvIterator<JObjectKey, MaybeTombstone<ByteBuffer>, MaybeTombstone<JDataVersionedWrapper>>(i,
                                d -> serializer.deserialize(((DataWrapper<ByteBuffer>) d).value())));
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


    /**
     * Serialize the objects, in parallel
     * @param objs the objects to serialize
     * @return the serialized objects
     */
    private TxManifestRaw prepareManifest(TxManifestObj<? extends JDataVersionedWrapper> objs) {
        return new TxManifestRaw(
                objs.written().parallelStream()
                        .map(e -> Pair.of(e.getKey(), serializer.serialize(e.getValue())))
                        .toList()
                , objs.deleted());
    }

    /**
     * Commit a transaction to the persistent store.
     * @param objects the transaction manifest
     * @param txId the transaction ID
     */
    void commitTx(TxManifestObj<? extends JDataVersionedWrapper> objects, long txId) {
        delegateStore.commitTx(prepareManifest(objects), txId);
    }
}
