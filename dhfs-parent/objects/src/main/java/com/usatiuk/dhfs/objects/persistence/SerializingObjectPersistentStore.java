package com.usatiuk.dhfs.objects.persistence;

import com.usatiuk.dhfs.objects.*;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Optional;

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

    // Returns an iterator with a view of all commited objects
    // Does not have to guarantee consistent view, snapshots are handled by upper layers
    public CloseableKvIterator<JObjectKey, JDataVersionedWrapper> getIterator(IteratorStart start, JObjectKey key) {
        return new MappingKvIterator<>(delegateStore.getIterator(start, key), d -> serializer.deserialize(d));
    }

    public TxManifestRaw prepareManifest(TxManifestObj<? extends JDataVersionedWrapper> names) {
        return new TxManifestRaw(
                names.written().stream()
                        .map(e -> Pair.of(e.getKey(), serializer.serialize(e.getValue())))
                        .toList()
                , names.deleted());
    }

    void commitTx(TxManifestObj<? extends JDataVersionedWrapper> names) {
        delegateStore.commitTx(prepareManifest(names));
    }

    void commitTx(TxManifestRaw names) {
        delegateStore.commitTx(names);
    }
}
