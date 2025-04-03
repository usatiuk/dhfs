package com.usatiuk.objects.stores;

import com.google.protobuf.ByteString;
import com.usatiuk.objects.JDataVersionedWrapper;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.JObjectKeyImpl;
import com.usatiuk.objects.ObjectSerializer;
import com.usatiuk.objects.iterators.CloseableKvIterator;
import com.usatiuk.objects.iterators.IteratorStart;
import com.usatiuk.objects.iterators.MappingKvIterator;
import com.usatiuk.objects.snapshot.Snapshot;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.util.Optional;

@ApplicationScoped
public class SerializingObjectPersistentStore {
    @Inject
    ObjectSerializer<JDataVersionedWrapper> serializer;

    @Inject
    ObjectPersistentStore delegateStore;

    @Nonnull
    Optional<JDataVersionedWrapper> readObject(JObjectKey name) {
        return delegateStore.readObject(name).map(serializer::deserialize);
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

    private TxManifestRaw prepareManifest(TxManifestObj<? extends JDataVersionedWrapper> objs) {
        return new TxManifestRaw(
                objs.written().stream()
                        .map(e -> Pair.of(e.getKey(), serializer.serialize(e.getValue())))
                        .toList()
                , objs.deleted());
    }

    Runnable prepareTx(TxManifestObj<? extends JDataVersionedWrapper> objects, long txId) {
        return delegateStore.prepareTx(prepareManifest(objects), txId);
    }
}
