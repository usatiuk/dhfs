package com.usatiuk.dhfs.objects.persistence;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.CloseableKvIterator;
import com.usatiuk.dhfs.objects.JDataVersionedWrapper;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.ObjectSerializer;
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

    private class SerializingKvIterator implements CloseableKvIterator<JObjectKey, JDataVersionedWrapper> {
        private final CloseableKvIterator<JObjectKey, ByteString> _delegate;

        private SerializingKvIterator(IteratorStart start, JObjectKey key) {
            _delegate = delegateStore.getIterator(start, key);
        }

        @Override
        public JObjectKey peekNextKey() {
            return _delegate.peekNextKey();
        }

        @Override
        public void skip() {
            _delegate.skip();
        }

        @Override
        public void close() {
            _delegate.close();
        }

        @Override
        public boolean hasNext() {
            return _delegate.hasNext();
        }

        @Override
        public Pair<JObjectKey, JDataVersionedWrapper> next() {
            var next = _delegate.next();
            return Pair.of(next.getKey(), serializer.deserialize(next.getValue()));
        }
    }

    // Returns an iterator with a view of all commited objects
    // Does not have to guarantee consistent view, snapshots are handled by upper layers
    public CloseableKvIterator<JObjectKey, JDataVersionedWrapper> getIterator(IteratorStart start, JObjectKey key) {
        return new SerializingKvIterator(start, key);
    }

    public CloseableKvIterator<JObjectKey, JDataVersionedWrapper> getIterator(JObjectKey key) {
        return getIterator(IteratorStart.GE, key);
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
