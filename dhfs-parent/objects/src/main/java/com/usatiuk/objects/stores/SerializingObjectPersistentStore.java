package com.usatiuk.objects.stores;

import com.usatiuk.objects.JDataVersionedWrapper;
import com.usatiuk.objects.JDataVersionedWrapperSerializer;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.iterators.*;
import com.usatiuk.objects.snapshot.Snapshot;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.stream.Stream;

@ApplicationScoped
public class SerializingObjectPersistentStore {
    @Inject
    JDataVersionedWrapperSerializer serializer;

    @Inject
    ObjectPersistentStore delegateStore;

    public Snapshot<JObjectKey, JDataVersionedWrapper> getSnapshot() {
        return new Snapshot<JObjectKey, JDataVersionedWrapper>() {
            private final Snapshot<JObjectKey, ByteBuffer> _backing = delegateStore.getSnapshot();

            @Override
            public Stream<CloseableKvIterator<JObjectKey, MaybeTombstone<JDataVersionedWrapper>>> getIterator(IteratorStart start, JObjectKey key) {
                return _backing.getIterator(start, key).map(i -> new MappingKvIterator<JObjectKey, MaybeTombstone<ByteBuffer>, MaybeTombstone<JDataVersionedWrapper>>(i,
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

    private TxManifestRaw prepareManifest(TxManifestObj<? extends JDataVersionedWrapper> objs) {
        return new TxManifestRaw(
                objs.written().parallelStream()
                        .map(e -> Pair.of(e.getKey(), serializer.serialize(e.getValue())))
                        .toList()
                , objs.deleted());
    }

    Runnable prepareTx(TxManifestObj<? extends JDataVersionedWrapper> objects, long txId) {
        return delegateStore.prepareTx(prepareManifest(objects), txId);
    }
}
