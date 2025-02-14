package com.usatiuk.dhfs.objects.persistence;

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
    ObjectPersistentStore delegate;

    @Nonnull
    Collection<JObjectKey> findAllObjects() {
        return delegate.findAllObjects();
    }

    @Nonnull
    Optional<JDataVersionedWrapper<?>> readObject(JObjectKey name) {
        return delegate.readObject(name).map(serializer::deserialize);
    }

    void commitTx(TxManifestObj<? extends JDataVersionedWrapper<?>> names) {
        delegate.commitTx(new TxManifestRaw(
                names.written().stream()
                        .map(e -> Pair.of(e.getKey(), serializer.serialize(e.getValue())))
                        .toList()
                , names.deleted()));
    }
}
