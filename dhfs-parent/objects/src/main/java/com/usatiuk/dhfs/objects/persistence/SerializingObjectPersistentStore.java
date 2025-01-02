package com.usatiuk.dhfs.objects.persistence;

import com.usatiuk.dhfs.objects.JDataVersionedWrapper;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.ObjectSerializer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

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

    void writeObject(JObjectKey name, JDataVersionedWrapper<?> object) {
        delegate.writeObject(name, serializer.serialize(object));
    }

    void commitTx(TxManifest names) {
        delegate.commitTx(names);
    }
}
