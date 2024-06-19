package com.usatiuk.dhfs.storage.objects.repository;

import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

import javax.annotation.Nonnull;
import java.util.List;

//@ApplicationScoped
public class SimplePersistentObjectRepository implements ObjectRepository {
    @Inject
    ObjectPersistentStore objectPersistentStore;

    @Nonnull
    @Override
    public List<String> findObjects(String prefix) {
        return objectPersistentStore.findObjects(prefix);
    }

    @Nonnull
    @Override
    public Boolean existsObject(String name) {
        return objectPersistentStore.existsObject(name);
    }

    @Nonnull
    @Override
    public byte[] readObject(String name) {
        return objectPersistentStore.readObject(name);
    }

    @Nonnull
    @Override
    public void writeObject(String name, byte[] data, String conflictResolver) {
        objectPersistentStore.writeObject(name, data);
    }

    @Nonnull
    @Override
    public void deleteObject(String name) {
        objectPersistentStore.deleteObject(name);
    }
}
