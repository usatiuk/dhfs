package com.usatiuk.dhfs.objects.repository.persistence;

import com.usatiuk.dhfs.objects.persistence.JObjectDataP;
import com.usatiuk.dhfs.objects.persistence.ObjectMetadataP;

import javax.annotation.Nonnull;
import java.util.Collection;

public interface ObjectPersistentStore {
    @Nonnull
    Collection<String> findAllObjects();

    @Nonnull
    Boolean existsObject(String name);

    @Nonnull
    Boolean existsObjectData(String name);

    @Nonnull
    JObjectDataP readObject(String name);

    @Nonnull
    ObjectMetadataP readObjectMeta(String name);

    void writeObject(String name, JObjectDataP data);

    void writeObjectMeta(String name, ObjectMetadataP data);

    void deleteObjectData(String name);

    // Deletes object metadata and data
    void deleteObject(String name);
}
