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

    void writeObjectDirect(String name, ObjectMetadataP meta, JObjectDataP data);

    void writeObjectMetaDirect(String name, ObjectMetadataP meta);

    void writeNewObject(String name, ObjectMetadataP meta, JObjectDataP data);

    void writeNewObjectMeta(String name, ObjectMetadataP meta);

    void commitTx(TxManifest names);

    // Deletes object metadata and data
    void deleteObject(String name);

    long getTotalSpace();

    long getFreeSpace();

    long getUsableSpace();
}
