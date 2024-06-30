package com.usatiuk.dhfs.storage.objects.repository.persistence;

import javax.annotation.Nonnull;
import java.util.List;

public interface ObjectPersistentStore {
    @Nonnull
    List<String> findObjects(String prefix);
    @Nonnull
    Boolean existsObject(String name);

    @Nonnull
    Object readObject(String name);
    void writeObject(String name, Object data);
    void deleteObject(String name);
}
