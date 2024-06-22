package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.storage.objects.repository.distributed.ObjectMetadata;

import java.util.Collection;
import java.util.Optional;

public interface JObjectManager {

    <D extends JObjectData> Optional<JObject<? extends D>> get(String name, Class<D> klass);
    Optional<JObject<?>> get(String name);

    Collection<JObject<?>> find(String prefix);

    // Put a new object
    <T extends JObjectData> JObject<T> put(T object);
    // Get an object with a name if it exists, otherwise create new one based on metadata
    JObject<?> getOrPut(String name, ObjectMetadata md);

    void onWriteback(String name);
    void unref(JObject<?> object);
}
