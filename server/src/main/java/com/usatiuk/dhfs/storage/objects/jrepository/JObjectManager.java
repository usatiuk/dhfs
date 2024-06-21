package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.storage.objects.repository.distributed.ObjectMetadata;

import java.util.Collection;
import java.util.Optional;

public interface JObjectManager {

    <D extends JObjectData> Optional<JObject<? extends D>> get(String name, Class<D> klass);
    Optional<JObject<?>> get(String name);

    Collection<JObject<?>> find(String prefix);

    <T extends JObjectData> JObject<T> put(T object);
    JObject<?> getOrPut(String name, ObjectMetadata md);
    <T extends JObjectData> JObject<T> getOrPut(String name, T object);

    void onWriteback(String name);
    void unref(JObject<?> object);
}
