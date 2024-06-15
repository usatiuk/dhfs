package com.usatiuk.dhfs.storage.objects.jrepository;

import io.smallrye.mutiny.Uni;

import java.util.Optional;

public interface JObjectManager {
    <T extends JObject> Uni<Optional<T>> get(String namespace, String key, Class<T> clazz);
    <T extends JObject> Uni<Void> put(String namespace, T object);
    // Returns the object from store if it existed, nothing otherwise
    <T extends JObject> Uni<Optional<T>> tryPut(String namespace, T object);
}
