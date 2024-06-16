package com.usatiuk.dhfs.storage.objects.jrepository;

import io.smallrye.mutiny.Uni;

import java.util.Optional;

public interface JObjectManager {
    <T extends JObject> Uni<Optional<T>> get(String name, Class<T> clazz);
    <T extends JObject> Uni<Void> put(T object);
    void invalidateJObject(String name);
}
