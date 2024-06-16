package com.usatiuk.dhfs.storage.objects.jrepository;

import io.smallrye.mutiny.Uni;

import java.util.Optional;

public interface JObjectManager {
    <T extends JObject> Optional<T> get(String name, Class<T> clazz);
    <T extends JObject> void put(T object);
    void invalidateJObject(String name);
}
