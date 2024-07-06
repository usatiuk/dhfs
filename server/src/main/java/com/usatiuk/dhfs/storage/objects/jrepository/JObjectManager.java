package com.usatiuk.dhfs.storage.objects.jrepository;

import java.util.Collection;
import java.util.Optional;

public interface JObjectManager {

    Optional<JObject<?>> get(String name);

    Collection<JObject<?>> find(String prefix);

    // Put a new object
    <T extends JObjectData> JObject<T> put(T object, Optional<String> parent);

    // Get an object with a name if it exists, otherwise create new one based on metadata
    JObject<?> getOrPut(String name, Class<? extends JObjectData> klass, Optional<String> parent);

    void notifySent(String key);
}
