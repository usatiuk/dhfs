package com.usatiuk.dhfs.objects.jrepository;

import java.util.Collection;
import java.util.Optional;

public interface JObjectManager {

    Optional<JObject<?>> get(String name);

    Collection<JObject<?>> findAll();

    // Put a new object
    <T extends JObjectData> JObject<T> put(T object, Optional<String> parent);

    // Get an object with a name if it exists, otherwise create new one based on metadata
    // Should be used when working with objects referenced from the outside
    JObject<?> getOrPut(String name, Class<? extends JObjectData> klass, Optional<String> parent);
}
