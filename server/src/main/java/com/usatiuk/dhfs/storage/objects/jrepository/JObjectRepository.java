package com.usatiuk.dhfs.storage.objects.jrepository;

import javax.annotation.Nonnull;
import java.util.Optional;

public interface JObjectRepository {
    @Nonnull
    Optional<JObject> readJObject(String namespace, String name);
    @Nonnull
    <T extends JObject> Optional<T> readJObjectChecked(String namespace, String name, Class<T> clazz);
    @Nonnull
    void writeJObject(String namespace, JObject object);
}
