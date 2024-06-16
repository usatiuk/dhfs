package com.usatiuk.dhfs.storage.objects.jrepository;

import javax.annotation.Nonnull;
import java.util.Optional;

public interface JObjectRepository {
    @Nonnull
    Optional<JObject> readJObject(String name);
    @Nonnull
    <T extends JObject> Optional<T> readJObjectChecked(String name, Class<T> clazz);
    @Nonnull
    void writeJObject(JObject object);
}
